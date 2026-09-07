// Package s3store provides bounded, explicitly owned S3 I/O. It does not own
// repository admission, object identity reconciliation, or cleanup policy.
package s3store

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/s3utils"
)

const (
	PartSize        int64 = 64 << 20
	MaxArtifactSize int64 = 512 << 30
	MaxSingleSize   int64 = (1 << 30) + (1 << 20)
	MaxMetadataSize int64 = 4 << 20
)

// Config is a complete immutable operation snapshot, not a credential provider.
// Zero I/O limits select the documented defaults. No proxy/redirect/TLS bypass
// or ambient credential settings are accepted.
type Config struct {
	Endpoint, Bucket, Prefix, Signature, Addressing, Region                                 string
	AccessKey, SecretKey, SessionToken                                                      string
	CA                                                                                      []byte
	ConnectTimeout, MetadataTimeout, DataRequestTimeout, OperationTimeout, WALUploadTimeout time.Duration
	ArtifactUploads, PartWorkers, WALUploads                                                int
}

// Hard process ceilings also cover overlapping credential/client snapshots.
var processWAL = make(chan struct{}, 2)
var processArtifacts = make(chan struct{}, 2)
var processHTTP = make(chan struct{}, 8)

type Store struct {
	core                                                       *minio.Core
	transport                                                  *http.Transport
	bucket, prefix                                             string
	metadataTimeout, dataTimeout, operationTimeout, walTimeout time.Duration
	artifacts, wal                                             chan struct{}
	workers                                                    int
}

func New(c Config) (*Store, error) {
	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") || u.RawPath != "" || strings.HasSuffix(c.Endpoint, "#") {
		return nil, failure(Invalid)
	}
	if c.Signature != "v2" && c.Signature != "v4" || c.Addressing != "path" || c.Region == "" || !plain(c.Region) || c.AccessKey == "" || c.SecretKey == "" || !plain(c.AccessKey) || !plain(c.SecretKey) || !plain(c.SessionToken) || c.Signature == "v2" && c.SessionToken != "" {
		return nil, failure(Invalid)
	}
	if s3utils.IsS3ExpressBucket(c.Bucket) || s3utils.IsAmazonExpressRegionalEndpoint(*u) || s3utils.IsAmazonExpressZonalEndpoint(*u) || s3utils.IsAmazonOutpostsEndpoint(*u) || c.Signature == "v2" && s3utils.IsAmazonEndpoint(*u) {
		return nil, failure(Invalid)
	}
	if s3utils.CheckValidBucketNameStrict(c.Bucket) != nil || len(c.Prefix) > 128 || (c.Prefix != "" && !validPath(c.Prefix)) {
		return nil, failure(Invalid)
	}
	defaults := []struct {
		p             *time.Duration
		def, min, max time.Duration
	}{
		{&c.ConnectTimeout, 10 * time.Second, time.Second, time.Minute},
		{&c.MetadataTimeout, 30 * time.Second, time.Second, 120 * time.Second},
		{&c.DataRequestTimeout, 15 * time.Minute, 10 * time.Second, time.Hour},
		{&c.OperationTimeout, 24 * time.Hour, time.Minute, 7 * 24 * time.Hour},
		{&c.WALUploadTimeout, 120 * time.Second, 10 * time.Second, 15 * time.Minute},
	}
	for _, d := range defaults {
		if *d.p == 0 {
			*d.p = d.def
		}
		if *d.p < d.min || *d.p > d.max {
			return nil, failure(Invalid)
		}
	}
	for _, n := range []*int{&c.ArtifactUploads, &c.PartWorkers, &c.WALUploads} {
		if *n == 0 {
			*n = 2
		}
		if *n < 1 || *n > 2 {
			return nil, failure(Invalid)
		}
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, failure(TLS)
	}
	if len(c.CA) > 1<<20 {
		return nil, failure(Invalid)
	}
	bundle := c.CA
	for len(bytes.TrimSpace(bundle)) > 0 {
		block, rest := pem.Decode(bundle)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 || !bytes.HasPrefix(bytes.TrimSpace(bundle), []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, failure(Invalid)
		}
		cert, e := x509.ParseCertificate(block.Bytes)
		if e != nil || !cert.IsCA {
			return nil, failure(Invalid)
		}
		roots.AddCert(cert)
		bundle = rest
	}
	// Fresh HTTP/1 connections prevent net/http's automatic replay on reused
	// connections, including bodyless destructive requests. No environment proxy.
	tr := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: c.ConnectTimeout}).DialContext,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, TLSHandshakeTimeout: c.ConnectTimeout,
		DisableKeepAlives: true, DisableCompression: true, MaxConnsPerHost: 8, MaxResponseHeaderBytes: 64 << 10,
		TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{}, ForceAttemptHTTP2: false}
	creds := credentials.NewStaticV4(c.AccessKey, c.SecretKey, c.SessionToken)
	if c.Signature == "v2" {
		creds = credentials.NewStaticV2(c.AccessKey, c.SecretKey, "")
	}
	wire := &transport{base: tr, endpoint: u.Host, slots: processHTTP, metadata: c.MetadataTimeout, data: c.DataRequestTimeout}
	core, err := minio.NewCore(u.Host, &minio.Options{Secure: true, Region: c.Region, BucketLookup: minio.BucketLookupPath, MaxRetries: 1, Creds: creds, Transport: wire})
	if err != nil {
		return nil, failure(Invalid)
	}
	core.SetS3EnableDualstack(false) // Do not silently change the administrator's endpoint.
	return &Store{core: core, transport: tr, bucket: c.Bucket, prefix: c.Prefix, metadataTimeout: c.MetadataTimeout, dataTimeout: c.DataRequestTimeout, operationTimeout: c.OperationTimeout, walTimeout: c.WALUploadTimeout, artifacts: make(chan struct{}, c.ArtifactUploads), wal: make(chan struct{}, c.WALUploads), workers: c.PartWorkers}, nil
}

func (s *Store) Close() { s.transport.CloseIdleConnections() }
func plain(v string) bool {
	for _, r := range v {
		if r < 32 || r > 126 {
			return false
		}
	}
	return true
}
func validPath(v string) bool {
	if v == "" || !plain(v) || strings.ContainsAny(v, "\\%?#") {
		return false
	}
	for _, part := range strings.Split(v, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}
func (s *Store) key(k string) (string, error) {
	if len(k) > 1024 || !validPath(k) {
		return "", failure(Invalid)
	}
	if s.prefix != "" {
		k = s.prefix + "/" + k
	}
	if len(k) > 1024 {
		return "", failure(Invalid)
	}
	return k, nil
}
func (s *Store) listPrefix(p string) (string, error) {
	if p == "" {
		if s.prefix == "" {
			return "", nil
		}
		return s.prefix + "/", nil
	}
	trail := strings.HasSuffix(p, "/")
	k, e := s.key(strings.TrimSuffix(p, "/"))
	if trail {
		k += "/"
	}
	return k, e
}
func acquire(ctx context.Context, slot chan struct{}) error {
	select {
	case slot <- struct{}{}:
		return nil
	case <-ctx.Done():
		return failure(Canceled)
	}
}
func acquireData(ctx context.Context, local, global chan struct{}) (func(), error) {
	if e := acquire(ctx, local); e != nil {
		return nil, e
	}
	if e := acquire(ctx, global); e != nil {
		<-local
		return nil, e
	}
	return func() { <-global; <-local }, nil
}

func (s *Store) operation(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.operationTimeout)
}
