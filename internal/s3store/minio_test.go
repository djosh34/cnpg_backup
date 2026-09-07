package s3store

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const minioSHA256 = "7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f"

// This test accepts no endpoint or credentials: it starts only a checksum-pinned
// binary on ephemeral loopback with a fresh private CA and disposable root keys.
// The binary is the same bootstrap download used by hack/test integration.
func TestMinIOPrimitives(t *testing.T) {
	if os.Getenv("CNPG_S3_INTEGRATION") != "1" {
		t.Skip("real MinIO runs in hack/test integration; no product qualification claimed")
	}
	binary := os.Getenv("CNPG_S3_MINIO_BINARY")
	b, e := os.Open(binary)
	if e != nil {
		t.Fatal(e)
	}
	h := sha256.New()
	_, e = io.Copy(h, b)
	b.Close()
	if e != nil || hex.EncodeToString(h.Sum(nil)) != minioSHA256 {
		t.Fatal("MinIO binary pin mismatch")
	}
	root := t.TempDir()
	ca := privateCA(t, root)
	listener, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	endpoint := listener.Addr().String()
	listener.Close()
	secret := make([]byte, 24)
	rand.Read(secret)
	c := config("https://"+endpoint, ca)
	c.AccessKey = "cnpg-test"
	c.SecretKey = hex.EncodeToString(secret)
	log, e := os.Create(filepath.Join(root, "minio.log"))
	if e != nil {
		t.Fatal(e)
	}
	defer log.Close()
	t.Cleanup(func() { retainMinIOLog(t, log.Name(), c.AccessKey, c.SecretKey) })
	cmd := exec.Command(binary, "server", "--address", endpoint, "--console-address", "127.0.0.1:0", "--certs-dir", filepath.Join(root, "certs"), filepath.Join(root, "data"))
	cmd.Env = []string{"HOME=" + root, "MINIO_ROOT_USER=" + c.AccessKey, "MINIO_ROOT_PASSWORD=" + c.SecretKey, "MINIO_BROWSER=off", "MINIO_UPDATE=off"}
	cmd.Stdout = log
	cmd.Stderr = log
	if e = cmd.Start(); e != nil {
		t.Fatal("MinIO start failed")
	}
	exited := make(chan struct{})
	go func() { cmd.Wait(); close(exited) }()
	t.Cleanup(func() {
		cmd.Process.Kill()
		select {
		case <-exited:
		case <-time.After(10 * time.Second):
			t.Error("MinIO did not drain")
		}
	})
	readyStore, e := New(c)
	if e != nil {
		t.Fatal(e)
	}
	defer readyStore.Close()
	client := &http.Client{Transport: readyStore.transport, Timeout: time.Second}
	ready := false
	for range 100 {
		resp, err := client.Get(c.Endpoint + "/minio/health/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				ready = true
				break
			}
		}
		select {
		case <-exited:
			t.Fatal("pinned MinIO exited before readiness (host may prohibit AF_NETLINK); real checks NOT executed")
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		t.Fatal("MinIO readiness deadline")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if e = fixtureCreateBucket(ctx, readyStore, c); e != nil {
		t.Fatal("SETUP MakeBucket failed before product assertions:", setupError(e))
	}
	for _, signature := range []string{"v2", "v4"} {
		t.Run(signature, func(t *testing.T) {
			c.Signature = signature
			c.Prefix = signature
			s, e := New(c)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			if e = s.CheckBucketSafety(ctx); e != nil {
				t.Fatal("bucket safety", e)
			}
			keys, e := s.CheckConditions(ctx, "probes", t.TempDir())
			if e != nil {
				t.Fatal("conditional probe", e)
			}
			for _, key := range keys {
				if e = s.Delete(ctx, key); e != nil {
					t.Fatal("probe explicit cleanup", e)
				}
			}
			f, expected := spool(t, []byte("same-content"))
			v, e := s.PutFile(ctx, "wal", f, expected, Condition{Create: true}, map[string]string{"cnpg-stored-sha256": expected.SHA256})
			if e != nil {
				t.Fatal(e)
			}
			head, e := s.Head(ctx, "wal")
			if e != nil || head.ETag != v.ETag || head.Modified.IsZero() {
				t.Fatal("HEAD", e)
			}
			dst, _ := spool(t, nil)
			if _, e = s.DownloadWAL(ctx, "wal", dst, expected); e != nil {
				t.Fatal("GET", e)
			}
			_, e = s.PutFile(ctx, "wal", f, expected, Condition{Create: true}, nil)
			mustKind(t, e, Precondition, false)
			// Concurrent atomic create: exactly one real remote winner under each signer.
			var wins atomic.Int32
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(func() {
					file, err := os.Open(f.Name())
					if err != nil {
						t.Error(err)
						return
					}
					defer file.Close()
					_, err = s.PutFile(ctx, "race", file, expected, Condition{Create: true}, nil)
					if err == nil {
						wins.Add(1)
					} else if !Is(err, Precondition) {
						t.Error(err)
					}
				})
			}
			wg.Wait()
			if wins.Load() != 1 {
				t.Fatal("conditional winners", wins.Load())
			}
			// Exercise production pagination with >1000 real objects. No SDK setup PUTs.
			for i := range 1001 {
				if _, e = s.PutFile(ctx, fmt.Sprintf("pages/%04d", i), f, expected, Condition{Create: true}, nil); e != nil {
					t.Fatal(e)
				}
			}
			count := 0
			if e = s.List(ctx, "pages/", 1100, func(v Info) error { count++; return nil }); e != nil || count != 1001 {
				t.Fatalf("object pagination %d: %v", count, e)
			}
			large, integrity := sparse(t, PartSize*2+17)
			u, _, e := s.UploadFile(ctx, "artifact", large, integrity, map[string]string{"cnpg-stored-sha256": integrity.SHA256})
			if e != nil || u.ID == "" {
				t.Fatal("multipart init/part/complete", e)
			}
			if _, e = s.Download(ctx, "artifact", dst, integrity); e != nil {
				t.Fatal("multipart verify", e)
			}
			// Real remote completion and PUT succeed; test-only wire loses the response.
			lost := faultClient(t, s, c, func(r *http.Request, resp *http.Response) bool {
				return r.Method == "PUT" && !r.URL.Query().Has("partNumber") || r.Method == "POST" && r.URL.Query().Has("uploadId")
			}, nil)
			_, e = lost.PutFile(ctx, "lost-put", f, expected, Condition{Create: true}, nil)
			mustKind(t, e, Transient, true)
			if _, e = s.DownloadWAL(ctx, "lost-put", dst, expected); e != nil {
				t.Fatal("lost PUT not durable", e)
			}
			_, _, e = lost.UploadFile(ctx, "lost-complete", large, integrity, nil)
			mustKind(t, e, Transient, true)
			if _, e = s.Download(ctx, "lost-complete", dst, integrity); e != nil {
				t.Fatal("lost completion not durable", e)
			}
			// Interrupt completion BEFORE dispatch. Parts remain remotely discoverable;
			// only a later explicit admitted abort removes them. No production auto-abort.
			var intercepted atomic.Int32
			interrupted := faultClient(t, s, c, nil, func(r *http.Request) bool {
				if r.Method == "POST" && r.URL.Query().Has("uploadId") {
					intercepted.Add(1)
					return true
				}
				return false
			})
			pending, _, e := interrupted.UploadFile(ctx, "incomplete", large, integrity, nil)
			mustKind(t, e, Transient, true)
			if intercepted.Load() != 1 {
				t.Fatal("complete retried")
			}
			n := 0
			if e = s.ListParts(ctx, pending, func(p Part) error { n++; return nil }); e != nil || n != 3 {
				t.Fatalf("part list %d %v", n, e)
			}
			found := false
			if e = s.ListUploads(ctx, "incomplete", 10, func(u Upload) error { found = u == pending; return nil }); e != nil || !found {
				t.Fatal("incomplete missing", e)
			}
			if e = s.Abort(ctx, pending); e != nil {
				t.Fatal("abort", e)
			}
			found = false
			if e = s.ListUploads(ctx, "incomplete", 10, func(u Upload) error { found = true; return nil }); e != nil || found {
				t.Fatal("abort did not complete", e)
			}
			if e = s.Delete(ctx, "artifact"); e != nil {
				t.Fatal("delete", e)
			}
			_, _, e = s.Read(ctx, "artifact", 100)
			mustKind(t, e, NotFound, false)
			bad := c
			bad.SecretKey = "incorrect-credential"
			denied, e := New(bad)
			if e != nil {
				t.Fatal(e)
			}
			defer denied.Close()
			_, _, e = denied.Read(ctx, "wal", 100)
			mustKind(t, e, Auth, false)
			bad = c
			bad.CA = nil
			untrusted, e := New(bad)
			if e != nil {
				t.Fatal(e)
			}
			defer untrusted.Close()
			_, _, e = untrusted.Read(ctx, "wal", 100)
			mustKind(t, e, TLS, false)
			t.Log("PASS pinned MinIO/private CA", signature, "conditional create/CAS, concurrent winner, verified GET/HEAD, object pages, MPU init/part/complete/list/abort, delete, bucket checks, response loss")
		})
	}
}

func privateCA(t *testing.T, root string) []byte {
	t.Helper()
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "cnpg-test-ca"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, e := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	leafKey, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"}, NotBefore: template.NotBefore, NotAfter: template.NotAfter, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost"}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, e = x509.CreateCertificate(rand.Reader, leaf, template, &leafKey.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(root, "certs")
	if e = os.Mkdir(path, 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(path, "public.crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); e != nil {
		t.Fatal(e)
	}
	pk, e := x509.MarshalECPrivateKey(leafKey)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(path, "private.key"), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: pk}), 0600); e != nil {
		t.Fatal(e)
	}
	return ca
}

type faultWire struct {
	base   http.RoundTripper
	after  func(*http.Request, *http.Response) bool
	before func(*http.Request) bool
}

func (f faultWire) RoundTrip(r *http.Request) (*http.Response, error) {
	if f.before != nil && f.before(r) {
		if r.Body != nil {
			r.Body.Close()
		}
		return nil, failure(Transient)
	}
	resp, e := f.base.RoundTrip(r)
	if e == nil && f.after != nil && resp.StatusCode < 300 && f.after(r, resp) {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return nil, failure(Transient)
	}
	return resp, e
}

// Reconstruct only the SDK client with an injected network boundary; every
// adapter method, production response guard and retry bound remains unchanged.
func faultClient(t *testing.T, s *Store, c Config, after func(*http.Request, *http.Response) bool, before func(*http.Request) bool) *Store {
	t.Helper()
	copy := *s
	creds := credentials.NewStaticV4(c.AccessKey, c.SecretKey, c.SessionToken)
	if c.Signature == "v2" {
		creds = credentials.NewStaticV2(c.AccessKey, c.SecretKey, "")
	}
	core, e := minio.NewCore(strings.TrimPrefix(c.Endpoint, "https://"), &minio.Options{Secure: true, Region: c.Region, BucketLookup: minio.BucketLookupPath, MaxRetries: 1, Creds: creds, Transport: &transport{base: faultWire{s.transport, after, before}, endpoint: strings.TrimPrefix(c.Endpoint, "https://"), slots: processHTTP, metadata: s.metadataTimeout, data: s.dataTimeout}})
	if e != nil {
		t.Fatal(e)
	}
	copy.core = core
	return &copy
}
