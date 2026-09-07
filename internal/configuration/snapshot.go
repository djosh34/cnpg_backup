// Copyright 2026 cnpg_backup contributors. All rights reserved.
package configuration

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Projection opens exactly one kubelet atomic-writer generation. Never use
// subPath mounts or reopen ..data between members of a credential/trust bundle.
func Projection(directory string) (*os.Root, error) {
	generation, err := filepath.EvalSymlinks(filepath.Join(directory, "..data"))
	if err != nil {
		return nil, errors.New("projection generation unavailable")
	}
	return os.OpenRoot(generation)
}
func Read(root *os.Root, path string, limit int64) ([]byte, error) {
	f, err := root.Open(path)
	if err != nil {
		return nil, errors.New("projected input missing")
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(b)) > limit {
		return nil, errors.New("projected input exceeds limit")
	}
	return b, nil
}
func CAPool(data []byte, system bool) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	if system {
		var err error
		pool, err = x509.SystemCertPool()
		if err != nil {
			return nil, errors.New("system trust unavailable")
		}
	}
	count := 0
	for len(data) > 0 {
		block, rest := pem.Decode(data)
		if block == nil || block.Type != "CERTIFICATE" {
			return nil, errors.New("invalid CA bundle")
		}
		data = rest
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !cert.IsCA || time.Now().Before(cert.NotBefore) || !time.Now().Before(cert.NotAfter) {
			return nil, errors.New("invalid or expired CA")
		}
		pool.AddCert(cert)
		count++
	}
	if count == 0 {
		return nil, errors.New("empty CA bundle")
	}
	return pool, nil
}

// Snapshot is fresh for each operation; callers retain it through complete drain.
// It has no formatter that prints credentials and is never serialized/logged.
type Snapshot struct {
	Spec                               Spec
	AccessKey, SecretKey, SessionToken []byte
	Roots                              *x509.CertPool
}

func LoadSnapshot(directory, role string) (*Snapshot, error) {
	if role != "destination" && role != "source" {
		return nil, errors.New("invalid repository role")
	}
	root, err := Projection(directory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	return loadRepositorySnapshot(root, role)
}

func loadRepositorySnapshot(root *os.Root, role string) (*Snapshot, error) {
	data, err := Read(root, role+"/repository.json", 256<<10)
	if err != nil {
		return nil, err
	}
	spec, err := DecodeSpec(data)
	if err != nil {
		return nil, err
	}
	result := &Snapshot{Spec: spec}
	for _, v := range []struct {
		path string
		out  *[]byte
	}{{"accessKey", &result.AccessKey}, {"secretKey", &result.SecretKey}} {
		*v.out, err = Read(root, role+"/"+v.path, 64<<10)
		if err != nil || len(*v.out) == 0 {
			return nil, errors.New("invalid credential snapshot")
		}
	}
	if spec.S3.SessionTokenSecret != nil {
		result.SessionToken, err = Read(root, role+"/sessionToken", 64<<10)
		if err != nil || len(result.SessionToken) == 0 {
			return nil, errors.New("invalid token snapshot")
		}
	}
	if spec.S3.CAConfigMap != nil {
		data, err = Read(root, role+"/ca.crt", 256<<10)
		if err != nil {
			return nil, err
		}
		result.Roots, err = CAPool(data, true)
	} else {
		result.Roots, err = x509.SystemCertPool()
	}
	if err != nil {
		return nil, errors.New("invalid trust snapshot")
	}
	return result, nil
}
func LoadManagerTLS(directory, clientName string) (*tls.Config, error) {
	root, err := Projection(directory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	cert, err := Read(root, "tls.crt", 256<<10)
	if err != nil {
		return nil, err
	}
	key, err := Read(root, "tls.key", 64<<10)
	if err != nil {
		return nil, err
	}
	ca, err := Read(root, "client-ca.crt", 256<<10)
	if err != nil {
		return nil, err
	}
	pair, err := tls.X509KeyPair(cert, key)
	if err != nil {
		return nil, errors.New("invalid manager leaf/key snapshot")
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil || time.Now().Before(leaf.NotBefore) || !time.Now().Before(leaf.NotAfter) {
		return nil, errors.New("invalid manager leaf")
	}
	roots, err := CAPool(ca, false)
	if err != nil {
		return nil, err
	}
	if clientName == "" {
		return nil, errors.New("client identity is required")
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h2"}, Certificates: []tls.Certificate{pair}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots,
		VerifyConnection: func(state tls.ConnectionState) error { return VerifyClient(state.PeerCertificates, roots, clientName) }}, nil
}
func VerifyClient(chain []*x509.Certificate, roots *x509.CertPool, name string) error {
	if len(chain) == 0 || chain[0].Subject.CommonName != name {
		return errors.New("unexpected manager client identity")
	}
	intermediates := x509.NewCertPool()
	for _, cert := range chain[1:] {
		intermediates.AddCert(cert)
	}
	_, err := chain[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	if err != nil {
		return errors.New("untrusted manager client")
	}
	return nil
}
