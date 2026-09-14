// Copyright 2026 cnpg_backup contributors. All rights reserved.
package configuration

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
)

// CaptureSnapshot holds repository and replication credentials from one kubelet
// generation. Keep it through operation shutdown and never log its contents.
type CaptureSnapshot struct {
	Repository                 *Snapshot
	Certificate, Key, ServerCA []byte
}

func LoadCaptureSnapshot(directory string) (*CaptureSnapshot, error) {
	root, err := Projection(directory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	return LoadCaptureSnapshotFromRoot(root)
}

// LoadCaptureSnapshotFromRoot reads credentials from a pinned projection generation.
func LoadCaptureSnapshotFromRoot(root *os.Root) (*CaptureSnapshot, error) {
	repo, err := loadRepositorySnapshot(root, "destination")
	if err != nil {
		return nil, err
	}
	result := &CaptureSnapshot{Repository: repo}
	for _, entry := range []struct {
		path   string
		target *[]byte
		limit  int64
	}{
		{"native/tls.crt", &result.Certificate, 256 << 10}, {"native/tls.key", &result.Key, 64 << 10}, {"native/ca.crt", &result.ServerCA, 256 << 10},
	} {
		*entry.target, err = Read(root, entry.path, entry.limit)
		if err != nil {
			return nil, err
		}
	}
	pair, err := tls.X509KeyPair(result.Certificate, result.Key)
	if err != nil {
		return nil, errors.New("invalid native replication certificate/key")
	}
	clientCA, err := Read(root, "native/client-ca.crt", 256<<10)
	if err != nil {
		return nil, err
	}
	roots, err := CAPool(clientCA, false)
	if err != nil {
		return nil, err
	}
	chain := make([]*x509.Certificate, len(pair.Certificate))
	for i, der := range pair.Certificate {
		chain[i], err = x509.ParseCertificate(der)
		if err != nil {
			return nil, errors.New("invalid native client chain")
		}
	}
	if VerifyClient(chain, roots, "streaming_replica") != nil {
		return nil, errors.New("native replication identity must be streaming_replica with current client-CA trust")
	}
	if _, err = CAPool(result.ServerCA, false); err != nil {
		return nil, err
	}
	return result, nil
}
