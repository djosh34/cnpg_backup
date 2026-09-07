// Copyright 2026 cnpg_backup contributors. All rights reserved.
package configuration

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
)

// CaptureSnapshot keeps repository credentials, replication identity and trust
// from one kubelet generation. It is never serialized or logged. Native callers
// must copy its key into private0600 operation files and retain this snapshot
// through child/request drain; libpq must use verify-full with local hostaddr.
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
	return loadCaptureSnapshot(root)
}

func loadCaptureSnapshot(root *os.Root) (*CaptureSnapshot, error) {
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

// PreflightCapture is the future native capture entry's mandatory pre-writer
// boundary, executable today without launching any native handler. It checks
// actual hard limits on workspace AND targets, then reserves the full capture
// formula on the workspace. A small differential estimate cannot bypass it.
// Primary/settings/control identity checks still belong to the native caller.
func PreflightCapture(directory string) (*CaptureSnapshot, error) {
	root, err := Projection(directory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	snapshot, err := loadCaptureSnapshot(root)
	if err != nil {
		return nil, err
	}
	data, err := Read(root, "capacity.json", 64<<10)
	if err != nil {
		return nil, err
	}
	var budgets []FilesystemBudget
	if err = StrictJSON(data, &budgets); err != nil {
		return nil, err
	}
	found := false
	required, err := CaptureCapacity(snapshot.Repository.Spec.Native)
	if err != nil {
		return nil, err
	}
	for i := range budgets {
		if budgets[i].Mount == "/cnpg-backup/work" {
			budgets[i].RequiredBytes = required
			found = true
		}
	}
	if !found {
		return nil, errors.New("native workspace budget missing")
	}
	if err = CheckCapacity(budgets); err != nil {
		return nil, err
	}
	return snapshot, nil
}
