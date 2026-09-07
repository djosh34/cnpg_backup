package wal

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/hack/miniofixture"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func TestMinIOWAL(t *testing.T) {
	if os.Getenv("CNPG_S3_INTEGRATION") != "1" {
		t.Skip("actual MinIO WAL checks run in hack/test integration")
	}
	config := walMinIO(t)
	for _, sig := range []string{"v2", "v4"} {
		t.Run(sig, func(t *testing.T) {
			config.Signature = sig
			config.Prefix = "wal-" + sig
			store, e := s3store.New(config)
			if e != nil {
				t.Fatal(e)
			}
			defer store.Close()
			ctx := context.Background()
			work := t.TempDir()
			id := repository.Identity{Schema: 1, RepositoryID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", PostgresMajor: 18, SystemIdentifier: "123456", WALSegmentBytes: 1 << 20, WriterClusterUID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}
			repo, e := repository.OpenWriter(ctx, store, id, work)
			if e != nil {
				t.Fatal("PRODUCT initialize", e)
			}
			w := Files{repo, store, work, "gzip"}
			name := "000000010000000000000001"
			raw := bytes.Repeat([]byte{19}, 1<<20)
			src := source(t, raw)
			if e = w.Archive(ctx, name, src); e != nil {
				t.Fatal("PRODUCT gzip archive", e)
			}
			w.Compression = "none"
			if e = w.Archive(ctx, name, src); e != nil {
				t.Fatal("PRODUCT identical raw/different compression retry", e)
			}
			other := bytes.Repeat([]byte{20}, 1<<20)
			if e = w.Archive(ctx, name, source(t, other)); e != ErrConflict {
				t.Fatal("PRODUCT differing retry", e)
			}
			root, dir := local(t)
			if e = w.Restore(ctx, name, root, "RECOVERYXLOG"); e != nil {
				t.Fatal("PRODUCT retrieve", e)
			}
			actual, e := os.ReadFile(filepath.Join(dir, "RECOVERYXLOG"))
			if e != nil || !bytes.Equal(actual, raw) {
				t.Fatal("independent raw byte oracle", e)
			}
			for _, history := range []string{"00000002.history", "000000010000000000000001.00000028.backup"} {
				data := []byte("synthetic bounded history fixture\n")
				if e = w.Archive(ctx, history, source(t, data)); e != nil {
					t.Fatal(e)
				}
				if e = w.Restore(ctx, history, root, history); e != nil {
					t.Fatal(e)
				}
				got, _ := os.ReadFile(filepath.Join(dir, history))
				if !bytes.Equal(got, data) {
					t.Fatal("history raw oracle")
				}
			}
			// Promotion auxiliaries coexist with the complete filename and use
			// the same durable conditional contract under both actual signers.
			for i, compression := range []string{"none", "gzip"} {
				partial := []string{name + ".partial", "000000010000000000000002.partial"}[i]
				w.Compression = compression
				if e = w.Archive(ctx, partial, source(t, other)); e != nil {
					t.Fatal("PRODUCT partial archive", e)
				}
				w.Compression = "none"
				if e = w.Archive(ctx, partial, source(t, other)); e != nil {
					t.Fatal("PRODUCT partial identical retry", e)
				}
				if e = w.Archive(ctx, partial, src); e != ErrConflict {
					t.Fatal("PRODUCT partial conflicting retry", e)
				}
				if e = w.Restore(ctx, partial, root, partial); e != nil {
					t.Fatal(e)
				}
				got, readErr := os.ReadFile(filepath.Join(dir, partial))
				if readErr != nil || !bytes.Equal(got, other) {
					t.Fatal("partial independent byte oracle", readErr)
				}
			}
			if e = w.Restore(ctx, "000000010000000000000002", root, "RECOVERYXLOG"); !s3store.Is(e, s3store.NotFound) {
				t.Fatal("partial substituted for absent full filename", e)
			}
			if e = w.Restore(ctx, "00000003.history", root, "RECOVERYHISTORY"); !s3store.Is(e, s3store.NotFound) {
				t.Fatal("PRODUCT authenticated absence", e)
			}
			key, _ := repo.WALKey(name)
			info, e := store.Head(ctx, key)
			if e != nil {
				t.Fatal(e)
			}
			owner, e := repo.AcquireGC(ctx)
			if e != nil {
				t.Fatal(e)
			}
			plan := repository.GCPlan{Schema: 1, RepositoryID: repo.Identity().RepositoryID, OperationID: owner.OperationID(), ProcessID: repo.ProcessID(), Cutoff: "2026-09-07T00:00:00Z", Policy: repository.Policy{WindowSeconds: 3600, MinimumFulls: 1}, Victims: []repository.Victim{{Kind: "retire-wal", WALName: &name, ExpectedETag: &info.ETag, SHA256: hash(raw), RawBytes: int64(len(raw))}}}
			if e = owner.Execute(ctx, plan); e != nil {
				t.Fatal("PRODUCT permanent WAL retirement", e)
			}
			if e = owner.Close(ctx); e != nil {
				t.Fatal(e)
			}
			if e = w.Restore(ctx, name+".partial", root, name+".partial"); e != nil {
				t.Fatal("full retirement affected distinct partial", e)
			}
			if e = w.Archive(ctx, name, src); e != ErrExpired {
				t.Fatal("PRODUCT retired slot recreated", e)
			}
			if e = w.Restore(ctx, name, root, name); e != ErrExpired {
				t.Fatal("PRODUCT retired slot became EOF", e)
			}
		})
	}
}

// No external endpoint/credential inputs. This fixture uses the same checksum
// pin and narrow startup barrier as the existing B/C integration suites.
func walMinIO(t *testing.T) s3store.Config {
	t.Helper()
	binary := os.Getenv("CNPG_S3_MINIO_BINARY")
	f, e := os.Open(binary)
	if e != nil {
		t.Fatal("SETUP pinned MinIO unavailable")
	}
	h := sha256.New()
	_, e = io.Copy(h, f)
	f.Close()
	if e != nil || hex.EncodeToString(h.Sum(nil)) != "7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f" {
		t.Fatal("SETUP MinIO checksum mismatch")
	}
	// Go's localhost-only test certificate is a CA; its private key remains in
	// the private fixture directory. Actual CNPG separately tests a two-cert chain.
	certServer := httptest.NewTLSServer(http.NotFoundHandler())
	pair := certServer.TLS.Certificates[0]
	certServer.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: pair.Certificate[0]})
	key, e := x509.MarshalPKCS8PrivateKey(pair.PrivateKey)
	if e != nil {
		t.Fatal(e)
	}
	directory := t.TempDir()
	certs := filepath.Join(directory, "certs")
	if e = os.Mkdir(certs, 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(certs, "public.crt"), ca, 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(certs, "private.key"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600); e != nil {
		t.Fatal(e)
	}
	listener, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	address := listener.Addr().String()
	listener.Close()
	secret := make([]byte, 24)
	if _, e = rand.Read(secret); e != nil {
		t.Fatal(e)
	}
	c := s3store.Config{Endpoint: "https://" + address, Bucket: "wal-test", Signature: "v4", Addressing: "path", Region: "us-east-1", AccessKey: "wal-fixture", SecretKey: hex.EncodeToString(secret), CA: ca}
	log, e := os.Create(filepath.Join(directory, "minio.log"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		log.Close()
		f, e := os.Open(log.Name())
		if e != nil {
			return
		}
		defer f.Close()
		b, _ := io.ReadAll(io.LimitReader(f, 128<<10))
		redacted := strings.ReplaceAll(strings.ReplaceAll(string(b), c.AccessKey, "<REDACTED>"), c.SecretKey, "<REDACTED>")
		if t.Failed() {
			t.Log(redacted)
		}
		if out := os.Getenv("CNPG_S3_ARTIFACT_DIR"); filepath.IsAbs(out) {
			if os.MkdirAll(out, 0700) == nil {
				if e := os.WriteFile(filepath.Join(out, "wal-minio-server.log"), []byte(redacted), 0600); e != nil {
					t.Error(e)
				}
			}
		}
	})
	cmd := exec.Command(binary, "server", "--address", address, "--console-address", "127.0.0.1:0", "--certs-dir", certs, filepath.Join(directory, "data"))
	cmd.Env = []string{"HOME=" + directory, "MINIO_ROOT_USER=" + c.AccessKey, "MINIO_ROOT_PASSWORD=" + c.SecretKey, "MINIO_BROWSER=off", "MINIO_UPDATE=off"}
	cmd.Stdout = log
	cmd.Stderr = log
	if e = cmd.Start(); e != nil {
		t.Fatal("SETUP MinIO start failed")
	}
	done := make(chan struct{})
	go func() { cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("SETUP MinIO did not terminate")
		}
	})
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca)
	tr := &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	client := http.Client{Transport: tr, Timeout: time.Second}
	ready := false
	for i := 0; i < 100; i++ {
		response, e := client.Get(c.Endpoint + "/minio/health/ready")
		if e == nil {
			response.Body.Close()
			if response.StatusCode == 200 {
				ready = true
				break
			}
		}
		select {
		case <-done:
			t.Fatal("SETUP MinIO exited; product checks unexecuted")
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		t.Fatal("SETUP MinIO readiness deadline")
	}
	core, e := minio.NewCore(address, &minio.Options{Secure: true, Region: c.Region, BucketLookup: minio.BucketLookupPath, MaxRetries: 1, Creds: credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""), Transport: tr})
	if e != nil {
		t.Fatal("SETUP SDK client")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if e = miniofixture.CreateBucket(ctx, core, c.Bucket, c.Region); e != nil {
		t.Fatalf("SETUP CreateBucket error_type=%T", e)
	}
	return c
}
