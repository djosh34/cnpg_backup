package repository

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/hack/miniofixture"
	"github.com/djosh34/cnpg_backup/internal/s3store"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Actual adapter + actual pinned MinIO, not the deterministic fake-store tests.
// Delivery faults below wrap calls AFTER actual remote success (or postpone
// actual dispatch); they do not model remote data in a map. Native fixture bytes
// are synthetic: this is repository integration, not PostgreSQL recovery.
func TestMinIORepository(t *testing.T) {
	if os.Getenv("CNPG_S3_INTEGRATION") != "1" {
		t.Skip("real repository/MinIO integration runs in hack/test integration")
	}
	c := startRepositoryMinIO(t)
	for _, sig := range []string{"v2", "v4"} {
		t.Run(sig, func(t *testing.T) {
			c.Signature = sig
			c.Prefix = "repository-" + sig
			retentionConfig := c
			retentionConfig.Prefix += "-retention"
			t.Run("retention-batches", func(t *testing.T) { minioRetentionBatches(t, retentionConfig) })
			s, e := s3store.New(c)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			r, e := Initialize(ctx, s, identity(), t.TempDir())
			if e != nil {
				t.Fatal("PRODUCT initialize:", e)
			}
			h1, e := r.AdmitBackup(ctx, writerID, backupID)
			if e != nil {
				t.Fatal(e)
			}
			h2, e := r.AdmitBackup(ctx, writerID, backupID)
			if e != nil {
				t.Fatal(e)
			}
			a1, _, e := h1.Begin(ctx, request(backupID))
			if e != nil {
				t.Fatal(e)
			}
			a2, _, e := h2.Begin(ctx, request(backupID))
			if e != nil {
				t.Fatal(e)
			}
			c1, m1, f1 := capture(t, a1, "actual MinIO winner")
			win, e := a1.Publish(ctx, c1, m1, f1)
			if e != nil {
				t.Fatal("PRODUCT publication:", e)
			}
			c2, m2, f2 := capture(t, a2, "different loser bytes")
			got, e := a2.Publish(ctx, c2, m2, f2)
			if e != nil || digest(got.Bytes) != digest(win.Bytes) || got.PublishedAt.IsZero() {
				t.Fatal("PRODUCT first-winner replay:", e)
			}
			h1.Close(ctx)
			h2.Close(ctx)
			fresh, e := OpenSource(ctx, s, repoID, t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			reader, e := fresh.AdmitRestore(ctx, targetID, UUID())
			if e != nil {
				t.Fatal(e)
			}
			cat, e := reader.Catalog(ctx, CatalogLimits{10, 32 << 20})
			if e != nil {
				t.Fatal(e)
			}
			count := 0
			if e = cat.Visit(ctx, func(en Entry) error {
				if en.Retired {
					t.Error("unexpected retirement")
				}
				count++
				return nil
			}); e != nil || count != 1 {
				t.Fatal("PRODUCT S3-only catalog:", count, e)
			}
			cat.Close()
			if _, e = r.AcquireGC(ctx); e != ErrBlocked {
				t.Fatal("PRODUCT restore protection:", e)
			}
			reader.Close(ctx)
			fresh.ReleaseLifetimeAfterTermination(ctx, targetID, reader.holder.OperationID)
			// Lost successful commit delivery: remote object is real, read and verified
			// through the adapter, while the producer holder remains conservative.
			delivery := &lostCommitStore{Storage: s}
			lostRepo, e := Open(ctx, delivery, identity(), t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			uid := "77777777-7777-4777-8777-777777777777"
			h, e := lostRepo.AdmitBackup(ctx, writerID, uid)
			if e != nil {
				t.Fatal(e)
			}
			a, _, e := h.Begin(ctx, request(uid))
			if e != nil {
				t.Fatal(e)
			}
			cc, mm, ff := capture(t, a, "lost response")
			res, e := a.Publish(ctx, cc, mm, ff)
			if e != nil || res == nil || !delivery.lost {
				t.Fatal("PRODUCT ambiguous commit reconciliation:", e)
			}
			if e = h.Close(ctx); e != ErrUncertain {
				t.Fatal("PRODUCT uncertain write holder released:", e)
			}
			// Separate repository lineage avoids force-clearing the intentionally
			// uncertain producer. Delayed DELETE is dispatched to REAL MinIO only after
			// the owner has returned ambiguity; replacement admission must stay blocked.
			id2 := identity()
			id2.RepositoryID = "88888888-8888-4888-8888-888888888888"
			r2, e := Initialize(ctx, s, id2, t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			// Reuse schema fixture values after binding them to this distinct repository.
			bh, e := r2.AdmitBackup(ctx, writerID, backupID)
			if e != nil {
				t.Fatal(e)
			}
			q := request(backupID)
			q.RepositoryID = id2.RepositoryID
			aa, _, e := bh.Begin(ctx, q)
			if e != nil {
				t.Fatal(e)
			}
			bc, bm, bf := capture(t, aa, "delete target")
			bc.RepositoryID = id2.RepositoryID
			br, e := aa.Publish(ctx, bc, bm, bf)
			if e != nil {
				t.Fatal(e)
			}
			bh.Close(ctx)
			delayed := &delayedDeleteStore{Storage: s}
			r3, e := Open(ctx, delayed, id2, t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			g, e := r3.AcquireGC(ctx)
			if e != nil {
				t.Fatal(e)
			}
			plan := gcplan(g, retire(br), payload(br, 0))
			plan.RepositoryID = id2.RepositoryID
			if e = g.Execute(ctx, plan); e == nil || delayed.key == "" {
				t.Fatal("PRODUCT delayed DELETE did not fire")
			}
			if e = g.Close(ctx); e != ErrUncertain {
				t.Fatal(e)
			}
			restarted, e := Open(ctx, s, id2, t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			if _, e = restarted.AdmitRestore(ctx, targetID, UUID()); e != ErrBlocked {
				t.Fatal("PRODUCT admission before delete drain:", e)
			}
			if e = s.Delete(ctx, delayed.key); e != nil {
				t.Fatal("actual delayed DELETE:", e)
			}
			if _, e = restarted.AdmitRestore(ctx, targetID, UUID()); e != ErrBlocked {
				t.Fatal("PRODUCT automatic ambiguous-owner takeover:", e)
			}
			t.Log("PASS actual MinIO " + sig + ": initialization/CAS, upload/verified winner, protected source-only catalog, lost real commit delivery, delayed actual DELETE/no owner takeover")
		})
	}
}

type lostCommitStore struct {
	Storage
	lost bool
}

func (s *lostCommitStore) PutFile(c context.Context, k string, f *os.File, in s3store.Integrity, cond s3store.Condition, m map[string]string) (s3store.Info, error) {
	i, e := s.Storage.PutFile(c, k, f, in, cond, m)
	if e == nil && strings.HasSuffix(k, "/commit.json") && !s.lost {
		s.lost = true
		return i, &s3store.Error{Kind: s3store.Transient, Ambiguous: true}
	}
	return i, e
}

type delayedDeleteStore struct {
	Storage
	key string
}

func (s *delayedDeleteStore) Delete(_ context.Context, key string) error {
	s.key = key
	return &s3store.Error{Kind: s3store.Transient, Ambiguous: true}
}

func startRepositoryMinIO(t *testing.T) s3store.Config {
	t.Helper()
	binary := os.Getenv("CNPG_S3_MINIO_BINARY")
	f, e := os.Open(binary)
	if e != nil {
		t.Fatal("SETUP pinned MinIO binary unavailable")
	}
	h := sha256.New()
	_, e = io.Copy(h, f)
	f.Close()
	if e != nil || hex.EncodeToString(h.Sum(nil)) != "7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f" {
		t.Fatal("SETUP MinIO binary checksum mismatch")
	}
	root := t.TempDir()
	ca := repositoryCA(t, root)
	listener, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal("SETUP loopback listen failed")
	}
	address := listener.Addr().String()
	listener.Close()
	secret := make([]byte, 24)
	if _, e = rand.Read(secret); e != nil {
		t.Fatal(e)
	}
	c := s3store.Config{Endpoint: "https://" + address, Bucket: "repository-test", Signature: "v4", Addressing: "path", Region: "us-east-1", AccessKey: "repository-test", SecretKey: hex.EncodeToString(secret), CA: ca}
	log, e := os.Create(filepath.Join(root, "minio.log"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		log.Close()
		f, e := os.Open(log.Name())
		if e != nil {
			return
		}
		b, _ := io.ReadAll(io.LimitReader(f, 128<<10))
		f.Close()
		text := strings.ReplaceAll(strings.ReplaceAll(string(b), c.SecretKey, "<REDACTED>"), c.AccessKey, "<REDACTED>")
		if t.Failed() {
			t.Log("bounded redacted MinIO server log:\n" + text)
		}
		if dir := os.Getenv("CNPG_S3_ARTIFACT_DIR"); dir != "" && filepath.IsAbs(dir) {
			if e = os.MkdirAll(dir, 0700); e != nil {
				t.Error(e)
				return
			}
			if e = os.WriteFile(filepath.Join(dir, "repository-minio-server.log"), []byte(text), 0600); e != nil {
				t.Error(e)
			}
		}
	})
	cmd := exec.Command(binary, "server", "--address", address, "--console-address", "127.0.0.1:0", "--certs-dir", filepath.Join(root, "certs"), filepath.Join(root, "data"))
	cmd.Env = []string{"HOME=" + root, "MINIO_ROOT_USER=" + c.AccessKey, "MINIO_ROOT_PASSWORD=" + c.SecretKey, "MINIO_BROWSER=off", "MINIO_UPDATE=off"}
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
	tr := &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: time.Second}
	ready := false
	for range 100 {
		resp, e := client.Get(c.Endpoint + "/minio/health/ready")
		if e == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				ready = true
				break
			}
		}
		select {
		case <-done:
			t.Fatal("SETUP MinIO exited before readiness; product checks NOT executed")
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		t.Fatal("SETUP MinIO readiness deadline")
	}
	core, e := minio.NewCore(address, &minio.Options{Secure: true, Region: c.Region, BucketLookup: minio.BucketLookupPath, MaxRetries: 1, Creds: credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""), Transport: tr})
	if e != nil {
		t.Fatal("SETUP SDK construction failed")
	}
	setupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if e = miniofixture.CreateBucket(setupCtx, core, c.Bucket, c.Region); e != nil {
		re := minio.ToErrorResponse(e)
		code := re.Code
		if len(code) > 80 || strings.ContainsAny(code, " /\r\n") {
			code = "redacted"
		}
		t.Fatalf("SETUP MakeBucket before product checks: sdk_code=%s http_status=%d error_type=%T", code, re.StatusCode, e)
	}
	return c
}
func repositoryCA(t *testing.T, root string) []byte {
	t.Helper()
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "repository-test-ca"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, e := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	roots := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, e = x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	dir := filepath.Join(root, "certs")
	if e = os.Mkdir(dir, 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, "public.crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); e != nil {
		t.Fatal(e)
	}
	kb, e := x509.MarshalECPrivateKey(key)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, "private.key"), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0600); e != nil {
		t.Fatal(e)
	}
	return roots
}
