package cnpgi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	wire "github.com/cloudnative-pg/cnpg-i/pkg/wal"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
	files "github.com/djosh34/cnpg_backup/internal/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testHash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func recoveryPlanFixture(t *testing.T) RecoveryPlan {
	_, c, _ := fixture(t, true)
	id := repository.Identity{Schema: 1, RepositoryID: "33333333-3333-4333-8333-333333333333", PostgresMajor: 18, SystemIdentifier: "123456789", WALSegmentBytes: 1 << 20, WriterClusterUID: "77777777-7777-4777-8777-777777777777", CreatedAt: "2026-09-07T00:00:00Z"}
	uid := "88888888-8888-4888-8888-888888888888"
	hash := strings.Repeat("a", 64)
	commit := repository.Commit{Schema: 1, RepositoryID: id.RepositoryID, BackupUID: uid, AttemptID: repository.UUID(), RequestSHA256: hash, Kind: "full", RootBackupUID: uid, SystemIdentifier: id.SystemIdentifier, PostgresMajor: 18, ToolVersion: "18.6", Timeline: 1, ChecksumVersion: 1, CaptureInstanceUID: repository.UUID(), PostmasterStartedAt: "2026-09-07T00:00:00Z", StartedAt: "2026-09-07T01:00:00Z", StoppedAt: "2026-09-07T01:01:00Z", StartLSN: "0/100028", StopLSN: "0/100100", RedoLSN: "0/100028", BundledWALStartLSN: "0/100028", BundledWALEndLSN: "0/100100", WALRanges: []repository.WALRange{{Timeline: 1, StartLSN: "0/100028", EndLSN: "0/100100"}}, BackupLabel: "native backup label", Tablespaces: []repository.Tablespace{}, ManifestBytes: 1024, ManifestSHA256: hash, Artifacts: []repository.Artifact{{Index: 0, Role: "base", Compression: "none", StoredBytes: 1024, RawBytes: 1024, StoredSHA256: hash, RawSHA256: hash}, {Index: 1, Role: "wal", Compression: "none", StoredBytes: 1024, RawBytes: 1024, StoredSHA256: hash, RawSHA256: hash}}}
	plan := repository.Plan{Schema: 1, Source: id, DestinationRepositoryID: "22222222-2222-4222-8222-222222222222", TargetClusterUID: string(c.Metadata.UID), BootstrapSHA256: c.BootstrapFingerprint(), OperationID: c.OperationUID(), LifetimeHoldID: c.OperationUID(), ReaderHoldID: repository.UUID(), Target: repository.Target{Kind: "latest", Timeline: 1}, Path: []repository.Timeline{{ID: 1}}, Chain: []repository.Commit{commit}, RequiredArchive: []repository.WALRange{}}
	p := RecoveryPlan{Plan: plan, Tuple: recoveryguard.Tuple{Owner: recoveryguard.Owner{ClusterUID: string(c.Metadata.UID), OperationUID: c.OperationUID(), PodUID: repository.UUID(), GuardUID: repository.UUID()}, SidecarUID: repository.UUID()}, ClusterDefinition: raw(recoveryCluster(c)), Bundled: map[string]s3store.Integrity{}}
	if e := p.validate(); e != nil {
		t.Fatal(e)
	}
	return p
}

type recoveryWALStore struct {
	repository.Storage
	id        repository.Identity
	body      []byte
	metadata  map[string]string
	fault     error
	headFault error
	corrupt   bool
	lookups   int
}

func (s *recoveryWALStore) Read(_ context.Context, key string, _ int64) ([]byte, s3store.Info, error) {
	if strings.HasSuffix(key, "repository.json") {
		return raw(s.id), s3store.Info{ETag: "identity"}, nil
	}
	if strings.HasSuffix(key, "gate.json") {
		return raw(repository.Gate{Schema: 1, RepositoryID: s.id.RepositoryID, Generation: "0", Nonce: repository.UUID(), Holders: []repository.Holder{}}), s3store.Info{ETag: "gate"}, nil
	}
	s.lookups++
	if s.fault != nil {
		return nil, s3store.Info{}, s.fault
	}
	return nil, s3store.Info{}, &s3store.Error{Kind: s3store.NotFound}
}
func (s *recoveryWALStore) Head(_ context.Context, _ string) (s3store.Info, error) {
	s.lookups++
	if s.headFault != nil {
		return s3store.Info{}, s.headFault
	}
	if s.fault != nil {
		return s3store.Info{}, s.fault
	}
	if s.body == nil {
		return s3store.Info{}, &s3store.Error{Kind: s3store.NotFound}
	}
	return s3store.Info{Size: int64(len(s.body)), Metadata: s.metadata}, nil
}
func (s *recoveryWALStore) DownloadWAL(_ context.Context, _ string, f *os.File, _ s3store.Integrity) (s3store.Info, error) {
	b := append([]byte(nil), s.body...)
	if s.corrupt {
		b[len(b)-1] ^= 1
	}
	_, e := f.Write(b)
	return s3store.Info{Size: int64(len(b)), Metadata: s.metadata}, e
}
func TestActualArchiveFirstBundleClassification(t *testing.T) {
	for _, name := range []string{"allowed duplicate", "required same segment", "TLS", "auth", "corrupt", "retired", "archive preferred", "local missing", "local changed", "future tail", "old unexpected missing", "HEAD auth then miss", "HEAD transport then miss", "HEAD corruption then miss"} {
		t.Run(name, func(t *testing.T) {
			p := recoveryPlanFixture(t)
			bundle := bytes.Repeat([]byte{0x41}, 1<<20)
			walName := "000000010000000000000001"
			p.Bundled[walName] = s3store.Integrity{Size: int64(len(bundle)), SHA256: testHash(bundle)}
			p.Materialized = true
			store := &recoveryWALStore{id: p.Plan.Source}
			dir := t.TempDir()
			if e := os.WriteFile(filepath.Join(dir, walName), bundle, 0600); e != nil {
				t.Fatal(e)
			}
			want := codes.NotFound
			switch name {
			case "required same segment":
				p.Plan.RequiredArchive = []repository.WALRange{{Timeline: 1, StartLSN: "0/100100", EndLSN: "0/200000"}}
				want = codes.FailedPrecondition
			case "HEAD auth then miss":
				store.headFault = &s3store.Error{Kind: s3store.Auth}
				want = codes.PermissionDenied
			case "HEAD transport then miss":
				store.headFault = &s3store.Error{Kind: s3store.Unknown}
				want = codes.Unavailable
			case "HEAD corruption then miss":
				store.headFault = &s3store.Error{Kind: s3store.Corrupt}
				want = codes.DataLoss
			case "TLS":
				store.fault = &s3store.Error{Kind: s3store.TLS}
				want = codes.Unavailable
			case "auth":
				store.fault = &s3store.Error{Kind: s3store.Auth}
				want = codes.PermissionDenied
			case "local missing":
				os.Remove(filepath.Join(dir, walName))
				want = codes.DataLoss
			case "local changed":
				os.WriteFile(filepath.Join(dir, walName), bytes.Repeat([]byte{0x42}, 1<<20), 0600)
				want = codes.DataLoss
			case "future tail":
				walName = "000000010000000000000002"
			case "old unexpected missing":
				walName = "000000010000000000000000"
				want = codes.FailedPrecondition
			case "archive preferred", "corrupt", "retired":
				store.body = bytes.Repeat([]byte{0x7a}, 1<<20)
				store.metadata = map[string]string{"cnpg-format": "wal-v1", "cnpg-system-id": p.Plan.Source.SystemIdentifier, "cnpg-raw-bytes": "1048576", "cnpg-raw-sha256": testHash(store.body), "cnpg-stored-sha256": testHash(store.body), "cnpg-compression": "none"}
				want = codes.OK
				if name == "corrupt" {
					store.corrupt = true
					want = codes.DataLoss
				}
				if name == "retired" {
					store.metadata["cnpg-format"] = "wal-retired-v1"
					want = codes.FailedPrecondition
				}
			}
			repo, e := repository.OpenSource(context.Background(), store, p.Plan.Source.RepositoryID, t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			source := files.Files{Repository: repo, Store: store, Workspace: t.TempDir(), Compression: "none"}
			root, e := os.OpenRoot(dir)
			if e != nil {
				t.Fatal(e)
			}
			defer root.Close()
			e = restoreSourceWAL(context.Background(), source, p, walName, root, "RECOVERYXLOG")
			if code := status.Code(walError(e)); code != want {
				t.Fatalf("%v want %v: %v", code, want, e)
			}
			if store.lookups == 0 {
				t.Fatal("did not attempt archive first")
			}
			output, e := os.ReadFile(filepath.Join(dir, "RECOVERYXLOG"))
			if want == codes.OK {
				if e != nil || !bytes.Equal(output, store.body) {
					t.Fatal("bundle replaced the full archive")
				}
			} else if !os.IsNotExist(e) {
				t.Fatal("published data on failed/missing archive")
			}
		})
	}
}

type fetchClient struct {
	wire.WALClient
	code    codes.Code
	request *wire.WALRestoreRequest
}

func (c *fetchClient) Restore(_ context.Context, r *wire.WALRestoreRequest, _ ...grpc.CallOption) (*wire.WALRestoreResult, error) {
	c.request = r
	if c.code == codes.OK {
		return &wire.WALRestoreResult{}, nil
	}
	return nil, status.Error(c.code, "test redacted")
}
func TestHelperExactRPCAndEveryErrorFatal(t *testing.T) {
	p := recoveryPlanFixture(t)
	for code := codes.OK; code <= codes.Unauthenticated; code++ {
		client := &fetchClient{code: code}
		want := 255
		if code == codes.OK {
			want = 0
		}
		if code == codes.NotFound {
			want = 1
		}
		if got := fetchWAL(context.Background(), client, p, "000000010000000000000001", "pg_wal/RECOVERYXLOG"); got != want {
			t.Fatal(code, got, want)
		}
		if !bytes.Equal(client.request.ClusterDefinition, p.ClusterDefinition) || client.request.Parameters["recoveryID"] != jsonText(p.Tuple) || client.request.Mode != wire.WALRestoreRequest_MODE_RECOVERY {
			t.Fatal("helper did not bind exact plan tuple")
		}
	}
	for _, args := range [][]string{nil, {"--help"}, {"--plan", "/elsewhere", "--", "000000010000000000000001", "pg_wal/RECOVERYXLOG"}, {"--plan", helperPlanPath, "--", "bad", "bad"}} {
		if WALFetch(context.Background(), args) != 255 {
			t.Fatal("CLI failure was ordinary EOF")
		}
	}
}
func TestDurableRecoveryEnvelopeAndAuthoritativeOperation(t *testing.T) {
	p := recoveryPlanFixture(t)
	dir := t.TempDir()
	if e := saveRecovery(dir, p); e != nil {
		t.Fatal(e)
	}
	got, e := readRecovery(dir)
	if e != nil || jsonText(got) != jsonText(p) {
		t.Fatal(e)
	}
	st, e := os.Stat(filepath.Join(dir, "recovery.json"))
	if e != nil || st.Mode().Perm() != 0600 {
		t.Fatal("unsafe plan permissions")
	}
	c, e := ParseCluster(p.ClusterDefinition)
	if e != nil {
		t.Fatal(e)
	}
	op, e := repository.RestoreOperationID(string(c.Metadata.UID), c.BootstrapFingerprint())
	if e != nil || op != c.OperationUID() {
		t.Fatal("multiple operation derivations")
	}
	b, _ := os.ReadFile(filepath.Join(dir, "recovery.json"))
	if bytes.Contains(b, []byte("test-secret")) {
		t.Fatal("credential leak")
	}
	if e = os.WriteFile(filepath.Join(dir, "recovery.json"), append(b, []byte("{}")...), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = readRecovery(dir); e == nil {
		t.Fatal("accepted trailing plan")
	}
}
