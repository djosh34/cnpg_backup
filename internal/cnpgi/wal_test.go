package cnpgi

import (
	"context"
	"encoding/json"
	"testing"

	wire "github.com/cloudnative-pg/cnpg-i/pkg/wal"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
	files "github.com/djosh34/cnpg_backup/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func walCluster(t *testing.T) []byte {
	t.Helper()
	c := map[string]any{"apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster", "metadata": map[string]any{"name": "database", "namespace": "fixture", "uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}, "spec": map[string]any{"imageName": "pg:18.6@" + DatabaseDigest, "walStorage": map[string]any{"size": "1Gi"}, "plugins": []any{map[string]any{"name": "cnpg-backup.djosh34.github.io", "isWALArchiver": true, "parameters": map[string]string{"repository": "destination"}}}}}
	b, e := json.Marshal(c)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func TestWALPathsFailBeforeAnyProjectionOrStorageIO(t *testing.T) {
	c := walCluster(t)
	name := "000000010000000000000001"
	for _, path := range []string{pgdataPath + "/pg_wal/" + name, "/var/lib/postgresql/wal/pg_wal/" + name} {
		if e := validateWALPaths(c, path, "", false); e != nil {
			t.Fatal("actual pinned BuildWALPath", e)
		}
		if e := validateWALPaths(c, path, name, true); e != nil {
			t.Fatal("exact rewind", e)
		}
	}
	for _, path := range []string{"pg_wal/" + name, pgdataPath + "/pg_wal/../escape", "/tmp/" + name, pgdataPath + "/pg_wal/a\";bad", pgdataPath + "/pg_wal/nested/" + name} {
		for _, empty := range []*bool{nil, ptr(false), ptr(true)} {
			_, e := (&WALService{}).Archive(context.Background(), &wire.WALArchiveRequest{ClusterDefinition: c, SourceFileName: path, CheckEmptyWalArchive: empty})
			if status.Code(e) != codes.InvalidArgument {
				t.Fatal("invalid request reached missing projection", e)
			}
		}
	}
	for _, mode := range []wire.WALRestoreRequest_Mode{wire.WALRestoreRequest_MODE_UNSPECIFIED, wire.WALRestoreRequest_MODE_RECOVERY, wire.WALRestoreRequest_MODE_REWIND} {
		_, e := (&WALService{}).Restore(context.Background(), &wire.WALRestoreRequest{ClusterDefinition: c, SourceWalName: name, DestinationFileName: pgdataPath + "/pg_wal/RECOVERYXLOG", Mode: mode, Parameters: map[string]string{"recoveryID": "old-source-plan"}})
		if status.Code(e) != codes.InvalidArgument {
			t.Fatal("ordinary mode admitted source token", e)
		}
	}
	parsed, e := ParseCluster(c)
	if e != nil {
		t.Fatal(e)
	}
	p := WALPlacement{ClusterUID: string(parsed.Metadata.UID), Namespace: "fixture", Cluster: "database", Repository: "destination"}
	if e = validateWALRequest(parsed, p); e != nil {
		t.Fatal(e)
	}
	p.ClusterUID = repository.UUID()
	if e = validateWALRequest(parsed, p); e == nil {
		t.Fatal("foreign target writer accepted")
	}
}
func TestConfiguredSingleWALSlotCoversClientSnapshots(t *testing.T) {
	first := walOperation{single: true, ctx: context.Background()}
	release, e := first.dataSlot()
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	second := walOperation{single: true, ctx: ctx}
	if unlock, e := second.dataSlot(); e == nil {
		unlock()
		t.Fatal("second client snapshot bypassed one-slot limit")
	}
	release()
	second.ctx = context.Background()
	release, e = second.dataSlot()
	if e != nil {
		t.Fatal(e)
	}
	release()
}

func TestWALMissingErrorClassificationAndCapabilities(t *testing.T) {
	for _, tc := range []struct {
		e    error
		code codes.Code
	}{
		{&s3store.Error{Kind: s3store.NotFound}, codes.NotFound},
		{&s3store.Error{Kind: s3store.Transient}, codes.Unavailable},
		{&s3store.Error{Kind: s3store.TLS}, codes.Unavailable},
		{&s3store.Error{Kind: s3store.Auth}, codes.PermissionDenied},
		{files.ErrExpired, codes.FailedPrecondition}, {files.ErrConflict, codes.AlreadyExists},
		{files.ErrCorrupt, codes.DataLoss}, {files.ErrLocal, codes.ResourceExhausted},
		{repository.ErrIdentity, codes.FailedPrecondition},
	} {
		if got := status.Code(walError(tc.e)); got != tc.code {
			t.Fatal(got, tc.code)
		}
	}
	service := &WALService{}
	caps, e := service.GetCapabilities(context.Background(), &wire.WALCapabilitiesRequest{})
	if e != nil || len(caps.Capabilities) != 2 {
		t.Fatal(e)
	}
	if caps.Capabilities[0].GetRpc().Type != wire.WALCapability_RPC_TYPE_ARCHIVE_WAL || caps.Capabilities[1].GetRpc().Type != wire.WALCapability_RPC_TYPE_RESTORE_WAL {
		t.Fatal("unimplemented capability")
	}
	if _, e = service.Status(context.Background(), &wire.WALStatusRequest{}); status.Code(e) != codes.Unimplemented {
		t.Fatal(e)
	}
	if _, e = service.SetFirstRequired(context.Background(), &wire.SetFirstRequiredRequest{}); status.Code(e) != codes.Unimplemented {
		t.Fatal(e)
	}
}
