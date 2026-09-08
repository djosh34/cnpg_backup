package cnpgi

import (
	"context"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	wire "github.com/cloudnative-pg/cnpg-i/pkg/wal"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
	files "github.com/djosh34/cnpg_backup/internal/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// This is a real wire/handler validation check, not a successful archive RPC:
// the full-name control reaches missing local configuration, while the partial
// must not fail earlier at the filename gate. Files tests own durable-byte I/O.
func TestPromotionPartialArchiveRPCValidation(t *testing.T) {
	if _, err := os.Lstat(projectionPath); !os.IsNotExist(err) {
		t.Fatal("RPC validation fixture requires absent deployment configuration; no real storage access allowed")
	}
	listener := bufconn.Listen(64 << 10)
	defer listener.Close()
	server := grpc.NewServer()
	wire.RegisterWALServer(server, &WALService{})
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///wal-test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := wire.NewWALClient(conn)
	const full = "/var/lib/postgresql/wal/pg_wal/000000010000000000000003"
	request := &wire.WALArchiveRequest{ClusterDefinition: walCluster(t), SourceFileName: full}
	_, err = client.Archive(ctx, request)
	control := status.Code(err)
	if control == codes.OK || control == codes.InvalidArgument {
		t.Fatalf("full-name control must reach missing deployment configuration: %v", err)
	}
	request.SourceFileName += ".partial"
	_, err = client.Archive(ctx, request)
	if status.Code(err) != control {
		t.Fatalf("promotion ArchiveWAL fails before same configuration boundary: full=%v partial=%v", control, err)
	}
}

type partialOnlyWALStore struct {
	*recoveryWALStore
	keys []string
}

func (s *partialOnlyWALStore) Head(ctx context.Context, key string) (s3store.Info, error) {
	s.keys = append(s.keys, key)
	if strings.HasSuffix(key, ".partial") {
		return s.recoveryWALStore.Head(ctx, key)
	}
	return s3store.Info{}, &s3store.Error{Kind: s3store.NotFound}
}

func TestPromotionPartialDoesNotRelaxHelperFailures(t *testing.T) {
	p := recoveryPlanFixture(t)
	p.Plan.RequiredArchive = []repository.WALRange{{Timeline: 1, StartLSN: "0/100100", EndLSN: "0/200000"}}
	p.Materialized = true
	store := &partialOnlyWALStore{recoveryWALStore: &recoveryWALStore{id: p.Plan.Source}}
	repo, err := repository.OpenSource(context.Background(), store, p.Plan.Source.RepositoryID, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w := files.Files{Repository: repo, Store: store, Workspace: t.TempDir(), Compression: "none"}
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	// A full request must never probe an alternate .partial, even if the
	// auxiliary key is live. This intentionally invalid payload is unreachable.
	store.body = []byte("partial must not be read for full-name gap")
	const full = "000000010000000000000001"
	err = restoreSourceWAL(context.Background(), w, p, full, root, "RECOVERYXLOG")
	if status.Code(walError(err)) != codes.FailedPrecondition || len(store.keys) != 1 || !strings.HasSuffix(store.keys[0], "/"+full) {
		t.Fatal("required full gap tried auxiliary substitute", err, store.keys)
	}
	store.body = nil
	err = restoreSourceWAL(context.Background(), w, p, "000000010000000000000002.partial", root, "RECOVERYXLOG")
	code := status.Code(walError(err))
	if code != codes.FailedPrecondition || fetchWAL(context.Background(), &fetchClient{code: code}, p, full, "pg_wal/RECOVERYXLOG") != 255 {
		t.Fatal("missing auxiliary became optional future EOF", err)
	}
}

func TestPromotionPartialArchiveCallbackPath(t *testing.T) {
	cluster := walCluster(t)
	const name = "000000010000000000000003.partial"
	for _, root := range []string{pgdataPath + "/pg_wal/", "/var/lib/postgresql/wal/pg_wal/"} {
		if err := validateWALPaths(cluster, root+name, "", false); err != nil {
			t.Errorf("CNPG promotion ArchiveWAL path %q rejected before storage: %v", root+name, err)
		}
		for _, bad := range []string{"../" + name, "nested/" + name, name + ".gz", name + ".partial", name + "\";bad"} {
			if err := validateWALPaths(cluster, root+bad, "", false); err == nil {
				t.Fatalf("unsafe auxiliary path accepted: %q", root+bad)
			}
		}
	}
}
