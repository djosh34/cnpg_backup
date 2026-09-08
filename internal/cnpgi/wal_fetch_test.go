package cnpgi

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	wire "github.com/cloudnative-pg/cnpg-i/pkg/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type walResultServer struct {
	wire.UnimplementedWALServer
	code     atomic.Uint32
	requests chan *wire.WALRestoreRequest
}

func (s *walResultServer) Restore(_ context.Context, r *wire.WALRestoreRequest) (*wire.WALRestoreResult, error) {
	s.requests <- r
	code := codes.Code(s.code.Load())
	if code == codes.OK {
		return &wire.WALRestoreResult{}, nil
	}
	return nil, status.Error(code, "bounded test fault")
}
func TestHelperUsesActualExistingUnixWALWire(t *testing.T) {
	directory, e := os.MkdirTemp("", "wal-rpc-")
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(directory)
	socket := filepath.Join(directory, "socket")
	listener, e := net.Listen("unix", socket)
	if e != nil {
		t.Fatal(e)
	}
	defer listener.Close()
	implementation := &walResultServer{requests: make(chan *wire.WALRestoreRequest, 1)}
	server := grpc.NewServer()
	wire.RegisterWALServer(server, implementation)
	go server.Serve(listener)
	defer server.Stop()
	conn, e := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	p := recoveryPlanFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, item := range []struct {
		code codes.Code
		exit int
	}{{codes.OK, 0}, {codes.NotFound, 1}, {codes.DataLoss, 255}, {codes.PermissionDenied, 255}, {codes.Unavailable, 255}, {codes.FailedPrecondition, 255}} {
		implementation.code.Store(uint32(item.code))
		exit := fetchWAL(ctx, wire.NewWALClient(conn), p, "000000010000000000000001", "pg_wal/RECOVERYXLOG")
		if exit != item.exit {
			t.Fatal(item.code, exit)
		}
		request := <-implementation.requests
		if request.Parameters["recoveryID"] != jsonText(p.Tuple) || string(request.ClusterDefinition) != string(p.ClusterDefinition) {
			t.Fatal("wire lost original source-session binding")
		}
	}
	server.Stop()
	if code := fetchWAL(ctx, wire.NewWALClient(conn), p, "000000010000000000000001", "pg_wal/RECOVERYXLOG"); code != 255 {
		t.Fatal("lost socket became EOF", code)
	}
}
func TestHelperPanicIsFatal(t *testing.T) {
	// A nil generated client panics before an RPC result; this exercises the
	// exact helper call boundary, not PostgreSQL's unsafe default panic exit2.
	if exit := fetchWAL(context.Background(), nil, recoveryPlanFixture(t), "000000010000000000000001", "pg_wal/RECOVERYXLOG"); exit != 255 {
		t.Fatal(exit)
	}
}
func TestTargetSyntaxAndDestinationNeverInterpolateUserInput(t *testing.T) {
	for _, v := range []map[string]any{
		{"targetTLI": "latest"}, {"targetTLI": "current"}, {"targetTLI": "0"}, {"targetTLI": "01"}, {"targetName": "point"}, {"targetXID": "123"}, {"targetImmediate": true}, {"targetTime": "2026-09-07 12:00:00"}, {"targetLSN": "0/20", "targetName": "other"}, {"unknown": "x"}, {"exclusive": true},
	} {
		if _, e := recoveryTarget(v); e == nil {
			t.Fatal("accepted target", v)
		}
	}
	p := recoveryPlanFixture(t)
	c, e := ParseCluster(p.ClusterDefinition)
	if e != nil {
		t.Fatal(e)
	}
	for _, path := range []string{"pg_wal/../postgresql.conf", "pg_wal/RECOVERYXLOG\";anything", "/tmp/RECOVERYXLOG", "pg_wal/$bad", "pg_wal/RECOVERYXLOG\n", "/var/lib/postgresql/wal/pg_wal/../bad"} {
		if _, e = recoveryDestination(c, "000000010000000000000001", path); e == nil {
			t.Fatal("accepted destination", path)
		}
	}
	for _, path := range []string{"pg_wal/RECOVERYXLOG", pgdataPath + "/pg_wal/RECOVERYXLOG", "/var/lib/postgresql/wal/pg_wal/RECOVERYXLOG"} {
		if _, e = recoveryDestination(c, "000000010000000000000001", path); e != nil {
			t.Fatal(path, e)
		}
	}
}
