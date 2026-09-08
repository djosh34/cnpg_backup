package cnpgi

import (
	"context"
	"path/filepath"
	"time"

	wire "github.com/cloudnative-pg/cnpg-i/pkg/wal"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// WALFetch is the narrow credential-free client used directly by PostgreSQL.
// Every unexpected status and every argument/plan/dial/panic failure is fatal;
// only the admitted sidecar may decide an authenticated absence is optional.
func WALFetch(ctx context.Context, args []string) (exit int) {
	exit = 255
	defer func() {
		if recover() != nil {
			exit = 255
		}
	}()
	if len(args) != 5 || args[0] != "--plan" || args[1] != helperPlanPath || args[2] != "--" {
		return 255
	}
	p, e := readRecovery(filepath.Dir(helperPlanPath))
	if e != nil || !p.Materialized {
		return 255
	}
	c, e := ParseCluster(p.ClusterDefinition)
	if e != nil {
		return 255
	}
	if _, e = recoveryDestination(c, args[3], args[4]); e != nil {
		return 255
	}
	ctx, cancel := context.WithTimeout(ctx, 75*time.Second)
	defer cancel()
	conn, e := grpc.NewClient("unix://"+recoveryguard.SocketPath, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(256<<10), grpc.MaxCallSendMsgSize(2<<20)))
	if e != nil {
		return 255
	}
	defer conn.Close()
	return fetchWAL(ctx, wire.NewWALClient(conn), p, args[3], args[4])
}
func fetchWAL(ctx context.Context, client wire.WALClient, p RecoveryPlan, name, destination string) (exit int) {
	exit = 255
	defer func() {
		if recover() != nil {
			exit = 255
		}
	}()
	_, e := client.Restore(ctx, &wire.WALRestoreRequest{ClusterDefinition: p.ClusterDefinition, SourceWalName: name, DestinationFileName: destination, Mode: wire.WALRestoreRequest_MODE_RECOVERY, Parameters: map[string]string{"recoveryID": jsonText(p.Tuple)}})
	if e == nil {
		return 0
	}
	if status.Code(e) == codes.NotFound {
		return 1
	}
	return 255
}
