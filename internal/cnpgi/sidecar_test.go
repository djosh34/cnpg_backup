package cnpgi

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cloudnative-pg/cnpg-i/pkg/backup"
	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
	job "github.com/cloudnative-pg/cnpg-i/pkg/restore/job"
	"github.com/cloudnative-pg/cnpg-i/pkg/wal"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestSidecarServices(t *testing.T) {
	for _, mode := range []string{"instance", "recovery"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var admission *recoveryguard.Admission
			if mode == "recovery" {
				config := recoveryguard.Config{ClusterUID: recoveryguard.NewUUID(), OperationUID: recoveryguard.NewUUID(), Targets: []recoveryguard.Target{{PVCUID: recoveryguard.NewUUID(), Mount: "/var/lib/postgresql/data"}}}
				var err error
				admission, err = recoveryguard.NewAdmission(config, recoveryguard.NewUUID())
				if err != nil {
					t.Fatal(err)
				}
				defer admission.Close()
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			done := make(chan error, 1)
			go func() { done <- Serve(ctx, listener, admission, "test") }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Error("sidecar did not stop")
				}
			})
			conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			capabilities, err := identity.NewIdentityClient(conn).GetPluginCapabilities(ctx, &identity.GetPluginCapabilitiesRequest{}, grpc.WaitForReady(true))
			if err != nil {
				t.Fatal(err)
			}
			if len(capabilities.Capabilities) != 2 {
				t.Fatalf("capabilities: %v", capabilities)
			}
			if _, err := wal.NewWALClient(conn).GetCapabilities(ctx, &wal.WALCapabilitiesRequest{}); err != nil {
				t.Fatalf("WAL capabilities: %v", err)
			}
			_, backupErr := backup.NewBackupClient(conn).GetCapabilities(ctx, &backup.BackupCapabilitiesRequest{})
			_, restoreErr := job.NewRestoreJobHooksClient(conn).GetCapabilities(ctx, &job.RestoreJobHooksCapabilitiesRequest{})
			wantBackup, wantRestore := codes.OK, codes.Unimplemented
			if mode == "recovery" {
				wantBackup, wantRestore = codes.Unimplemented, codes.OK
			}
			if status.Code(backupErr) != wantBackup || status.Code(restoreErr) != wantRestore {
				t.Fatalf("backup: %v; restore: %v", backupErr, restoreErr)
			}
		})
	}
}
