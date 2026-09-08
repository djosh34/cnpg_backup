package cnpgi

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	wire "github.com/cloudnative-pg/cnpg-i/pkg/backup"
	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func fullRequest(t *testing.T, kind string) *wire.BackupRequest {
	t.Helper()
	b := backupDefinition{APIVersion: "postgresql.cnpg.io/v1", Kind: "Backup", Metadata: meta.ObjectMeta{Name: "full", Namespace: "fixture", UID: types.UID("11111111-1111-4111-8111-111111111111")}}
	b.Spec.Cluster.Name = "database"
	b.Spec.Method = "plugin"
	b.Spec.Target = "primary"
	b.Spec.Plugin.Name = recoveryguard.PluginName
	b.Spec.Plugin.Parameters = map[string]string{"backupType": kind}
	b.Status.Phase = "started"
	b.Status.InstanceID.PodName = "database-1"
	data, _ := json.Marshal(b)
	return &wire.BackupRequest{ClusterDefinition: walCluster(t), BackupDefinition: data, Parameters: b.Spec.Plugin.Parameters}
}
func TestFullRPCValidationAndNoDifferentialFallback(t *testing.T) {
	service := &BackupService{}
	if _, _, e := parseBackup(fullRequest(t, "full")); e != nil {
		t.Fatal(e)
	}
	for _, kind := range []string{"", "incremental", "differential"} {
		_, e := service.Backup(context.Background(), fullRequest(t, kind))
		want := codes.InvalidArgument
		if kind == "differential" {
			want = codes.FailedPrecondition
		}
		if status.Code(e) != want {
			t.Fatal("invalid request admitted or missing projection did not fail requested differential", kind, e)
		}
	}
	for _, mutate := range []func(*backupDefinition){func(b *backupDefinition) { b.Spec.Target = "prefer-standby" }, func(b *backupDefinition) { b.Status.Phase = "failed" }, func(b *backupDefinition) { b.Status.Phase = "completed" }, func(b *backupDefinition) { b.Metadata.Namespace = "foreign" }, func(b *backupDefinition) {
		b.Spec.Plugin.Parameters = map[string]string{"backupType": "full", "fallback": "full"}
	}} {
		r := fullRequest(t, "full")
		var b backupDefinition
		json.Unmarshal(r.BackupDefinition, &b)
		mutate(&b)
		r.BackupDefinition, _ = json.Marshal(b)
		_, e := service.Backup(context.Background(), r)
		if status.Code(e) != codes.InvalidArgument {
			t.Fatal("invalid request reached I/O", e)
		}
	}
	capabilities, e := service.GetCapabilities(context.Background(), &wire.BackupCapabilitiesRequest{})
	if e != nil || len(capabilities.Capabilities) != 1 || capabilities.Capabilities[0].GetRpc().Type != wire.BackupCapability_RPC_TYPE_BACKUP {
		t.Fatal(capabilities, e)
	}
	caps, e := (Identity{WAL: true, Backup: true}).GetPluginCapabilities(context.Background(), &identity.GetPluginCapabilitiesRequest{})
	if e != nil || len(caps.Capabilities) != 2 {
		t.Fatal(caps, e)
	}
	for _, c := range caps.Capabilities {
		v := c.GetService().Type
		if v != identity.PluginCapability_Service_TYPE_WAL_SERVICE && v != identity.PluginCapability_Service_TYPE_BACKUP_SERVICE {
			t.Fatal("unimplemented primary restore advertised")
		}
	}
}
func TestBackupResponseUsesWinningNativeBoundaries(t *testing.T) {
	r := &repository.Result{Commit: repository.Commit{BackupUID: "winning-uid", Kind: "full", RootBackupUID: "winning-uid", RepositoryID: "repository", StartedAt: "2026-09-07T01:00:00Z", StoppedAt: "2026-09-07T01:01:00Z", StartLSN: "0/1000028", StopLSN: "0/2000000", Timeline: 1, BackupLabel: "original-label", TablespaceMap: "original-map", CaptureInstanceUID: "original-pod"}, PublishedAt: time.Date(2026, 9, 7, 1, 20, 0, 0, time.UTC)}
	result := backupResult(r, 16<<20)
	if result.BackupId != "winning-uid" || result.EndWal != "000000010000000000000001" || result.BeginLsn != r.Commit.StartLSN || result.EndLsn != r.Commit.StopLSN || result.Metadata["publishedAt"] != "2026-09-07T01:20:00Z" || !result.Online || string(result.BackupLabelFile) != "original-label" || string(result.TablespaceMapFile) != "original-map" {
		t.Fatal(result)
	}
	expected, _ := time.Parse(time.RFC3339, r.Commit.StoppedAt)
	if result.StoppedAt != expected.Unix() {
		t.Fatal("substituted RPC or publication clock")
	}
}
