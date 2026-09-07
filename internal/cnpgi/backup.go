// Copyright 2026 cnpg_backup contributors. All rights reserved.
package cnpgi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"reflect"
	"time"

	wire "github.com/cloudnative-pg/cnpg-i/pkg/backup"
	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/postgres"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var captureSlot = make(chan struct{}, 1)

type BackupService struct{ wire.UnimplementedBackupServer }
type backupDefinition struct {
	APIVersion string          `json:"apiVersion"`
	Kind       string          `json:"kind"`
	Metadata   meta.ObjectMeta `json:"metadata"`
	Spec       struct {
		Cluster struct {
			Name string `json:"name"`
		} `json:"cluster"`
		Method string `json:"method"`
		Target string `json:"target"`
		Plugin struct {
			Name       string            `json:"name"`
			Parameters map[string]string `json:"parameters"`
		} `json:"pluginConfiguration"`
	} `json:"spec"`
	Status struct {
		Phase      string `json:"phase"`
		InstanceID struct {
			PodName string `json:"podName"`
		} `json:"instanceID"`
	} `json:"status"`
}

func (*BackupService) GetCapabilities(context.Context, *wire.BackupCapabilitiesRequest) (*wire.BackupCapabilitiesResult, error) {
	return &wire.BackupCapabilitiesResult{Capabilities: []*wire.BackupCapability{{Type: &wire.BackupCapability_Rpc{Rpc: &wire.BackupCapability_RPC{Type: wire.BackupCapability_RPC_TYPE_BACKUP}}}}}, nil
}
func parseBackup(r *wire.BackupRequest) (Cluster, backupDefinition, error) {
	var b backupDefinition
	if r == nil || len(r.BackupDefinition) > 1<<20 || json.Unmarshal(r.BackupDefinition, &b) != nil {
		return Cluster{}, b, errors.New("invalid backup definition")
	}
	c, e := ParseCluster(r.ClusterDefinition)
	if e != nil {
		return c, b, e
	}
	if b.APIVersion != "postgresql.cnpg.io/v1" || b.Kind != "Backup" || b.Metadata.UID == "" || b.Metadata.Namespace != c.Metadata.Namespace || b.Spec.Cluster.Name != c.Metadata.Name || b.Spec.Method != "plugin" || b.Spec.Plugin.Name != recoveryguard.PluginName || !reflect.DeepEqual(r.Parameters, b.Spec.Plugin.Parameters) {
		return c, b, errors.New("backup identity mismatch")
	}
	if e = ValidateBackup(b.Spec.Target, r.Parameters); e != nil {
		return c, b, e
	}
	if b.Status.Phase == "failed" || b.Status.Phase == "completed" {
		return c, b, errors.New("terminal Backup cannot execute again")
	}
	return c, b, nil
}
func backupResult(r *repository.Result, segment int64) *wire.BackupResult {
	c := r.Commit
	start, _ := time.Parse(time.RFC3339Nano, c.StartedAt)
	stop, _ := time.Parse(time.RFC3339Nano, c.StoppedAt)
	a, _ := repository.ParseLSN(c.StartLSN)
	b, _ := repository.ParseLSN(c.StopLSN)
	return &wire.BackupResult{BackupId: c.BackupUID, StartedAt: start.Unix(), StoppedAt: stop.Unix(), BeginWal: postgres.WALFilename(c.Timeline, a, segment), EndWal: postgres.WALFilename(c.Timeline, b-1, segment), BeginLsn: c.StartLSN, EndLsn: c.StopLSN, BackupLabelFile: []byte(c.BackupLabel), TablespaceMapFile: []byte(c.TablespaceMap), InstanceId: c.CaptureInstanceUID, Online: true, Metadata: map[string]string{"backupType": c.Kind, "repositoryID": c.RepositoryID, "rootBackupID": c.RootBackupUID, "formatVersion": "1", "publishedAt": r.PublishedAt.UTC().Format(time.RFC3339Nano), "bootstrapWAL": "bundled-verified; no post-backup coverage claim"}}
}
func (*BackupService) Backup(ctx context.Context, r *wire.BackupRequest) (result *wire.BackupResult, err error) {
	start := time.Now()
	defer func() {
		slog.Info("backup callback", "success", err == nil, "duration_seconds", time.Since(start).Seconds(), "code", status.Code(err).String())
	}()
	c, b, e := parseBackup(r)
	if e != nil {
		return nil, status.Error(codes.InvalidArgument, "backup requires matching UID, primary and explicit requested type")
	}
	if r.Parameters["backupType"] != "full" {
		return nil, status.Error(codes.Unimplemented, "differential capture is not implemented; no full fallback")
	}
	ctx, cancel := context.WithTimeout(ctx, 7*24*time.Hour)
	defer cancel()
	select {
	case captureSlot <- struct{}{}:
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	defer func() { <-captureSlot }()
	root, e := configuration.Projection(projectionPath)
	if e != nil {
		return nil, backupError(e)
	}
	defer root.Close()
	bytes, e := configuration.Read(root, "wal.json", 64<<10)
	if e != nil {
		return nil, backupError(e)
	}
	var placement WALPlacement
	if configuration.StrictJSON(bytes, &placement) != nil || validateWALRequest(c, placement) != nil {
		return nil, status.Error(codes.InvalidArgument, "backup Cluster differs from placement")
	}
	hostname, e := os.Hostname()
	if e != nil || b.Status.InstanceID.PodName == "" || b.Status.InstanceID.PodName != hostname {
		return nil, status.Error(codes.FailedPrecondition, "backup must run on CNPG selected local instance")
	}
	capture, e := postgres.OpenCapture(ctx, root)
	if e != nil {
		return nil, backupError(e)
	}
	defer capture.Close()
	spec := capture.Snapshot.Repository.Spec
	duration, _ := time.ParseDuration(spec.IO.OperationTimeout)
	ctx, stop := context.WithDeadline(ctx, start.Add(duration))
	defer stop()
	store, e := capture.Snapshot.Repository.Store()
	if e != nil {
		return nil, backupError(e)
	}
	defer store.Close()
	if e = store.CheckBucketSafety(ctx); e != nil {
		return nil, backupError(e)
	}
	repo, e := repository.OpenWriter(ctx, store, capture.Identity(spec.RepositoryID, placement.ClusterUID), capture.Directory)
	if e != nil {
		return nil, backupError(e)
	}
	hold, e := repo.AdmitBackup(ctx, placement.ClusterUID, string(b.Metadata.UID))
	if e != nil {
		return nil, backupError(e)
	}
	defer func() {
		clean, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if ce := hold.Close(clean); ce != nil {
			slog.Warn("backup repository holder retained", "reason", "uncertain or undrained operation")
		}
	}()
	// Credential/trust rotation and deadline changes do not change UID semantics.
	semantic, _ := json.Marshal(struct {
		Identity    repository.Identity
		Compression string
		Connection  postgres.Connection
		Tool        string
	}{capture.Identity(spec.RepositoryID, placement.ClusterUID), spec.Compression, capture.Connection, "18.6/full"})
	digest := sha256.Sum256(semantic)
	req := repository.Request{Schema: 1, RepositoryID: spec.RepositoryID, BackupUID: string(b.Metadata.UID), WriterClusterUID: placement.ClusterUID, RequestedKind: "full", ConfigSHA256: hex.EncodeToString(digest[:])}
	attempt, winner, e := hold.Begin(ctx, req)
	if e != nil {
		return nil, backupError(e)
	}
	if winner != nil {
		return backupResult(winner, repo.Identity().WALSegmentBytes), nil
	}
	captured, e := capture.Full(ctx, os.Getenv("POD_UID"))
	if e != nil {
		return nil, backupError(e)
	}
	defer captured.Close()
	commit := captured.Commit
	commit.RepositoryID = spec.RepositoryID
	commit.BackupUID = req.BackupUID
	commit.RootBackupUID = req.BackupUID
	commit.AttemptID = attempt.ID()
	commit.RequestSHA256 = attempt.RequestSHA256()
	winner, e = attempt.PublishChecked(ctx, commit, captured.Manifest, captured.Files, func(ctx context.Context) error { _, e := capture.Postflight(ctx); return e })
	if e != nil {
		return nil, backupError(e)
	}
	if e = ctx.Err(); e != nil {
		return nil, backupError(e)
	}
	return backupResult(winner, repo.Identity().WALSegmentBytes), nil
}
func backupError(e error) error {
	if e == nil {
		return nil
	}
	code := status.Code(walError(e))
	return status.Error(code, "full backup failed; no incomplete backup is published")
}
