package cnpgi

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	wire "github.com/cloudnative-pg/cnpg-i/pkg/wal"
	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/postgres"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
	files "github.com/djosh34/cnpg_backup/internal/wal"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const projectionPath = "/cnpg-backup/projection"
const workspacePath = "/cnpg-backup/work"
const pgdataPath = "/var/lib/postgresql/data/pgdata"

// WALPlacement is manager-owned immutable nonsecret authorization, not callback
// input. Ordinary mode never projects or opens the external source repository.
type WALPlacement struct {
	ClusterUID   string `json:"clusterUID"`
	Namespace    string `json:"namespace"`
	Cluster      string `json:"cluster"`
	Repository   string `json:"repository"`
	WALDirectory string `json:"walDirectory"`
}

// Two entire callbacks (including local spools/native checks) per process,
// independent of artifact transfer capacity. No pending durability queue.
var walCallbacks = make(chan struct{}, 2)
var singleWALData = make(chan struct{}, 1)

type WALService struct{ wire.UnimplementedWALServer }

func (*WALService) GetCapabilities(context.Context, *wire.WALCapabilitiesRequest) (*wire.WALCapabilitiesResult, error) {
	result := &wire.WALCapabilitiesResult{}
	for _, kind := range []wire.WALCapability_RPC_Type{wire.WALCapability_RPC_TYPE_ARCHIVE_WAL, wire.WALCapability_RPC_TYPE_RESTORE_WAL} {
		result.Capabilities = append(result.Capabilities, &wire.WALCapability{Type: &wire.WALCapability_Rpc{Rpc: &wire.WALCapability_RPC{Type: kind}}})
	}
	return result, nil
}

func walError(e error) error {
	if e == nil {
		return nil
	}
	code := codes.FailedPrecondition
	switch {
	case s3store.Is(e, s3store.NotFound):
		code = codes.NotFound
	case errors.Is(e, files.ErrInvalid):
		code = codes.InvalidArgument
	case errors.Is(e, files.ErrConflict):
		code = codes.AlreadyExists
	case errors.Is(e, files.ErrCorrupt), s3store.Is(e, s3store.Corrupt):
		code = codes.DataLoss
	case s3store.Is(e, s3store.Auth):
		code = codes.PermissionDenied
	case errors.Is(e, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
	case errors.Is(e, context.Canceled), s3store.Is(e, s3store.Canceled):
		code = codes.Canceled
	case s3store.Is(e, s3store.Transient), s3store.Is(e, s3store.TLS), s3store.Is(e, s3store.Unknown):
		code = codes.Unavailable
	case errors.Is(e, files.ErrLocal), s3store.Is(e, s3store.LocalIO):
		code = codes.ResourceExhausted
	}
	// Never reflect native/OS/SDK/request strings or credentials in RPC errors.
	return status.Error(code, "WAL operation failed")
}

func validateWALRequest(c Cluster, p WALPlacement) error {
	dst, _, e := c.Repositories()
	if e != nil || string(c.Metadata.UID) != p.ClusterUID || c.Metadata.Namespace != p.Namespace || c.Metadata.Name != p.Cluster || dst != p.Repository {
		return files.ErrInvalid
	}
	return nil
}
func walBasename(path string, p WALPlacement) (string, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", files.ErrInvalid
	}
	dir := filepath.Dir(path)
	if dir != pgdataPath+"/pg_wal" && dir != p.WALDirectory {
		return "", files.ErrInvalid
	}
	if p.WALDirectory != pgdataPath+"/pg_wal" && p.WALDirectory != "/var/lib/postgresql/wal/pg_wal" {
		return "", files.ErrInvalid
	}
	return filepath.Base(path), nil
}

// Reject filename/path syntax before any native command, disk or S3 access.
func validateWALPaths(definition []byte, path, source string, restore bool) error {
	c, e := ParseCluster(definition)
	if e != nil {
		return files.ErrInvalid
	}
	if _, _, e = c.Repositories(); e != nil {
		return files.ErrInvalid
	}
	directory := pgdataPath + "/pg_wal"
	if len(c.Spec.WALStorage) > 0 && string(c.Spec.WALStorage) != "null" {
		directory = "/var/lib/postgresql/wal/pg_wal"
	}
	name, e := walBasename(path, WALPlacement{WALDirectory: directory})
	if e != nil {
		return e
	}
	if !restore {
		source = name
	}
	if !repository.ValidWALFilename(source) {
		return files.ErrInvalid
	}
	if restore && name != source && name != "RECOVERYXLOG" && name != "RECOVERYHISTORY" {
		return files.ErrInvalid
	}
	return nil
}

type walOperation struct {
	files     files.Files
	root      *os.Root
	store     *s3store.Store
	placement WALPlacement
	ctx       context.Context
	cancel    context.CancelFunc
	single    bool
}

func (o *walOperation) close() { o.cancel(); o.root.Close(); o.store.Close() }

// The public per-Pod IO configuration is immutable; Secret/CA rotation does not
// change it. Honor a lower one-transfer setting across fresh client snapshots.
func (o *walOperation) dataSlot() (func(), error) {
	if !o.single {
		return func() {}, nil
	}
	select {
	case singleWALData <- struct{}{}:
		return func() { <-singleWALData }, nil
	case <-o.ctx.Done():
		return nil, o.ctx.Err()
	}
}
func openWAL(ctx context.Context, cluster []byte, archive bool) (result *walOperation, err error) {
	started := time.Now()
	root, e := configuration.Projection(projectionPath)
	if e != nil {
		return nil, e
	}
	defer root.Close()
	b, e := configuration.Read(root, "wal.json", 64<<10)
	if e != nil {
		return nil, e
	}
	var p WALPlacement
	if configuration.StrictJSON(b, &p) != nil {
		return nil, files.ErrInvalid
	}
	c, e := ParseCluster(cluster)
	if e != nil {
		return nil, e
	}
	if e = validateWALRequest(c, p); e != nil {
		return nil, e
	}
	// CaptureSnapshot validates current native trust and all destination members
	// from one generation. Restoring with stopped PG does not require live SQL.
	snap, e := configuration.LoadCaptureSnapshotFromRoot(root)
	if e != nil {
		return nil, e
	}
	budget := 60 * time.Second
	if archive {
		budget, _ = time.ParseDuration(snap.Repository.Spec.IO.WALUploadTimeout)
	}
	ctx, cancel := context.WithDeadline(ctx, started.Add(budget))
	defer func() {
		if result == nil {
			cancel()
		}
	}()
	var control postgres.Control
	if archive {
		control, e = postgres.CheckWAL(ctx, root)
	} else {
		control, e = postgres.ReadControl(ctx)
	}
	if e != nil {
		return nil, e
	}
	b, e = configuration.Read(root, "capacity.json", 64<<10)
	if e != nil {
		return nil, e
	}
	var budgets []configuration.FilesystemBudget
	if configuration.StrictJSON(b, &budgets) != nil {
		return nil, files.ErrInvalid
	}
	if e = walCapacity(budgets, p.WALDirectory, control.WALSegmentBytes, archive); e != nil {
		return nil, e
	}
	if e = configuration.CheckCapacity(budgets); e != nil {
		return nil, e
	}
	actual, e := filepath.EvalSymlinks(pgdataPath + "/pg_wal")
	if e != nil || actual != p.WALDirectory {
		return nil, files.ErrInvalid
	}
	physical, e := filepath.EvalSymlinks(p.WALDirectory)
	if e != nil || physical != p.WALDirectory {
		return nil, files.ErrInvalid
	}
	local, e := os.OpenRoot(p.WALDirectory)
	if e != nil {
		return nil, files.ErrLocal
	}
	store, e := snap.Repository.Store()
	if e != nil {
		local.Close()
		return nil, e
	}
	fail := func(e error) (*walOperation, error) { local.Close(); store.Close(); return nil, e }
	id := repository.Identity{Schema: 1, RepositoryID: snap.Repository.Spec.RepositoryID, PostgresMajor: 18, SystemIdentifier: control.SystemIdentifier, WALSegmentBytes: control.WALSegmentBytes, WriterClusterUID: p.ClusterUID}
	var repo *repository.Repository
	if archive {
		if e = store.CheckBucketSafety(ctx); e != nil {
			return fail(e)
		}
		repo, e = repository.OpenWriter(ctx, store, id, workspacePath)
	} else {
		repo, e = repository.OpenSource(ctx, store, id.RepositoryID, workspacePath)
		if e == nil {
			id.CreatedAt = repo.Identity().CreatedAt
			if id != repo.Identity() {
				e = repository.ErrIdentity
			}
		}
	}
	if e != nil { // A missing repository/gate is not an absent WAL file.
		if s3store.Is(e, s3store.NotFound) {
			e = repository.ErrIdentity
		}
		return fail(e)
	}
	return &walOperation{files: files.Files{Repository: repo, Store: store, Workspace: workspacePath, Compression: snap.Repository.Spec.Compression}, root: local, store: store, placement: p, ctx: ctx, cancel: cancel, single: snap.Repository.Spec.IO.WALUploads == 1}, nil
}

// walCapacity assigns only this operation's allocations; CheckCapacity still
// validates every declared physical filesystem, including read-only inputs.
func walCapacity(budgets []configuration.FilesystemBudget, directory string, segmentBytes int64, archive bool) error {
	workspace, wal := false, false
	for i := range budgets {
		budgets[i].RequiredBytes = 0 // source data/WAL/tablespaces are never staging
		if budgets[i].Mount == workspacePath {
			budgets[i].RequiredBytes = 6*(segmentBytes+(1<<20)) + (16 << 20)
			workspace = true
		}
		if budgets[i].Mount == filepath.Dir(directory) || directory == pgdataPath+"/pg_wal" && budgets[i].Mount == filepath.Dir(pgdataPath) {
			wal = true
			if !archive {
				budgets[i].RequiredBytes = 2*segmentBytes + (16 << 20)
			}
		}
	}
	if !workspace || !wal {
		return files.ErrInvalid
	}
	return nil
}

func acquireWAL(ctx context.Context) error {
	select {
	case walCallbacks <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (*WALService) Archive(ctx context.Context, r *wire.WALArchiveRequest) (*wire.WALArchiveResult, error) {
	if r == nil || len(r.Parameters) != 0 {
		return nil, walError(files.ErrInvalid)
	}
	if e := validateWALPaths(r.ClusterDefinition, r.SourceFileName, "", false); e != nil {
		return nil, walError(e)
	}
	// Even false/absent optional empty checks cannot bypass immutable ownership.
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	if e := acquireWAL(ctx); e != nil {
		return nil, walError(e)
	}
	defer func() { <-walCallbacks }()
	start := time.Now()
	o, e := openWAL(ctx, r.ClusterDefinition, true)
	if e == nil {
		defer o.close()
		release, se := o.dataSlot()
		if se != nil {
			return nil, walError(se)
		}
		defer release()
		var name string
		name, e = walBasename(r.SourceFileName, o.placement)
		if e == nil {
			_, _, e = o.files.Limits(name)
		}
		if e == nil {
			var source *os.File
			source, e = o.root.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
			if e == nil {
				defer source.Close()
				e = o.files.Archive(o.ctx, name, source)
			}
		}
	}
	result := walError(e)
	slog.Info("WAL archive callback", "success", e == nil, "duration_seconds", time.Since(start).Seconds(), "code", status.Code(result).String())
	if e != nil {
		return nil, result
	}
	return &wire.WALArchiveResult{}, nil
}
func (*WALService) Restore(ctx context.Context, r *wire.WALRestoreRequest) (*wire.WALRestoreResult, error) {
	if r == nil || len(r.Parameters) != 0 || r.Mode < wire.WALRestoreRequest_MODE_UNSPECIFIED || r.Mode > wire.WALRestoreRequest_MODE_REWIND {
		return nil, walError(files.ErrInvalid)
	}
	if e := validateWALPaths(r.ClusterDefinition, r.DestinationFileName, r.SourceWalName, true); e != nil {
		return nil, walError(e)
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	if e := acquireWAL(ctx); e != nil {
		return nil, walError(e)
	}
	defer func() { <-walCallbacks }()
	o, e := openWAL(ctx, r.ClusterDefinition, false)
	if e != nil {
		return nil, walError(e)
	}
	defer o.close()
	release, e := o.dataSlot()
	if e != nil {
		return nil, walError(e)
	}
	defer release()
	name, e := walBasename(r.DestinationFileName, o.placement)
	if e == nil {
		e = o.files.Restore(o.ctx, r.SourceWalName, o.root, name)
	}
	if e != nil {
		return nil, walError(e)
	}
	return &wire.WALRestoreResult{}, nil
}
