package cnpgi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	job "github.com/cloudnative-pg/cnpg-i/pkg/restore/job"
	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/postgres"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
	files "github.com/djosh34/cnpg_backup/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecoveryService owns one original guard session and source snapshot. Nothing
// is opened on a target at construction/startup. The mutex serializes bootstrap
// callbacks, never creates a new/adopted session after failure or clean drain.
type RecoveryService struct {
	job.UnimplementedRestoreJobHooksServer
	admission                *recoveryguard.Admission
	mu                       sync.Mutex
	attempted, ready, closed bool
	plan                     RecoveryPlan
	hold                     *repository.Hold
	store                    *s3store.Store
	files                    files.Files
	budgets                  []configuration.FilesystemBudget
}

func NewRecoveryService(a *recoveryguard.Admission) (*RecoveryService, error) {
	s := &RecoveryService{admission: a}
	if e := a.AfterDrain(s.drain); e != nil {
		return nil, e
	}
	return s, nil
}
func (s *RecoveryService) drain(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	// Admission has irrevocably closed and every Task finished, including native
	// children and WAL publications. This is not called on session/server loss.
	if s.hold != nil {
		if e := s.hold.Close(ctx); e != nil {
			return e
		}
	}
	if s.store != nil {
		s.store.Close()
	}
	return nil
}
func (*RecoveryService) GetCapabilities(context.Context, *job.RestoreJobHooksCapabilitiesRequest) (*job.RestoreJobHooksCapabilitiesResult, error) {
	return &job.RestoreJobHooksCapabilitiesResult{Capabilities: []*job.RestoreJobHooksCapability{{Kind: job.RestoreJobHooksCapability_KIND_RESTORE}}}, nil
}
func (s *RecoveryService) Restore(ctx context.Context, r *job.RestoreRequest) (*job.RestoreResponse, error) {
	if r == nil {
		return nil, walError(repository.ErrInvalid)
	}
	c, e := ParseCluster(r.ClusterDefinition)
	if e != nil {
		return nil, walError(repository.ErrInvalid)
	}
	if _, _, e = c.Repositories(); e != nil || c.Spec.Bootstrap.Recovery == nil {
		return nil, walError(repository.ErrInvalid)
	}
	tuple, e := s.admission.ActiveTuple()
	if e != nil {
		return nil, e
	}
	task, e := s.admission.Admit(tuple)
	if e != nil {
		return nil, e
	}
	defer task.Done()
	ctx, cancel := taskContext(ctx, task)
	defer cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || tuple.Owner.ClusterUID != string(c.Metadata.UID) || tuple.Owner.OperationUID != c.OperationUID() {
		return nil, walError(errRecoveryClosed)
	}
	if s.ready {
		old, _ := ParseCluster(s.plan.ClusterDefinition)
		if !reflect.DeepEqual(recoveryCluster(c).Spec, old.Spec) {
			return nil, walError(repository.ErrIdentity)
		}
		if e = s.hold.ReadSource(ctx, func(context.Context) error { return nil }); e != nil {
			return nil, walError(e)
		}
		return &job.RestoreResponse{RestoreConfig: restoreConfig(s.plan.Plan.Target.Timeline)}, nil
	}
	if s.attempted {
		return nil, walError(errRecoveryClosed)
	}
	s.attempted = true
	if e = s.bootstrap(ctx, c, tuple); e != nil {
		// No subsequent RPC rewrites partial targets or selects a newer base.
		// Original guard may cleanly drain; uncertainty still poisons its markers.
		if errors.Is(e, errDifferentialRestore) {
			return nil, status.Error(codes.Unimplemented, "differential restore is not implemented")
		}
		return nil, status.Error(codes.FailedPrecondition, "protected full restore failed")
	}
	s.ready = true
	return &job.RestoreResponse{RestoreConfig: restoreConfig(s.plan.Plan.Target.Timeline)}, nil
}

var errDifferentialRestore = errors.New("differential restore is not implemented")

func (s *RecoveryService) bootstrap(ctx context.Context, c Cluster, tuple recoveryguard.Tuple) (err error) {
	phase := "configuration"
	defer func() {
		if err != nil {
			slog.Warn("protected full restore failed", "phase", phase)
		}
	}()
	root, e := configuration.Projection(projectionPath)
	if e != nil {
		return e
	}
	defer root.Close()
	b, e := configuration.Read(root, "recovery.json", 64<<10)
	if e != nil {
		return e
	}
	var placement RecoveryPlacement
	if configuration.StrictJSON(b, &placement) != nil {
		return repository.ErrInvalid
	}
	dst, src, e := c.Repositories()
	if e != nil || placement.ClusterUID != string(c.Metadata.UID) || placement.Namespace != c.Metadata.Namespace || placement.Cluster != c.Metadata.Name || placement.OperationID != c.OperationUID() || placement.BootstrapSHA256 != c.BootstrapFingerprint() || placement.Source != src || placement.Destination != dst {
		return repository.ErrIdentity
	}
	if e = activeOperation(root, placement); e != nil {
		return e
	}
	source, e := configuration.LoadSnapshotFromRoot(root, "source")
	if e != nil {
		return e
	}
	destination, e := configuration.LoadSnapshotFromRoot(root, "destination")
	if e != nil {
		return e
	}
	if source.Spec.Hash() != placement.SourceConfigSHA256 || destination.Spec.Hash() != placement.DestinationConfigSHA256 || source.Spec.RepositoryID == destination.Spec.RepositoryID {
		return repository.ErrIdentity
	}
	duration, _ := time.ParseDuration(destination.Spec.IO.OperationTimeout)
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	b, e = configuration.Read(root, "capacity.json", 64<<10)
	if e != nil {
		return e
	}
	if configuration.StrictJSON(b, &s.budgets) != nil {
		return repository.ErrInvalid
	}
	phase = "selection-capacity"
	// Catalog/winner verification precedes extraction, but itself uses a bounded
	// artifact spool. Reserve a complete supported input before any payload read.
	found := false
	for i := range s.budgets {
		if s.budgets[i].Mount == workspacePath {
			n := destination.Spec.Native.MaxBackupBytes
			peak := n + max(configuration.GiB, (n+99)/100) + (256 << 20) + (64 << 20) + (16 << 20)
			s.budgets[i].RequiredBytes = peak + configuration.CapacityMargin(peak)
			found = true
		}
	}
	if !found {
		return repository.ErrInvalid
	}
	if e = configuration.CheckCapacity(s.budgets); e != nil {
		return e
	}
	phase = "source-admission"
	store, e := source.Store()
	if e != nil {
		return e
	}
	s.store = store
	repo, e := repository.OpenSource(ctx, store, source.Spec.RepositoryID, postgres.NativeWorkspace)
	if e != nil {
		return e
	}
	if repo.Identity().WriterClusterUID == string(c.Metadata.UID) {
		return repository.ErrIdentity
	}
	s.hold, e = repo.AdmitRestore(ctx, string(c.Metadata.UID), c.OperationUID())
	if e != nil {
		return e
	}
	s.files = files.Files{Repository: repo, Store: store, Workspace: postgres.NativeWorkspace, Compression: source.Spec.Compression}
	phase = "protected-selection"
	directory := recoveryDirectory(c.OperationUID())
	var plan repository.Plan
	checkInput := func(c repository.Commit) error { return checkRestoreInput(c, destination.Spec.Native) }
	previous, e := readRecovery(directory)
	if e == nil {
		if previous.Plan.BootstrapSHA256 != c.BootstrapFingerprint() || previous.Plan.DestinationRepositoryID != destination.Spec.RepositoryID {
			return repository.ErrIdentity
		}
		for _, input := range previous.Plan.Chain {
			if e = checkInput(input); e != nil {
				return e
			}
		}
		plan, e = s.hold.ValidatePlan(ctx, previous.Plan)
		if e != nil {
			return e
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	} else {
		target, e := recoveryTarget(c.Spec.Bootstrap.Recovery.RecoveryTarget)
		if e != nil {
			return e
		}
		catalog, e := s.hold.Catalog(ctx, repository.CatalogLimits{MaxRecords: repository.MaxCatalogRecords, MaxSpoolBytes: min(destination.Spec.Native.MaxBackupBytes, 256<<20)})
		if e != nil {
			return e
		}
		defer catalog.Close()
		names, e := s.hold.ArchiveNames(ctx)
		if e != nil {
			return e
		}
		plan, e = s.hold.Resolve(ctx, catalog, repository.Plan{DestinationRepositoryID: destination.Spec.RepositoryID, BootstrapSHA256: c.BootstrapFingerprint(), Target: target}, names, checkInput, s.history)
		closeErr := catalog.Close()
		if e != nil {
			return e
		}
		if closeErr != nil {
			return closeErr
		}
	}
	if len(plan.Chain) != 1 || plan.Chain[0].Kind != "full" {
		return errDifferentialRestore
	}
	phase = "destination-identity"
	// Reject source/destination ownership overlap before publishing a usable
	// restore response; initialize destination with source physical identity.
	id := repo.Identity()
	id.RepositoryID = destination.Spec.RepositoryID
	id.WriterClusterUID = string(c.Metadata.UID)
	id.CreatedAt = ""
	targetStore, e := destination.Store()
	if e != nil {
		return e
	}
	defer targetStore.Close()
	if _, e = repository.OpenWriter(ctx, targetStore, id, postgres.NativeWorkspace); e != nil {
		return e
	}
	phase = "durable-plan"
	s.plan = RecoveryPlan{Plan: plan, Tuple: tuple, ClusterDefinition: json.RawMessage(jsonText(recoveryCluster(c))), Bundled: map[string]s3store.Integrity{}}
	// Only guard-owned state outside PGDATA is touched. No startup target sweep.
	parent, e := os.OpenRoot("/var/lib/postgresql/data")
	if e != nil {
		return e
	}
	statePath := ".cnpg-backup/" + c.OperationUID()
	e = parent.Mkdir(statePath, 0700)
	if errors.Is(e, os.ErrExist) {
		var st os.FileInfo
		st, e = parent.Lstat(statePath)
		if e == nil && !st.IsDir() {
			e = repository.ErrInvalid
		}
	}
	if e == nil {
		for _, path := range []string{".cnpg-backup", "."} {
			directory, se := parent.Open(path)
			if se != nil {
				e = se
				break
			}
			e = directory.Sync()
			ce := directory.Close()
			if e == nil {
				e = ce
			}
			if e != nil {
				break
			}
		}
	}
	parent.Close()
	if e != nil {
		return e
	}
	if e = saveRecovery(directory, s.plan); e != nil {
		return e
	}
	if e = saveRecovery(filepath.Dir(helperPlanPath), s.plan); e != nil {
		return e
	}
	layout := postgres.RestoreLayout{PGDATA: pgdataPath, WALDirectory: physicalWAL(c), Tablespaces: map[string]string{}}
	for _, t := range c.Spec.Tablespaces {
		layout.Tablespaces[t.Name] = "/var/lib/postgresql/tablespaces/" + t.Name + "/data"
	}
	phase = "native-materialization"
	// A generic native/final-fsync failure (or panic) is not a certificate of
	// clean descendant drainage. Refuse Drain and retain this process-reader;
	// retry after this boundary requires a fresh Cluster and all fresh PVCs.
	complete := false
	defer func() {
		if !complete {
			s.admission.Close()
		}
	}()
	s.plan.Bundled, e = postgres.RestoreFull(ctx, s.hold, plan, destination.Spec.Native, s.budgets, layout)
	if e != nil {
		return e
	}
	phase = "materialized-plan"
	s.plan.Materialized = true
	if e = saveRecovery(directory, s.plan); e != nil {
		return e
	}
	if e = saveRecovery(filepath.Dir(helperPlanPath), s.plan); e != nil {
		return e
	}
	complete = true
	return nil
}
func checkRestoreInput(c repository.Commit, n configuration.Native) error {
	if c.Kind != "full" {
		return errDifferentialRestore
	}
	raw, stored := c.ManifestBytes, c.ManifestBytes
	for _, a := range c.Artifacts {
		raw += a.RawBytes
		stored += a.StoredBytes
		if a.Role == "wal" && a.RawBytes > n.MaxBootstrapWALBytes {
			return repository.ErrCapacity
		}
	}
	if raw > n.MaxBackupBytes || stored > n.MaxBackupBytes+max(configuration.GiB, (n.MaxBackupBytes+99)/100) {
		return repository.ErrCapacity
	}
	return nil
}
func (s *RecoveryService) history(ctx context.Context, name string) ([]byte, error) {
	f, e := os.CreateTemp(postgres.NativeWorkspace, "history-")
	if e != nil {
		return nil, e
	}
	defer f.Close()
	if e = os.Remove(f.Name()); e != nil {
		return nil, e
	}
	e = s.hold.ReadSource(ctx, func(ctx context.Context) error { _, e := s.files.Retrieve(ctx, name, f); return e })
	if e != nil {
		return nil, e
	}
	if _, e = f.Seek(0, 0); e != nil {
		return nil, e
	}
	return io.ReadAll(io.LimitReader(f, (1<<20)+1))
}
