package cnpgi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	wire "github.com/cloudnative-pg/cnpg-i/pkg/wal"
	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
	files "github.com/djosh34/cnpg_backup/internal/wal"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type recoveryWAL struct {
	wire.UnimplementedWALServer
	recovery *RecoveryService
}

func (*recoveryWAL) GetCapabilities(context.Context, *wire.WALCapabilitiesRequest) (*wire.WALCapabilitiesResult, error) {
	return &wire.WALCapabilitiesResult{Capabilities: []*wire.WALCapability{{Type: &wire.WALCapability_Rpc{Rpc: &wire.WALCapability_RPC{Type: wire.WALCapability_RPC_TYPE_RESTORE_WAL}}}}}, nil
}
func (w *recoveryWAL) Restore(ctx context.Context, r *wire.WALRestoreRequest) (*wire.WALRestoreResult, error) {
	if r == nil || r.Mode != wire.WALRestoreRequest_MODE_RECOVERY || len(r.Parameters) != 1 {
		return nil, walError(files.ErrInvalid)
	}
	var tuple recoveryguard.Tuple
	if configuration.StrictJSON([]byte(r.Parameters["recoveryID"]), &tuple) != nil {
		return nil, walError(files.ErrInvalid)
	}
	c, e := ParseCluster(r.ClusterDefinition)
	if e != nil {
		return nil, walError(files.ErrInvalid)
	}
	destination, e := recoveryDestination(c, r.SourceWalName, r.DestinationFileName)
	if e != nil {
		return nil, walError(files.ErrInvalid)
	}
	s := w.recovery
	task, e := s.admission.Admit(tuple)
	if e != nil {
		return nil, e
	}
	defer task.Done()
	ctx, cancel := taskContext(ctx, task)
	defer cancel()
	ctx, timeout := context.WithTimeout(ctx, 60*time.Second)
	defer timeout()
	if e = acquireWAL(ctx); e != nil {
		return nil, walError(e)
	}
	defer func() { <-walCallbacks }()
	s.mu.Lock()
	if !s.ready || s.closed || tuple != s.plan.Tuple || string(r.ClusterDefinition) != string(s.plan.ClusterDefinition) {
		s.mu.Unlock()
		return nil, walError(errRecoveryClosed)
	}
	p, hold, source := s.plan, s.hold, s.files
	budgets := append([]configuration.FilesystemBudget(nil), s.budgets...)
	s.mu.Unlock()
	if e = walCapacity(budgets, physicalWAL(c), p.Plan.Source.WALSegmentBytes, false); e != nil {
		return nil, walError(e)
	}
	if e = configuration.CheckCapacity(budgets); e != nil {
		return nil, walError(e)
	}
	for _, path := range []string{physicalWAL(c), pgdataPath + "/pg_wal"} {
		actual, e := filepath.EvalSymlinks(path)
		if e != nil || actual != physicalWAL(c) {
			return nil, walError(files.ErrInvalid)
		}
	}
	root, e := os.OpenRoot(physicalWAL(c))
	if e != nil {
		return nil, walError(files.ErrLocal)
	}
	defer root.Close()
	called := false
	e = hold.ReadSource(ctx, func(ctx context.Context) error {
		called = true
		return restoreSourceWAL(ctx, source, p, r.SourceWalName, root, destination)
	})
	// Missing identity/gate/lifetime is NEVER archive exhaustion.
	if !called && e != nil {
		return nil, status.Error(codes.FailedPrecondition, "source reader admission failed")
	}
	if e != nil {
		return nil, walError(e)
	}
	return &wire.WALRestoreResult{}, nil
}

// This actual I/O path always tries the archive first. Only authenticated
// NoSuchKey is classifiable as an ordinary miss. A local bundle is never sent
// as archive success and never masks required post-EndLSN bytes in its filename.
func restoreSourceWAL(ctx context.Context, source files.Files, p RecoveryPlan, name string, root *os.Root, destination string) error {
	bundle, remote, e := p.Plan.Coverage(name)
	if e != nil {
		return files.ErrInvalid
	}
	e = source.Restore(ctx, name, root, destination)
	if !s3store.Is(e, s3store.NotFound) {
		return e
	}
	if remote {
		return repository.ErrBlocked
	}
	if bundle {
		expected, ok := p.Bundled[name]
		if !ok {
			return files.ErrCorrupt
		}
		if e = verifyLocalBundle(ctx, root, name, expected); e != nil {
			return e
		}
		return &s3store.Error{Kind: s3store.NotFound}
	}
	if !optionalFuture(p.Plan, name) {
		return repository.ErrBlocked
	}
	return &s3store.Error{Kind: s3store.NotFound}
}
func verifyLocalBundle(ctx context.Context, root *os.Root, name string, expected s3store.Integrity) error {
	f, e := root.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if e != nil {
		return files.ErrCorrupt
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil || !st.Mode().IsRegular() || st.Size() != expected.Size {
		return files.ErrCorrupt
	}
	hash := sha256.New()
	buf := make([]byte, 128<<10)
	var size int64
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n, e := f.Read(buf)
		size += int64(n)
		if size > expected.Size {
			return files.ErrCorrupt
		}
		_, _ = hash.Write(buf[:n])
		if e == io.EOF {
			break
		}
		if e != nil {
			return files.ErrLocal
		}
	}
	after, e := f.Stat()
	if e != nil || size != expected.Size || after.Size() != st.Size() || after.ModTime() != st.ModTime() || hex.EncodeToString(hash.Sum(nil)) != expected.SHA256 {
		return files.ErrCorrupt
	}
	return nil
}
func optionalFuture(p repository.Plan, name string) bool {
	if strings.HasSuffix(name, ".history") {
		return true
	}
	if len(name) != 24 {
		return false
	}
	timeline, _ := strconv.ParseUint(name[:8], 16, 32)
	if uint32(timeline) != p.Target.Timeline {
		return false
	}
	log, _ := strconv.ParseUint(name[8:16], 16, 32)
	seg, _ := strconv.ParseUint(name[16:], 16, 32)
	start := log<<32 | seg*uint64(p.Source.WALSegmentBytes)
	frontier, _ := repository.ParseLSN(p.Chain[len(p.Chain)-1].BundledWALEndLSN)
	if n := len(p.RequiredArchive); n > 0 {
		frontier, _ = repository.ParseLSN(p.RequiredArchive[n-1].EndLSN)
	}
	return start >= frontier
}
