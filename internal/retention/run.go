package retention

import (
	"context"
	"errors"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/wal"
)

type Options struct {
	Enabled, DryRun bool
	Window          time.Duration
	MinimumFulls    int
	FirstRequired   *Hint
}
type Result struct {
	Decision    Decision
	Planned     int
	Executed    bool
	OperationID string
}

// Run performs ONE bounded batch. The manager is the sole local periodic
// caller. Every retry is a new admission/inventory; a crashed/uncertain owner
// cannot be adopted by this function. Diagnostic failures cannot veto cleanup.
func Run(ctx context.Context, r *repository.Repository, files wal.Files, now time.Time, o Options) (result Result, err error) {
	if !o.Enabled {
		return result, nil
	}
	if r == nil || o.Window < time.Hour || o.Window > 3650*24*time.Hour || o.MinimumFulls < 1 || o.MinimumFulls > 100 {
		return result, repository.ErrInvalid
	}
	g, e := r.AcquireGC(ctx)
	if e != nil {
		return result, e
	}
	result.OperationID = g.OperationID()
	defer func() {
		// A canceled work context is not a remote drain certificate. Close knows
		// the dispatched request outcomes and refuses any uncertain release.
		drain, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err = errors.Join(err, g.Close(drain))
	}()
	in := Inventory{SegmentBytes: uint64(r.Identity().WALSegmentBytes)}
	hashes := map[string]string{}
	timelines := map[uint32]bool{1: true}
	// Compact planning facts + bounded maps stay below a conservative 256MiB
	// budget; large native metadata remains in the repository's disk spool.
	budget := int64(0)
	reserve := func(n int64) error {
		budget += n
		if budget > 256<<20 {
			return repository.ErrCapacity
		}
		return nil
	}
	e = g.Inventory(ctx, repository.CatalogLimits{MaxRecords: repository.MaxCatalogRecords, MaxSpoolBytes: 256 << 20}, func(en repository.Entry) error {
		if en.Retired {
			return nil
		}
		if e := reserve(1024); e != nil {
			return e
		}
		c := en.Commit
		start, _ := repository.ParseLSN(c.StartLSN)
		redo, _ := repository.ParseLSN(c.RedoLSN)
		end, _ := repository.ParseLSN(c.BundledWALEndLSN)
		stop, _ := time.Parse(time.RFC3339Nano, c.StoppedAt)
		parent := ""
		if c.ParentBackupUID != nil {
			parent = *c.ParentBackupUID
		}
		in.Backups = append(in.Backups, Backup{c.BackupUID, parent, c.Timeline, stop, start, redo, end})
		hashes[c.BackupUID] = en.CommitSHA256
		timelines[c.Timeline] = true
		return nil
	})
	if e != nil {
		return result, e
	}
	objects := []repository.WALObject{}
	histories := map[uint32]string{}
	e = g.ArchiveInventory(ctx, func(ob repository.WALObject) error {
		if e := reserve(1024); e != nil {
			return e
		}
		observed, _, positionErr := repository.ArchivePosition(ob.Name[:8]+"0000000000000000", r.Identity().WALSegmentBytes)
		if positionErr != nil {
			return positionErr
		}
		timelines[observed] = true
		if len(ob.Name) == 24 {
			t, start, e := repository.ArchivePosition(ob.Name, r.Identity().WALSegmentBytes)
			if e != nil {
				return e
			}
			timelines[t] = true
			in.Segments = append(in.Segments, Segment{t, start, !ob.Retired})
			objects = append(objects, ob)
		} else if strings.HasSuffix(ob.Name, ".history") {
			// Derive the numeric ID using the already validated original name.
			t, _, e := repository.ArchivePosition(ob.Name[:8]+"0000000000000000", r.Identity().WALSegmentBytes)
			if e != nil {
				return e
			}
			timelines[t] = true
			histories[t] = ob.Name
		}
		return nil
	})
	if e != nil {
		return result, e
	}
	ids := []uint32{}
	for id := range timelines {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if id == 1 {
			in.Paths = append(in.Paths, []Timeline{{ID: 1}})
			continue
		}
		name, ok := histories[id]
		if !ok {
			return result, ErrCoverage
		}
		f, e := os.CreateTemp(files.Workspace, "retention-history-*")
		if e != nil {
			return result, e
		}
		path, e := func() ([]repository.Timeline, error) {
			defer os.Remove(f.Name())
			defer f.Close()
			if _, e := files.Retrieve(ctx, name, f); e != nil {
				return nil, e
			}
			b, e := os.ReadFile(f.Name())
			if e != nil {
				return nil, e
			}
			return repository.ParseHistory(id, b)
		}()
		if e != nil {
			return result, e
		}
		converted := []Timeline{}
		for _, t := range path {
			fork := uint64(0)
			if t.ForkLSN != nil {
				fork, _ = repository.ParseLSN(*t.ForkLSN)
			}
			converted = append(converted, Timeline{t.ID, fork})
		}
		in.Paths = append(in.Paths, converted)
	}
	result.Decision, e = Plan(in, now.Add(-o.Window), o.MinimumFulls, o.FirstRequired)
	if e != nil {
		return result, e
	}
	// Complete cleanup discovery is a prerequisite even when retirements fill
	// this batch. Partial MPU lists never authorize unrelated destruction.
	cleanup, e := g.Cleanup(ctx, 128)
	if e != nil {
		return result, e
	}
	victims := []repository.Victim{}
	for _, id := range result.Decision.Expire {
		if len(victims) == 128 {
			break
		}
		victims = append(victims, repository.Victim{Kind: "retire-backup", BackupUID: &id, SHA256: hashes[id]})
	}
	for _, v := range cleanup {
		if len(victims) == 128 {
			break
		}
		victims = append(victims, v)
	}
	// WAL follows ALL needed backup retirements, not just the bounded prefix.
	if len(result.Decision.Expire) <= 128 && len(victims) < 128 {
		for i, seg := range in.Segments {
			if len(victims) == 128 {
				break
			}
			if seg.Live && seg.Timeline == result.Decision.Current && seg.Start < result.Decision.WALFloor {
				ob := objects[i]
				victims = append(victims, repository.Victim{Kind: "retire-wal", WALName: &ob.Name, ExpectedETag: &ob.ETag, SHA256: ob.SHA256, RawBytes: ob.RawBytes})
			}
		}
	}
	result.Planned = len(victims)
	if o.DryRun || len(victims) == 0 {
		return result, nil
	}
	e = g.Execute(ctx, g.Plan(now.Add(-o.Window).UTC().Format(time.RFC3339Nano), repository.Policy{WindowSeconds: int64(o.Window / time.Second), MinimumFulls: o.MinimumFulls}, victims))
	result.Executed = e == nil
	return result, e
}
