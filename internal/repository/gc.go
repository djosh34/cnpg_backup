package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

type Policy struct {
	WindowSeconds int64 `json:"window_seconds"`
	MinimumFulls  int   `json:"minimum_fulls"`
}

// Victim identifies only derived repository slots, never an arbitrary key.
// Retention decides eligibility; this module enforces ownership, permanent
// exclusion, dependency ordering and the exact destructive request boundary.
type Victim struct {
	Kind         string  `json:"kind"`
	BackupUID    *string `json:"backup_uid"`
	AttemptID    *string `json:"attempt_id"`
	Index        *int    `json:"index"`
	Compression  *string `json:"compression"`
	UploadID     *string `json:"upload_id"`
	WALName      *string `json:"wal_name"`
	ExpectedETag *string `json:"expected_etag"`
	SHA256       string  `json:"sha256"`
	RawBytes     int64   `json:"raw_bytes"`
}
type GCPlan struct {
	Schema       int      `json:"schema"`
	RepositoryID string   `json:"repository_id"`
	OperationID  string   `json:"operation_id"`
	ProcessID    string   `json:"process_id"`
	Cutoff       string   `json:"cutoff"`
	Policy       Policy   `json:"policy"`
	Victims      []Victim `json:"victims"`
}
type GCOwner struct {
	mu                          sync.Mutex
	r                           *Repository
	owner                       Owner
	started                     time.Time
	catalog                     *os.File
	requests                    int
	closed, uncertain, executed bool
}

func (r *Repository) AcquireGC(ctx context.Context) (*GCOwner, error) {
	if e := r.store.CheckBucketSafety(ctx); e != nil {
		return nil, e
	}
	r.gcMu.Lock()
	wait := time.Until(r.gcNotBefore)
	r.gcMu.Unlock()
	if wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	o := Owner{OperationID: UUID(), ProcessID: r.process, Kind: "gc"}
	e := r.changeGate(ctx, func(g *Gate) (bool, error) {
		if g.Owner != nil {
			if *g.Owner == o {
				return true, nil
			}
			return false, ErrBlocked
		}
		if len(g.Holders) != 0 {
			return false, ErrBlocked
		}
		g.Owner = &o
		return false, nil
	})
	if e != nil {
		return nil, e
	}
	return &GCOwner{r: r, owner: o}, nil
}
func (g *GCOwner) OperationID() string { return g.owner.OperationID }
func (g *GCOwner) check(ctx context.Context) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if g.closed {
		return ErrClosed
	}
	if g.uncertain {
		return ErrUncertain
	}
	gate, _, e := g.r.readGate(ctx)
	if e != nil {
		return e
	}
	if gate.Owner == nil || *gate.Owner != g.owner {
		return ErrBlocked
	}
	return nil
}

// Inventory is authoritative only under this exact live owner. It includes
// historical retired metadata, but never treats it as a usable backup.
func (g *GCOwner) Inventory(ctx context.Context, l CatalogLimits, visit func(Entry) error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if e := g.check(ctx); e != nil {
		return e
	}
	if e := l.validate(); e != nil {
		return e
	}
	if visit == nil || g.executed || g.catalog != nil {
		return ErrInvalid
	}
	if l.MaxSpoolBytes > GCCatalogBytes {
		return ErrCapacity
	}
	f, e := g.r.spoolCatalog(ctx, l)
	if e != nil {
		return e
	}
	if e = scanEntries(ctx, f, visit); e != nil {
		removeFile(f)
		return e
	}
	// This owner excludes backup writers and all other GC. Keep its validated
	// snapshot for dependency checks, rather than downloading every input twice.
	g.catalog = f
	return nil
}
func (g *GCOwner) Execute(ctx context.Context, p GCPlan) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if e := g.check(ctx); e != nil {
		return e
	}
	if g.executed {
		return ErrClosed
	}
	g.executed = true
	if p.Schema != 1 || p.RepositoryID != g.r.id.RepositoryID || p.OperationID != g.owner.OperationID || p.ProcessID != g.owner.ProcessID || len(p.Victims) < 1 || len(p.Victims) > 128 {
		return ErrInvalid
	}
	if _, ok := stamp(p.Cutoff); !ok {
		return ErrInvalid
	}
	if p.Policy.WindowSeconds < 3600 || p.Policy.WindowSeconds > 3650*86400 || p.Policy.MinimumFulls < 1 || p.Policy.MinimumFulls > 100 {
		return ErrInvalid
	}
	// No effect until the entire list/graph and all victim shapes are valid.
	var e error
	if g.catalog == nil {
		g.catalog, e = g.r.spoolCatalog(ctx, CatalogLimits{MaxCatalogRecords, GCCatalogBytes})
		if e != nil {
			return e
		}
	}
	retired := map[string]bool{}
	walRetirement := false
	for _, v := range p.Victims {
		// Validate the COMPLETE ordering before persisting or executing the
		// plan. An interrupted batch must never leave a still-live backup
		// after its planned remote WAL has already been retired.
		if v.Kind == "retire-wal" {
			walRetirement = true
		}
		if v.Kind == "retire-backup" && walRetirement {
			return ErrInvalid
		}
		if e = g.validateVictim(ctx, v, retired); e != nil {
			return e
		}
		if v.Kind == "retire-backup" {
			uid := *v.BackupUID
			e = scanEntries(ctx, g.catalog, func(en Entry) error {
				if !en.Retired && en.Commit.ParentBackupUID != nil && *en.Commit.ParentBackupUID == uid && !retired[en.Commit.BackupUID] {
					return ErrBlocked
				}
				return nil
			})
			if e != nil {
				return e
			}
			retired[uid] = true
		}
	}
	b, _ := json.Marshal(p)
	if int64(len(b)) > commitLimit {
		return ErrCapacity
	}
	_, e = g.r.put(ctx, g.r.root+"gc/"+p.OperationID+".json", b, s3store.Condition{Create: true})
	if e != nil {
		if ambiguous(e) {
			g.uncertain = true
		}
		return e
	}
	// One destructive-phase budget, not time spent proving inventory and not
	// a fresh allowance per request. The caller's deadline still bounds reads.
	for _, v := range p.Victims {
		if g.requests >= 128 {
			return ErrCapacity
		}
		if e = g.check(ctx); e != nil {
			return e
		}
		if g.started.IsZero() {
			g.started = time.Now()
		} else if time.Since(g.started) >= 30*time.Second {
			return ErrCapacity
		}
		g.requests++
		e = g.destroy(ctx, v)
		if e != nil {
			if ambiguous(e) {
				g.uncertain = true
			}
			return e
		}
	}
	return nil
}
func (g *GCOwner) validateVictim(ctx context.Context, v Victim, retired map[string]bool) error {
	if !hashRE.MatchString(v.SHA256) || v.RawBytes < 0 {
		return ErrInvalid
	}
	if v.Kind == "retire-wal" {
		if v.WALName == nil || v.ExpectedETag == nil || *v.ExpectedETag == "" || len(*v.ExpectedETag) > 256 || v.BackupUID != nil || v.AttemptID != nil || v.Index != nil || v.Compression != nil || v.UploadID != nil {
			return ErrInvalid
		}
		name := *v.WALName
		if len(name) != 24 || v.RawBytes != g.r.id.WALSegmentBytes {
			return ErrInvalid
		}
		key, e := g.r.WALKey(name)
		if e != nil {
			return e
		}
		info, e := g.r.store.Head(ctx, key)
		if e != nil {
			return e
		}
		if info.ETag != *v.ExpectedETag || info.Metadata["cnpg-format"] != "wal-v1" || info.Metadata["cnpg-system-id"] != g.r.id.SystemIdentifier || info.Metadata["cnpg-raw-sha256"] != v.SHA256 || info.Metadata["cnpg-raw-bytes"] != strconv.FormatInt(v.RawBytes, 10) {
			return ErrIdentity
		}
		return nil
	}
	if v.BackupUID == nil || !validID(*v.BackupUID) || v.WALName != nil || v.ExpectedETag != nil {
		return ErrInvalid
	}
	uid := *v.BackupUID
	c, b, _, e := g.r.readCommit(ctx, uid)
	if v.Kind == "retire-backup" {
		if v.AttemptID != nil || v.Index != nil || v.Compression != nil || v.UploadID != nil {
			return ErrInvalid
		}
		if e != nil {
			return e
		}
		if digest(b) != v.SHA256 {
			return ErrIdentity
		}
		return nil
	}
	if v.AttemptID == nil || !validID(*v.AttemptID) {
		return ErrInvalid
	}
	attempt := *v.AttemptID
	if e == nil {
		if c.AttemptID == attempt {
			le := g.r.live(ctx, c, b)
			if le != nil && le != ErrRetired {
				return le
			}
			if le != ErrRetired && !retired[uid] {
				return ErrBlocked
			}
		}
	} else if !s3store.Is(e, s3store.NotFound) {
		return e
	}
	// Every orphan must have a valid claimed namespace, not an age heuristic.
	cb, _, e := g.r.store.Read(ctx, g.r.attempt(uid, attempt)+"claim.json", smallLimit)
	if e != nil {
		return e
	}
	var cl Claim
	if strict(cb, smallLimit, &cl) != nil || cl.Schema != 1 || cl.RepositoryID != g.r.id.RepositoryID || cl.BackupUID != uid || cl.AttemptID != attempt || !validID(cl.ProcessID) || !hashRE.MatchString(cl.RequestSHA256) {
		return ErrCorrupt
	}
	switch v.Kind {
	case "delete-manifest":
		if v.Index != nil || v.Compression != nil || v.UploadID != nil {
			return ErrInvalid
		}
	case "delete-artifact", "abort-artifact":
		if v.Index == nil || *v.Index < 0 || *v.Index >= 66 || v.Compression == nil || (*v.Compression != "none" && *v.Compression != "gzip") {
			return ErrInvalid
		}
		if v.Kind == "abort-artifact" {
			if v.UploadID == nil || !validText(*v.UploadID, 2048) {
				return ErrInvalid
			}
		} else if v.UploadID != nil {
			return ErrInvalid
		}
	case "abort-manifest":
		if v.Index != nil || v.Compression != nil || v.UploadID == nil || !validText(*v.UploadID, 2048) {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
func (g *GCOwner) destroy(ctx context.Context, v Victim) error {
	r := g.r
	if v.Kind == "retire-backup" {
		ret := Retirement{1, r.id.RepositoryID, *v.BackupUID, v.SHA256, g.owner.OperationID}
		b, _ := json.Marshal(ret)
		_, e := r.put(ctx, r.backup(*v.BackupUID)+"retired.json", b, s3store.Condition{Create: true})
		if s3store.Is(e, s3store.Precondition) {
			old, _, re := r.store.Read(ctx, r.backup(*v.BackupUID)+"retired.json", smallLimit)
			if re != nil {
				return re
			}
			var got Retirement
			if strict(old, smallLimit, &got) != nil {
				return ErrCorrupt
			}
			return got.validate(r.id, *v.BackupUID, v.SHA256)
		}
		return e
	}
	if v.Kind == "retire-wal" {
		key, e := r.WALKey(*v.WALName)
		if e != nil {
			return e
		}
		ret := WALRetirement{1, "retired", r.id.RepositoryID, *v.WALName, v.RawBytes, v.SHA256, g.owner.OperationID}
		b, _ := json.Marshal(ret)
		_, e = r.putWALTombstone(ctx, key, b, *v.ExpectedETag)
		return e
	}
	key := r.attempt(*v.BackupUID, *v.AttemptID) + "manifest.pg.json"
	if v.Index != nil {
		key = r.artifact(Commit{BackupUID: *v.BackupUID, AttemptID: *v.AttemptID}, Artifact{Index: *v.Index, Compression: *v.Compression})
	}
	if strings.HasPrefix(v.Kind, "abort-") {
		return r.store.Abort(ctx, s3store.Upload{Key: key, ID: *v.UploadID})
	}
	return r.store.Delete(ctx, key)
}

// Close irrevocably stops dispatch first. No HEAD, retry result, elapsed time or
// subsequent process can clear an uncertain owner. Successful batches yield at
// least one second before the caller can finish and start another batch.
func (g *GCOwner) Close(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	if g.catalog != nil {
		removeFile(g.catalog)
		g.catalog = nil
	}
	g.r.gcMu.Lock()
	g.r.gcNotBefore = time.Now().Add(time.Second)
	g.r.gcMu.Unlock()
	if g.uncertain {
		return ErrUncertain
	}
	e := g.r.changeGate(ctx, func(gate *Gate) (bool, error) {
		if gate.Owner == nil {
			return true, nil
		}
		if *gate.Owner != g.owner {
			return false, ErrIdentity
		}
		gate.Owner = nil
		return false, nil
	})
	if e != nil {
		return e
	}
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type WALRetirement struct {
	Schema        int    `json:"schema"`
	State         string `json:"state"`
	RepositoryID  string `json:"repository_id"`
	Name          string `json:"name"`
	RawBytes      int64  `json:"raw_bytes"`
	RawSHA256     string `json:"raw_sha256"`
	GCOperationID string `json:"gc_operation_id"`
}

// WALKey validates PostgreSQL's original name, including segment arithmetic.
// Public for the WAL slice, whose no-clobber single PUT requires no gate hold.
func (r *Repository) WALKey(name string) (string, error) {
	timeline, e := walTimeline(name, r.id.WALSegmentBytes)
	if e != nil {
		return "", e
	}
	return fmt.Sprintf("%swal/%08X/%s", r.root, timeline, name), nil
}
func (r *Repository) putWALTombstone(ctx context.Context, key string, b []byte, etag string) (s3store.Info, error) {
	// As with put, spool failures are definitive only BEFORE dispatch.
	f, e := r.temp()
	if e != nil {
		return s3store.Info{}, &s3store.Error{Kind: s3store.LocalIO}
	}
	defer removeFile(f)
	if _, e = f.Write(b); e != nil {
		return s3store.Info{}, &s3store.Error{Kind: s3store.LocalIO}
	}
	return r.store.PutFile(ctx, key, f, s3store.Integrity{Size: int64(len(b)), SHA256: digest(b)}, s3store.Condition{Match: etag}, map[string]string{"cnpg-format": "wal-retired-v1"})
}
