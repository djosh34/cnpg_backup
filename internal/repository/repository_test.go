package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

const repoID = "11111111-1111-4111-8111-111111111111"
const writerID = "22222222-2222-4222-8222-222222222222"
const backupID = "33333333-3333-4333-8333-333333333333"
const targetID = "44444444-4444-4444-8444-444444444444"
const destID = "55555555-5555-4555-8555-555555555555"
const capturedID = "66666666-6666-4666-8666-666666666666"

var ctx = context.Background()

type object struct {
	b    []byte
	info s3store.Info
}
type fakeStore struct {
	mu                 sync.Mutex
	objects            map[string]object
	version, n, failAt int
	fault              string
	pending            []func()
	trace              []string
	partialList        bool
	deletes            int
	oracleErrors       []string
}

func (s *fakeStore) CheckBucketSafety(context.Context) error { return nil }
func (s *fakeStore) Head(c context.Context, k string) (s3store.Info, error) {
	_, i, e := s.Read(c, k, MaxArtifactBytes)
	return i, e
}
func newFake() *fakeStore { return &fakeStore{objects: map[string]object{}} }
func (s *fakeStore) Read(_ context.Context, k string, max int64) ([]byte, s3store.Info, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[k]
	if !ok {
		return nil, s3store.Info{}, &s3store.Error{Kind: s3store.NotFound}
	}
	if int64(len(o.b)) > max {
		return nil, o.info, &s3store.Error{Kind: s3store.Limit}
	}
	return bytes.Clone(o.b), o.info, nil
}
func (s *fakeStore) mutate(k string, b []byte, c s3store.Condition, destructive bool) (s3store.Info, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	fault := ""
	if s.n == s.failAt {
		fault = s.fault
	}
	s.trace = append(s.trace, fmt.Sprintf("%d dispatch %s %s", s.n, operationName(k, destructive), fault))
	apply := func() (s3store.Info, error) {
		o, ok := s.objects[k]
		if c.Create && ok || c.Match != "" && (!ok || c.Match != o.info.ETag) {
			s.trace = append(s.trace, "rejected stale CAS")
			return s3store.Info{}, &s3store.Error{Kind: s3store.Precondition}
		}
		if destructive {
			s.deletes++
			for key, v := range s.objects {
				if strings.HasSuffix(key, "/gate.json") {
					var gate struct {
						Holders []json.RawMessage `json:"holders"`
						Owner   *json.RawMessage  `json:"owner"`
					}
					json.Unmarshal(v.b, &gate)
					if len(gate.Holders) != 0 || gate.Owner == nil {
						s.oracleErrors = append(s.oracleErrors, "destruction without exclusive owner")
					}
				}
			}
		}
		s.version++
		info := s3store.Info{Key: k, ETag: fmt.Sprint(s.version), Size: int64(len(b)), Modified: time.Unix(int64(s.version), 0).UTC()}
		if destructive && b == nil {
			delete(s.objects, k)
		} else {
			s.objects[k] = object{bytes.Clone(b), info}
		}
		s.trace = append(s.trace, "durable "+operationName(k, destructive))
		if e := independentOracle(s.objects); e != nil {
			s.oracleErrors = append(s.oracleErrors, e.Error())
		}
		return info, nil
	}
	switch fault {
	case "reject":
		return s3store.Info{}, &s3store.Error{Kind: s3store.Auth}
	case "delay":
		s.pending = append(s.pending, func() { apply() })
		return s3store.Info{}, &s3store.Error{Kind: s3store.Transient, Ambiguous: true}
	case "lost":
		i, e := apply()
		if e != nil {
			return i, e
		}
		return i, &s3store.Error{Kind: s3store.Transient, Ambiguous: true}
	default:
		return apply()
	}
}
func operationName(k string, d bool) string {
	if strings.Contains(k, "/gc/") {
		return "gc-plan"
	}
	if d {
		return "destructive"
	}
	p := strings.Split(k, "/")
	last := p[len(p)-1]
	if last == "gate.json" {
		return "gate"
	}
	if strings.HasSuffix(last, ".tar") || strings.HasSuffix(last, ".gz") {
		return "artifact"
	}
	return last
}
func (s *fakeStore) PutFile(_ context.Context, k string, f *os.File, in s3store.Integrity, c s3store.Condition, m map[string]string) (s3store.Info, error) {
	b, e := io.ReadAll(io.NewSectionReader(f, 0, in.Size))
	if e != nil {
		return s3store.Info{}, e
	}
	if digest(b) != in.SHA256 {
		return s3store.Info{}, ErrCorrupt
	}
	destructive := strings.HasSuffix(k, "/retired.json") || m["cnpg-format"] == "wal-retired-v1"
	return s.mutate(k, b, c, destructive)
}
func (s *fakeStore) UploadFile(_ context.Context, k string, f *os.File, in s3store.Integrity, _ map[string]string) (s3store.Upload, s3store.Info, error) {
	b, e := io.ReadAll(io.NewSectionReader(f, 0, in.Size))
	if e != nil {
		return s3store.Upload{}, s3store.Info{}, e
	}
	if digest(b) != in.SHA256 {
		return s3store.Upload{}, s3store.Info{}, ErrCorrupt
	}
	i, e := s.mutate(k, b, s3store.Condition{}, false)
	return s3store.Upload{Key: k, ID: "upload"}, i, e
}
func (s *fakeStore) Download(_ context.Context, k string, f *os.File, in s3store.Integrity) (s3store.Info, error) {
	b, i, e := s.Read(ctx, k, in.Size+1)
	if e != nil {
		return i, e
	}
	if int64(len(b)) != in.Size || digest(b) != in.SHA256 {
		return i, ErrCorrupt
	}
	if e = f.Truncate(0); e != nil {
		return i, e
	}
	if _, e = f.Seek(0, 0); e != nil {
		return i, e
	}
	_, e = f.Write(b)
	return i, e
}
func (s *fakeStore) List(_ context.Context, p string, max int, visit func(s3store.Info) error) error {
	s.mu.Lock()
	var list []s3store.Info
	for k, o := range s.objects {
		if strings.HasPrefix(k, p) {
			list = append(list, o.info)
		}
	}
	partial := s.partialList
	s.mu.Unlock()
	sort.Slice(list, func(i, j int) bool { return list[i].Key < list[j].Key })
	for i, v := range list {
		if i >= max {
			return ErrCapacity
		}
		if e := visit(v); e != nil {
			return e
		}
		if partial {
			return &s3store.Error{Kind: s3store.Transient}
		}
	}
	return nil
}
func (s *fakeStore) Delete(_ context.Context, k string) error {
	_, e := s.mutate(k, nil, s3store.Condition{}, true)
	return e
}
func (s *fakeStore) Abort(_ context.Context, u s3store.Upload) error {
	_, e := s.mutate(u.Key+".upload", nil, s3store.Condition{}, true)
	return e
}
func (s *fakeStore) ListUploads(context.Context, string, int, func(s3store.Upload) error) error {
	return nil
}
func (s *fakeStore) deliver() {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.pending
	s.pending = nil
	for _, f := range p {
		f()
	}
}

// Independent byte-level oracle: does NOT call production validation, key
// construction, graph selection or retirement planning. Inspect after effects,
// not API returns; incomplete/retired attempts may exist without being visible.
func independentOracle(objects map[string]object) error {
	keys := make([]string, 0, len(objects))
	for k := range objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if root, _, wal := strings.Cut(k, "/wal/"); wal {
			// Check each observed WAL-retirement effect against its persisted
			// victim set, independently of production plan/order validation.
			var tomb struct {
				State     string `json:"state"`
				Operation string `json:"gc_operation_id"`
			}
			if json.Unmarshal(objects[k].b, &tomb) == nil && tomb.State == "retired" {
				var plan struct {
					Victims []struct {
						Kind   string `json:"kind"`
						Backup string `json:"backup_uid"`
					} `json:"victims"`
				}
				p, ok := objects[root+"/gc/"+tomb.Operation+".json"]
				if !ok || json.Unmarshal(p.b, &plan) != nil {
					return errors.New("WAL retirement lacks durable plan")
				}
				for _, v := range plan.Victims {
					if v.Kind == "retire-backup" {
						if _, ok := objects[root+"/backups/"+v.Backup+"/retired.json"]; !ok {
							return errors.New("WAL retired before planned backup exclusion")
						}
					}
				}
			}
		}
		if !strings.HasSuffix(k, "/commit.json") {
			continue
		}
		root := strings.TrimSuffix(k, "commit.json")
		if _, retired := objects[root+"retired.json"]; retired {
			continue
		}
		var c struct {
			Attempt      string  `json:"attempt_id"`
			Parent       *string `json:"parent_backup_uid"`
			ManifestSize int64   `json:"manifest_bytes"`
			ManifestHash string  `json:"manifest_sha256"`
			Artifacts    []struct {
				Index       int    `json:"index"`
				Compression string `json:"compression"`
				Size        int64  `json:"stored_bytes"`
				Hash        string `json:"stored_sha256"`
			} `json:"artifacts"`
		}
		if json.Unmarshal(objects[k].b, &c) != nil {
			return errors.New("unparseable visible commit")
		}
		attempt := root + "attempts/" + c.Attempt + "/"
		check := func(key string, size int64, hash string) error {
			o, ok := objects[key]
			sum := sha256.Sum256(o.b)
			if !ok || int64(len(o.b)) != size || hex.EncodeToString(sum[:]) != hash {
				return errors.New("visible commit references absent/damaged payload")
			}
			return nil
		}
		if e := check(attempt+"manifest.pg.json", c.ManifestSize, c.ManifestHash); e != nil {
			return e
		}
		for _, a := range c.Artifacts {
			key := fmt.Sprintf("%sdata/%d.tar", attempt, a.Index)
			if a.Compression == "gzip" {
				key += ".gz"
			}
			if e := check(key, a.Size, a.Hash); e != nil {
				return e
			}
		}
		if c.Parent != nil {
			parts := strings.Split(root, "/")
			parent := strings.Join(parts[:len(parts)-2], "/") + "/" + *c.Parent + "/"
			if _, ok := objects[parent+"commit.json"]; !ok {
				return errors.New("live differential lost full parent")
			}
			if _, ok := objects[parent+"retired.json"]; ok {
				return errors.New("live differential has retired full parent")
			}
		}
	}
	return nil
}
func identity() Identity {
	return Identity{1, repoID, 18, "123456789", 16 << 20, writerID, "2026-09-07T00:00:00Z"}
}
func setup(t *testing.T) (*Repository, *fakeStore) {
	t.Helper()
	s := newFake()
	dir := t.TempDir()
	if e := initialize(ctx, s, identity(), dir); e != nil {
		t.Fatal(e)
	}
	r, e := Open(ctx, s, identity(), dir)
	if e != nil {
		t.Fatal(e)
	}
	return r, s
}
func file(t *testing.T, b []byte) *os.File {
	t.Helper()
	f, e := os.CreateTemp(t.TempDir(), "fixture")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.Write(b); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { f.Close() })
	return f
}
func request(uid string) Request {
	return Request{1, repoID, uid, writerID, "full", nil, digest([]byte("nonsecret-settings"))}
}
func capture(t *testing.T, a *Attempt, body string) (Commit, *os.File, []*os.File) {
	t.Helper()
	m := []byte("original native manifest fixture")
	data := [][]byte{[]byte(body), []byte("bootstrap WAL tar fixture")}
	arts := []Artifact{}
	files := []*os.File{}
	for i, b := range data {
		role := "base"
		if i == 1 {
			role = "wal"
		}
		arts = append(arts, Artifact{i, role, nil, "none", int64(len(b)), int64(len(b)), digest(b), digest(b)})
		files = append(files, file(t, b))
	}
	c := Commit{Schema: 1, RepositoryID: repoID, BackupUID: a.claim.BackupUID, AttemptID: a.ID(), RequestSHA256: a.claim.RequestSHA256, Kind: "full", RootBackupUID: a.claim.BackupUID, SystemIdentifier: identity().SystemIdentifier, PostgresMajor: 18, ToolVersion: "18.6", Timeline: 1, ChecksumVersion: 1, CaptureInstanceUID: capturedID, PostmasterStartedAt: "2026-09-07T00:00:00Z", StartedAt: "2026-09-07T01:00:00Z", StoppedAt: "2026-09-07T01:01:00Z", StartLSN: "0/1000028", StopLSN: "0/1000100", RedoLSN: "0/1000028", BundledWALStartLSN: "0/1000028", BundledWALEndLSN: "0/1000100", WALRanges: []WALRange{{1, "0/1000028", "0/1000100"}}, BackupLabel: "native label fixture", Tablespaces: []Tablespace{}, ManifestBytes: int64(len(m)), ManifestSHA256: digest(m), Artifacts: arts}
	return c, file(t, m), files
}
func publish(t *testing.T, r *Repository, uid, body string) (*Hold, *Result) {
	t.Helper()
	h, e := r.AdmitBackup(ctx, writerID, uid)
	if e != nil {
		t.Fatal(e)
	}
	a, old, e := h.Begin(ctx, request(uid))
	if e != nil || old != nil {
		t.Fatalf("begin %v %v", old, e)
	}
	c, m, files := capture(t, a, body)
	res, e := a.Publish(ctx, c, m, files)
	if e != nil {
		t.Fatal(e)
	}
	return h, res
}
func requireOracle(t *testing.T, s *fakeStore) {
	t.Helper()
	if len(s.oracleErrors) != 0 {
		t.Fatal(s.oracleErrors)
	}
	if e := independentOracle(s.objects); e != nil {
		t.Fatal(e)
	}
}

func TestPublicationCrashBoundaries(t *testing.T) {
	for _, fault := range []string{"reject", "lost", "delay"} {
		for boundary := 1; boundary <= 6; boundary++ {
			t.Run(fmt.Sprintf("%s-%d", fault, boundary), func(t *testing.T) {
				r, s := setup(t)
				h, e := r.AdmitBackup(ctx, writerID, backupID)
				if e != nil {
					t.Fatal(e)
				}
				s.failAt = s.n + boundary
				s.fault = fault
				a, _, e := h.Begin(ctx, request(backupID))
				if e == nil {
					c, m, fs := capture(t, a, "first")
					_, e = a.Publish(ctx, c, m, fs)
				}
				s.deliver()
				requireOracle(t, s)
				// Restart throws away all producer-local state. Old uncertain holder remains;
				// a fresh restore admits without any source Kubernetes or prior controller.
				rr, e := Open(ctx, s, identity(), r.workspace)
				if e != nil {
					t.Fatal(e)
				}
				rh, e := rr.AdmitRestore(ctx, targetID, UUID())
				if e != nil {
					t.Fatal(e)
				}
				cat, e := rh.Catalog(ctx, CatalogLimits{10, 32 << 20})
				if e != nil {
					t.Fatal(e)
				}
				seen := 0
				if e = cat.Visit(ctx, func(Entry) error { seen++; return nil }); e != nil {
					t.Fatal(e)
				}
				cat.Close()
				if seen > 1 {
					t.Fatal("multiple UID winners")
				}
				// Once faults clear a new holder/attempt can publish or replay the winner.
				fresh, e := rr.AdmitBackup(ctx, writerID, backupID)
				if e != nil {
					t.Fatal(e)
				}
				a, res, e := fresh.Begin(ctx, request(backupID))
				if e != nil {
					t.Fatal(e)
				}
				if res == nil {
					c, m, fs := capture(t, a, "retry")
					if _, e = a.Publish(ctx, c, m, fs); e != nil {
						t.Fatal(e)
					}
				}
				requireOracle(t, s)
			})
		}
	}
}
func TestUIDFirstWinnerAndVerifiedReplay(t *testing.T) {
	r, s := setup(t)
	h1, _ := r.AdmitBackup(ctx, writerID, backupID)
	h2, _ := r.AdmitBackup(ctx, writerID, backupID)
	a1, _, e := h1.Begin(ctx, request(backupID))
	if e != nil {
		t.Fatal(e)
	}
	a2, _, e := h2.Begin(ctx, request(backupID))
	if e != nil {
		t.Fatal(e)
	}
	c2, m2, f2 := capture(t, a2, "winner")
	win, e := a2.Publish(ctx, c2, m2, f2)
	if e != nil {
		t.Fatal(e)
	}
	c1, m1, f1 := capture(t, a1, "loser")
	got, e := a1.Publish(ctx, c1, m1, f1)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(win.Bytes, got.Bytes) || !got.PublishedAt.Equal(win.PublishedAt) || got.Commit.AttemptID != a2.ID() {
		t.Fatal("returned loser instead of exact durable winner")
	}
	requireOracle(t, s)
	changed := request(backupID)
	changed.ConfigSHA256 = digest([]byte("changed"))
	if _, _, e = h1.Begin(ctx, changed); e != ErrIdentity {
		t.Fatalf("semantic conflict %v", e)
	}
	delete(s.objects, r.artifact(win.Commit, win.Commit.Artifacts[0]))
	if _, _, e = h1.Begin(ctx, request(backupID)); e == nil {
		t.Fatal("acknowledged damaged winner")
	}
}
func TestGateAmbiguityBarrierAndStaleCAS(t *testing.T) {
	r, s := setup(t)
	old, oi, e := r.readGate(ctx)
	if e != nil {
		t.Fatal(e)
	}
	s.failAt = s.n + 1
	s.fault = "delay"
	h, e := r.AdmitRestore(ctx, targetID, UUID())
	if e != nil {
		t.Fatal(e)
	}
	before, _, _ := r.readGate(ctx)
	s.deliver()
	after, _, _ := r.readGate(ctx)
	if before.Generation != after.Generation {
		t.Fatal("delayed gate CAS escaped barrier")
	}
	if e = h.check(ctx); e != nil {
		t.Fatal(e)
	}
	old.Generation = "1"
	old.Nonce = UUID()
	b, _ := json.Marshal(old)
	if _, e = r.put(ctx, r.root+"gate.json", b, s3store.Condition{Match: oi.ETag}); !s3store.Is(e, s3store.Precondition) {
		t.Fatal(e)
	}
	requireOracle(t, s)
}
func TestHoldersNeverExpireAndCloseDrains(t *testing.T) {
	r, _ := setup(t)
	h, e := r.AdmitBackup(ctx, writerID, backupID)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = r.AcquireGC(ctx); e != ErrBlocked {
		t.Fatal(e)
	}
	reader, e := r.AdmitRestore(ctx, targetID, UUID())
	if e != nil {
		t.Fatal(e)
	}
	if e = r.ReleaseLifetimeAfterTermination(ctx, targetID, reader.holder.OperationID); e != nil {
		t.Fatal(e)
	}
	if e = reader.check(ctx); e != ErrBlocked {
		t.Fatal("terminal lifetime still reads", e)
	}
	if _, e = r.AcquireGC(ctx); e != ErrBlocked {
		t.Fatal(e)
	}
	if e = reader.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if e = h.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if _, _, e = h.Begin(ctx, request(backupID)); e != ErrClosed {
		t.Fatal(e)
	}
	owner, e := r.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = r.AdmitRestore(ctx, targetID, UUID()); e != ErrBlocked {
		t.Fatal(e)
	}
	if e = owner.Close(ctx); e != nil {
		t.Fatal(e)
	}
}
func TestCatalogPartialAndResourceBounds(t *testing.T) {
	r, s := setup(t)
	h, _ := publish(t, r, backupID, "data")
	s.partialList = true
	if c, e := h.Catalog(ctx, CatalogLimits{10, 32 << 20}); e == nil {
		c.Close()
		t.Fatal("partial catalog escaped")
	}
	s.partialList = false
	cat, e := h.Catalog(ctx, CatalogLimits{1, commitLimit})
	if e != nil {
		t.Fatal(e)
	}
	cat.Close()
	if _, e = h.Catalog(ctx, CatalogLimits{MaxCatalogRecords + 1, commitLimit}); e != ErrCapacity {
		t.Fatal(e)
	}
	entries, _ := os.ReadDir(r.workspace)
	if len(entries) != 0 {
		t.Fatal("spools leaked", entries)
	}
	s.objects[r.root+"backups/../commit.json"] = object{nil, s3store.Info{Key: r.root + "backups/../commit.json"}}
	if _, e = h.Catalog(ctx, CatalogLimits{10, 32 << 20}); e != ErrCorrupt {
		t.Fatal(e)
	}
}
func TestIndependentOracleNegativeControl(t *testing.T) {
	r, s := setup(t)
	_, res := publish(t, r, backupID, "data")
	requireOracle(t, s)
	delete(s.objects, r.artifact(res.Commit, res.Commit.Artifacts[1]))
	if e := independentOracle(s.objects); e == nil {
		t.Fatal("oracle failed deliberately broken publication")
	}
}
func TestDeterministicProductionSimulation(t *testing.T) {
	replay := func(seed int64) string {
		rng := rand.New(rand.NewSource(seed))
		var trace []string
		for i := 0; i < 16; i++ {
			r, s := setup(t)
			h, _ := r.AdmitBackup(ctx, writerID, backupID)
			s.failAt = s.n + 1 + rng.Intn(6)
			s.fault = []string{"reject", "lost", "delay"}[rng.Intn(3)]
			a, _, e := h.Begin(ctx, request(backupID))
			if e == nil {
				c, m, f := capture(t, a, "seeded")
				_, _ = a.Publish(ctx, c, m, f)
			}
			s.deliver()
			requireOracle(t, s)
			trace = append(trace, fmt.Sprintf("event %d", i))
			trace = append(trace, s.trace...)
		}
		return strings.Join(trace, "\n")
	}
	for _, seed := range []int64{9, 17, 5564016816} {
		a, b := replay(seed), replay(seed)
		if a != b {
			t.Fatalf("seed %d not reproducible", seed)
		}
		t.Logf("seed=%d operations=16 trace_sha256=%s\n%s", seed, digest([]byte(a)), a)
	}
}
