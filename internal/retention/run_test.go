package retention

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
	"github.com/djosh34/cnpg_backup/internal/wal"
)

type stored struct {
	body []byte
	info s3store.Info
}
type inventoryStore struct {
	objects         map[string]stored
	root            string
	effects, lists  int
	partial, unsafe bool
	beforeList      func(string) error
	afterList       func(string) error
	beforeEffect    func(string) error
	afterEffect     func(string) error
	uploads         []s3store.Upload
	uploadError     error
	downloads       int
	afterPut        func(string) error
	downloadHook    func(string, *os.File, s3store.Integrity) (bool, error)
}

func sum(b []byte) string { v := sha256.Sum256(b); return hex.EncodeToString(v[:]) }
func (s *inventoryStore) seed(key string, b []byte) {
	s.objects[key] = stored{b, s3store.Info{Key: key, ETag: sum(b), Size: int64(len(b)), Modified: time.Now().UTC()}}
}
func (s *inventoryStore) CheckBucketSafety(context.Context) error { return nil }
func (s *inventoryStore) Read(_ context.Context, key string, max int64) ([]byte, s3store.Info, error) {
	v, ok := s.objects[key]
	if !ok {
		return nil, s3store.Info{}, &s3store.Error{Kind: s3store.NotFound}
	}
	if int64(len(v.body)) > max {
		return nil, v.info, &s3store.Error{Kind: s3store.Limit}
	}
	return v.body, v.info, nil
}
func (s *inventoryStore) Head(c context.Context, k string) (s3store.Info, error) {
	_, v, e := s.Read(c, k, 1<<30)
	return v, e
}
func (s *inventoryStore) exclusive() {
	var gate struct {
		Owner   *json.RawMessage  `json:"owner"`
		Holders []json.RawMessage `json:"holders"`
	}
	json.Unmarshal(s.objects[s.root+"gate.json"].body, &gate)
	if gate.Owner == nil || len(gate.Holders) > 0 {
		s.unsafe = true
	}
}
func (s *inventoryStore) PutFile(_ context.Context, k string, f *os.File, want s3store.Integrity, c s3store.Condition, m map[string]string) (s3store.Info, error) {
	old, exists := s.objects[k]
	if c.Create && exists || c.Match != "" && (!exists || c.Match != old.info.ETag) {
		return s3store.Info{}, &s3store.Error{Kind: s3store.Precondition}
	}
	if strings.HasSuffix(k, "/retired.json") || m["cnpg-format"] == "wal-retired-v1" {
		if s.beforeEffect != nil {
			if e := s.beforeEffect(k); e != nil {
				return s3store.Info{}, e
			}
		}
		s.exclusive()
		s.effects++
	}
	b, e := io.ReadAll(io.NewSectionReader(f, 0, want.Size))
	if e != nil {
		return s3store.Info{}, e
	}
	if sum(b) != want.SHA256 {
		return s3store.Info{}, repository.ErrCorrupt
	}
	s.seed(k, b)
	v := s.objects[k]
	v.info.Metadata = m
	s.objects[k] = v
	if s.afterPut != nil {
		if e := s.afterPut(k); e != nil {
			return v.info, e
		}
	}
	if s.afterEffect != nil && (strings.HasSuffix(k, "/retired.json") || m["cnpg-format"] == "wal-retired-v1") {
		if e := s.afterEffect(k); e != nil {
			return v.info, e
		}
	}
	return v.info, nil
}
func (s *inventoryStore) UploadFile(c context.Context, k string, f *os.File, i s3store.Integrity, m map[string]string) (s3store.Upload, s3store.Info, error) {
	v, e := s.PutFile(c, k, f, i, s3store.Condition{Create: true}, m)
	return s3store.Upload{}, v, e
}
func (s *inventoryStore) Download(c context.Context, k string, f *os.File, want s3store.Integrity) (s3store.Info, error) {
	s.downloads++
	if s.downloadHook != nil {
		if handled, e := s.downloadHook(k, f, want); handled {
			return s.objects[k].info, e
		}
	}
	b, i, e := s.Read(c, k, want.Size)
	if e != nil {
		return i, e
	}
	if sum(b) != want.SHA256 {
		return i, repository.ErrCorrupt
	}
	_, e = f.WriteAt(b, 0)
	return i, e
}
func (s *inventoryStore) DownloadWAL(c context.Context, k string, f *os.File, want s3store.Integrity) (s3store.Info, error) {
	return s.Download(c, k, f, want)
}
func (s *inventoryStore) List(_ context.Context, p string, max int, visit func(s3store.Info) error) error {
	s.exclusive()
	s.lists++
	if s.beforeList != nil {
		if e := s.beforeList(p); e != nil {
			return e
		}
	}
	keys := []string{}
	for k := range s.objects {
		if strings.HasPrefix(k, p) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for i, k := range keys {
		if i >= max {
			return repository.ErrCapacity
		}
		if e := visit(s.objects[k].info); e != nil {
			return e
		}
		if s.partial {
			return &s3store.Error{Kind: s3store.Transient}
		}
	}
	if s.afterList != nil {
		return s.afterList(p)
	}
	return nil
}
func (s *inventoryStore) ListUploads(_ context.Context, _ string, _ int, visit func(s3store.Upload) error) error {
	s.exclusive()
	for _, u := range s.uploads {
		if e := visit(u); e != nil {
			return e
		}
	}
	if s.partial {
		return repository.ErrCorrupt
	}
	return s.uploadError
}
func (s *inventoryStore) Delete(_ context.Context, k string) error {
	if s.beforeEffect != nil {
		if e := s.beforeEffect(k); e != nil {
			return e
		}
	}
	s.exclusive()
	s.effects++
	delete(s.objects, k)
	if s.afterEffect != nil {
		return s.afterEffect(k)
	}
	return nil
}
func (s *inventoryStore) Abort(_ context.Context, u s3store.Upload) error {
	if s.beforeEffect != nil {
		if e := s.beforeEffect(u.Key); e != nil {
			return e
		}
	}
	s.exclusive()
	s.effects++
	for i, v := range s.uploads {
		if v == u {
			s.uploads = append(s.uploads[:i], s.uploads[i+1:]...)
			break
		}
	}
	if s.afterEffect != nil {
		return s.afterEffect(u.Key)
	}
	return nil
}

func runtimeFixture(t *testing.T) (*repository.Repository, *inventoryStore, wal.Files, time.Time, []string) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	id := repository.Identity{Schema: 1, RepositoryID: repository.UUID(), PostgresMajor: 18, SystemIdentifier: "7512345678901234567", WALSegmentBytes: 1 << 24, WriterClusterUID: repository.UUID(), CreatedAt: now.Add(-72 * time.Hour).Format(time.RFC3339Nano)}
	s := &inventoryStore{objects: map[string]stored{}, root: "v1/" + id.RepositoryID + "/"}
	putJSON := func(k string, v any) []byte {
		b, e := json.Marshal(v)
		if e != nil {
			t.Fatal(e)
		}
		s.seed(s.root+k, b)
		return b
	}
	putJSON("repository.json", id)
	putJSON("gate.json", repository.Gate{Schema: 1, RepositoryID: id.RepositoryID, Generation: "0", Nonce: repository.UUID(), Holders: []repository.Holder{}})
	ids := []string{}
	for i := 0; i < 2; i++ {
		uid, attempt := repository.UUID(), repository.UUID()
		ids = append(ids, uid)
		q := repository.Request{Schema: 1, RepositoryID: id.RepositoryID, BackupUID: uid, WriterClusterUID: id.WriterClusterUID, RequestedKind: "full", ConfigSHA256: sum([]byte("config"))}
		qb := putJSON("backups/"+uid+"/request.json", q)
		prefix := "backups/" + uid + "/attempts/" + attempt + "/"
		putJSON(prefix+"claim.json", repository.Claim{Schema: 1, RepositoryID: id.RepositoryID, BackupUID: uid, AttemptID: attempt, ProcessID: repository.UUID(), RequestSHA256: sum(qb)})
		body := []byte("bounded verified synthetic native fixture")
		hash := sum(body)
		s.seed(s.root+prefix+"manifest.pg.json", body)
		arts := []repository.Artifact{}
		for index, role := range []string{"base", "wal"} {
			s.seed(s.root+prefix+fmt.Sprintf("data/%d.tar", index), body)
			arts = append(arts, repository.Artifact{Index: index, Role: role, Compression: "none", StoredBytes: int64(len(body)), RawBytes: int64(len(body)), StoredSHA256: hash, RawSHA256: hash})
		}
		start := fmt.Sprintf("0/%X", (i+1)<<24)
		end := fmt.Sprintf("0/%X", (i+1)<<24|0x100)
		c := repository.Commit{Schema: 1, RepositoryID: id.RepositoryID, BackupUID: uid, AttemptID: attempt, RequestSHA256: sum(qb), Kind: "full", RootBackupUID: uid, SystemIdentifier: id.SystemIdentifier, PostgresMajor: 18, ToolVersion: "18.6", Timeline: 1, ChecksumVersion: 1, CaptureInstanceUID: repository.UUID(), PostmasterStartedAt: now.Add(-72 * time.Hour).Format(time.RFC3339Nano), StartedAt: now.Add(time.Duration(i-4) * time.Hour).Format(time.RFC3339Nano), StoppedAt: now.Add(time.Duration(i-4)*time.Hour + time.Minute).Format(time.RFC3339Nano), StartLSN: start, RedoLSN: start, StopLSN: end, BundledWALStartLSN: start, BundledWALEndLSN: end, WALRanges: []repository.WALRange{{Timeline: 1, StartLSN: start, EndLSN: end}}, BackupLabel: "fixture", Tablespaces: []repository.Tablespace{}, Artifacts: arts, ManifestBytes: int64(len(body)), ManifestSHA256: hash}
		putJSON("backups/"+uid+"/commit.json", c)
	}
	dir := t.TempDir()
	r, e := repository.OpenSource(ctx, s, id.RepositoryID, dir)
	if e != nil {
		t.Fatal(e)
	}
	return r, s, wal.Files{Repository: r, Store: s, Workspace: dir, Compression: "none"}, now, ids
}
func TestActualRunnerAdmissionDryRunAndPermanentRestartCleanup(t *testing.T) {
	r, s, files, now, ids := runtimeFixture(t)
	ctx := context.Background()
	options := Options{Window: time.Hour, MinimumFulls: 1, DryRun: true}
	if _, e := Run(ctx, r, files, now, options); e != nil || s.lists != 0 {
		t.Fatal("disabled inventoried", e)
	}
	options.Enabled = true
	result, e := Run(ctx, r, files, now, options)
	if e != nil || result.Planned != 1 || s.effects != 0 {
		t.Fatal("dry run", result, e)
	}
	options.DryRun = false
	for n := 0; n < 3; n++ {
		r, e = repository.OpenSource(ctx, s, r.Identity().RepositoryID, t.TempDir())
		if e != nil {
			t.Fatal(e)
		}
		result, e = Run(ctx, r, files, now, options)
		if e != nil {
			t.Fatal("new batch", n, result, e)
		}
	}
	if result.Planned != 0 || s.effects != 4 || s.unsafe {
		t.Fatal("unsafe/incomplete runtime", result, s.effects, s.unsafe)
	}
	if _, ok := s.objects[s.root+"backups/"+ids[0]+"/retired.json"]; !ok {
		t.Fatal("old backup not retired")
	}
	if _, ok := s.objects[s.root+"backups/"+ids[1]+"/retired.json"]; ok {
		t.Fatal("last root lost")
	}
	holder, e := r.AdmitRestore(ctx, repository.UUID(), repository.UUID())
	if e != nil {
		t.Fatal(e)
	}
	before := s.lists
	if _, e = Run(ctx, r, files, now.Add(365*24*time.Hour), options); e != repository.ErrBlocked || s.lists != before {
		t.Fatal("inventoried under holder or clock expired it", e)
	}
	_ = holder // deliberately crashed reader; never clear from another incarnation
}
func TestActualRunnerIncompleteInventoryStopsAllDeletion(t *testing.T) {
	for _, fault := range []string{"partial", "missing-manifest", "unknown-WAL"} {
		t.Run(fault, func(t *testing.T) {
			r, s, files, now, _ := runtimeFixture(t)
			switch fault {
			case "partial":
				s.partial = true
			case "missing-manifest":
				for k := range s.objects {
					if strings.HasSuffix(k, "/manifest.pg.json") {
						delete(s.objects, k)
						break
					}
				}
			case "unknown-WAL":
				s.seed(s.root+"wal/00000001/000000010000000000000000", []byte("untrusted"))
			}
			if _, e := Run(context.Background(), r, files, now, Options{Enabled: true, Window: time.Hour, MinimumFulls: 1}); e == nil || s.effects != 0 || s.unsafe {
				t.Fatal("uncertainty authorized deletion", s.effects, s.unsafe, e)
			}
		})
	}
}

func runOptions() Options { return Options{Enabled: true, Window: time.Hour, MinimumFulls: 1} }

func TestRunSlowCompleteInventoryMakesProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, s, files, now, _ := runtimeFixture(t)
		delayed := false
		s.beforeList = func(string) error {
			if !delayed {
				time.Sleep(31 * time.Second)
				delayed = true
			}
			return nil
		}
		result, e := Run(context.Background(), r, files, now, runOptions())
		if e != nil || !result.Executed || s.effects != 1 {
			t.Fatalf("healthy inventory livelocked: %+v effects=%d err=%v", result, s.effects, e)
		}
		if s.downloads != 2 {
			t.Fatalf("redundant catalog validation: %d downloads", s.downloads)
		}
	})
}

func emptyFailedAttempt(t *testing.T) (*repository.Repository, *inventoryStore, wal.Files, time.Time) {
	t.Helper()
	r, s, files, now, _ := runtimeFixture(t)
	for k := range s.objects {
		if strings.Contains(k, "/backups/") {
			delete(s.objects, k)
		}
	}
	uid := repository.UUID()
	h, e := r.AdmitBackup(context.Background(), r.Identity().WriterClusterUID, uid)
	if e != nil {
		t.Fatal(e)
	}
	a, _, e := h.Begin(context.Background(), repository.Request{Schema: 1, RepositoryID: r.Identity().RepositoryID, BackupUID: uid, WriterClusterUID: r.Identity().WriterClusterUID, RequestedKind: "full", ConfigSHA256: sum([]byte("config"))})
	if e != nil {
		t.Fatal(e)
	}
	prefix := s.root + "backups/" + uid + "/attempts/" + a.ID() + "/"
	s.seed(prefix+"manifest.pg.json", []byte("failed initial input"))
	s.seed(prefix+"data/0.tar", []byte("failed payload"))
	s.uploads = []s3store.Upload{{Key: prefix + "data/1.tar", ID: "known-upload"}}
	if e = h.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	return r, s, files, now
}

func TestRunValidEmptyCatalogOnlyCleansProvenOrphans(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, s, files, now := emptyFailedAttempt(t)
		// Authenticated live WAL is deliberately NOT reclamation entitlement.
		name := "000000010000000000000001"
		s.seed(s.root+"wal/00000001/"+name, []byte("not downloaded by inventory"))
		v := s.objects[s.root+"wal/00000001/"+name]
		v.info.Size = r.Identity().WALSegmentBytes
		v.info.Metadata = map[string]string{"cnpg-format": "wal-v1", "cnpg-system-id": r.Identity().SystemIdentifier, "cnpg-raw-bytes": fmt.Sprint(r.Identity().WALSegmentBytes), "cnpg-raw-sha256": sum(v.body), "cnpg-stored-sha256": sum(v.body), "cnpg-compression": "none"}
		s.objects[v.info.Key] = v
		o := runOptions()
		o.DryRun = true
		result, e := Run(context.Background(), r, files, now, o)
		if e != nil || result.Planned != 3 || !result.Decision.Shortened || result.Decision.Current != 0 || s.effects != 0 {
			t.Fatalf("empty dry-run: %+v %v effects=%d", result, e, s.effects)
		}
		o.DryRun = false
		for pass := 0; pass < 2; pass++ {
			result, e = Run(context.Background(), r, files, now, o)
			if e != nil {
				t.Fatal(e)
			}
		}
		if result.Planned != 0 || s.effects != 3 || len(s.uploads) != 0 || s.unsafe {
			t.Fatal(result, s.effects, s.uploads, s.unsafe)
		}
		if s.objects[v.info.Key].info.Metadata["cnpg-format"] != "wal-v1" {
			t.Fatal("empty catalog retired WAL")
		}
	})
}

func TestRunLateInventoryFailureNoVictimsAndCleanRelease(t *testing.T) {
	for _, empty := range []bool{false, true} {
		for _, phase := range []string{"archive", "mpu"} {
			t.Run(fmt.Sprint(empty)+phase, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					r, s, files, now, _ := runtimeFixture(t)
					if empty {
						r, s, files, now = emptyFailedAttempt(t)
					}
					fault := errors.New("late inventory page failure")
					if phase == "archive" {
						s.beforeList = func(p string) error {
							if strings.HasSuffix(p, "/wal/") {
								return fault
							}
							return nil
						}
					} else {
						s.uploadError = fault
					}
					if _, e := Run(context.Background(), r, files, now, runOptions()); !errors.Is(e, fault) || s.effects != 0 {
						t.Fatal("partial inventory authorized effects", e, s.effects)
					}
					target, operation := repository.UUID(), repository.UUID()
					h, e := r.AdmitRestore(context.Background(), target, operation)
					if e != nil {
						t.Fatal("conclusive read failure retained owner", e)
					}
					h.Close(context.Background())
					if e = r.ReleaseLifetimeAfterTermination(context.Background(), target, operation); e != nil {
						t.Fatal(e)
					}
					s.beforeList = nil
					s.uploadError = nil
					if result, e := Run(context.Background(), r, files, now, runOptions()); e != nil || !result.Executed {
						t.Fatal("fault cleared without progress", result, e)
					}
				})
			})
		}
	}
}
