package repository

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

// Fail the repository's spool creation, not a dispatched storage operation.
type workspaceFailureStore struct {
	Storage
	afterList func()
	afterPlan func()
}

func (s *workspaceFailureStore) List(c context.Context, p string, max int, visit func(s3store.Info) error) error {
	e := s.Storage.List(c, p, max, visit)
	if e == nil && s.afterList != nil {
		s.afterList()
	}
	return e
}
func (s *workspaceFailureStore) PutFile(c context.Context, k string, f *os.File, in s3store.Integrity, cond s3store.Condition, m map[string]string) (s3store.Info, error) {
	i, e := s.Storage.PutFile(c, k, f, in, cond, m)
	if e == nil && strings.Contains(k, "/gc/") && s.afterPlan != nil {
		s.afterPlan()
	}
	return i, e
}

func TestGCLocalSpoolFailureAllowsCleanClose(t *testing.T) {
	for _, stage := range []string{"plan", "backup-retirement", "wal-retirement"} {
		t.Run(stage, func(t *testing.T) {
			r, s := setup(t)
			h, res := publish(t, r, backupID, "full")
			if e := h.Close(ctx); e != nil {
				t.Fatal(e)
			}
			g, e := r.AcquireGC(ctx)
			if e != nil {
				t.Fatal(e)
			}
			victim := retire(res)
			if stage == "wal-retirement" {
				victim = archivedWAL(t, r, s)
			}
			path := r.workspace
			hidden := path + "-unavailable"
			fail := func() {
				if e := os.Rename(path, hidden); e != nil {
					t.Fatal(e)
				}
			}
			wrapper := &workspaceFailureStore{Storage: s}
			if stage == "plan" {
				wrapper.afterList = fail
			} else {
				wrapper.afterPlan = fail
			}
			r.store = wrapper
			before := s.n
			e = g.Execute(ctx, gcplan(g, victim))
			if re := os.Rename(hidden, path); re != nil {
				t.Fatal(re)
			}
			want := 0
			if stage != "plan" {
				want = 1
			} // acknowledged immutable plan only
			if e == nil || s.n-before != want || s.deletes != 0 {
				t.Fatalf("Execute=%v mutations=%d destructive=%d", e, s.n-before, s.deletes)
			}
			if !s3store.Is(e, s3store.LocalIO) || ambiguous(e) {
				t.Errorf("unsent local I/O was not definitive: %v", e)
			}
			if e = g.Close(ctx); e != nil {
				t.Fatalf("recovered workspace Close: %v", e)
			}
			rh, e := r.AdmitRestore(ctx, targetID, UUID())
			if e != nil {
				t.Fatalf("new admission: %v", e)
			}
			if e = rh.Close(ctx); e != nil {
				t.Fatal(e)
			}
			requireOracle(t, s)
		})
	}
}

// RLIMIT_FSIZE affects only an isolated test subprocess. It produces an actual
// tempfile Write failure without filling the runner filesystem or adding a
// production filesystem/fault-injection abstraction.
func TestGCSpoolWriteFailureAllowsCleanClose(t *testing.T) {
	const child = "CNPG_TEST_SPOOL_WRITE_FAILURE"
	if os.Getenv(child) != "1" {
		exe, e := os.Executable()
		if e != nil {
			t.Fatal(e)
		}
		cmd := exec.Command(exe, "-test.run=^TestGCSpoolWriteFailureAllowsCleanClose$", "-test.v")
		cmd.Env = append(os.Environ(), child+"=1")
		if out, e := cmd.CombinedOutput(); e != nil {
			t.Fatalf("write-failure child: %v\n%s", e, out)
		}
		return
	}
	for _, wal := range []bool{false, true} {
		t.Run(strconv.FormatBool(wal), func(t *testing.T) {
			r, s := setup(t)
			h, res := publish(t, r, backupID, "full")
			if e := h.Close(ctx); e != nil {
				t.Fatal(e)
			}
			v := retire(res)
			if wal {
				v = archivedWAL(t, r, s)
			}
			g, e := r.AcquireGC(ctx)
			if e != nil {
				t.Fatal(e)
			}
			var saved syscall.Rlimit
			if e = syscall.Getrlimit(syscall.RLIMIT_FSIZE, &saved); e != nil {
				t.Fatal(e)
			}
			r.store = &workspaceFailureStore{Storage: s, afterPlan: func() {
				limit := saved
				limit.Cur = 0
				if e := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limit); e != nil {
					t.Fatal(e)
				}
			}}
			before := s.n
			e = g.Execute(ctx, gcplan(g, v))
			if re := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &saved); re != nil {
				t.Fatal(re)
			}
			if !s3store.Is(e, s3store.LocalIO) || ambiguous(e) || s.n != before+1 || s.deletes != 0 {
				t.Errorf("wal=%v write failure: err=%v mutations=%d destructive=%d", wal, e, s.n-before, s.deletes)
			}
			if e = g.Close(ctx); e != nil {
				t.Fatalf("wal=%v Close: %v", wal, e)
			}
			if _, e = r.AdmitRestore(ctx, targetID, UUID()); e != nil {
				t.Fatal(e)
			}
			requireOracle(t, s)
		})
	}
}

func TestBackupLocalSpoolFailureAllowsCleanClose(t *testing.T) {
	r, s := setup(t)
	h, e := r.AdmitBackup(ctx, writerID, backupID)
	if e != nil {
		t.Fatal(e)
	}
	path := r.workspace
	if e = os.Rename(path, path+"-unavailable"); e != nil {
		t.Fatal(e)
	}
	before := s.n
	_, _, e = h.Begin(ctx, request(backupID))
	if re := os.Rename(path+"-unavailable", path); re != nil {
		t.Fatal(re)
	}
	if !s3store.Is(e, s3store.LocalIO) || ambiguous(e) || s.n != before {
		t.Errorf("unsent Begin: err=%v mutations=%d", e, s.n-before)
	}
	if e = h.Close(ctx); e != nil {
		t.Fatalf("Close: %v", e)
	}
	if _, e = r.AdmitBackup(ctx, writerID, backupID); e != nil {
		t.Fatal(e)
	}
}
