package recoveryguard

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"golang.org/x/sys/unix"
)

func testConfig(t *testing.T) (Config, Owner) {
	t.Helper()
	c := Config{ClusterUID: NewUUID(), OperationUID: NewUUID()}
	for range 3 {
		c.Targets = append(c.Targets, Target{NewUUID(), t.TempDir()})
	}
	return c, Owner{c.ClusterUID, c.OperationUID, NewUUID(), NewUUID()}
}
func TestLockMarkerLifetime(t *testing.T) {
	c, owner := testConfig(t)
	for _, target := range c.Targets {
		if err := os.WriteFile(filepath.Join(target.Mount, "sentinel"), []byte("unchanged"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	h, err := acquire(c, owner)
	if err != nil {
		t.Fatal(err)
	}
	defer h.close()
	for _, v := range h.volumes {
		flags, err := unix.FcntlInt(v.lock.Fd(), unix.F_GETFD, 0)
		if err != nil || flags&unix.FD_CLOEXEC == 0 {
			t.Fatal("lock inherited by CNPG", err)
		}
	}
	if err := verifyOwner(c, owner); err != nil {
		t.Fatal(err)
	}
	foreign := owner
	foreign.GuardUID = NewUUID()
	if err := verifyOwner(c, foreign); err == nil {
		t.Fatal("accepted foreign owner")
	}
	reversed := c
	reversed.Targets = append([]Target(nil), c.Targets...)
	reversed.Targets[0], reversed.Targets[2] = reversed.Targets[2], reversed.Targets[0]
	if _, err := acquire(reversed, foreign); !errors.Is(err, ErrBusy) {
		t.Fatalf("overlap: %v", err)
	}
	for _, target := range c.Targets {
		b, err := os.ReadFile(filepath.Join(target.Mount, "sentinel"))
		if err != nil || string(b) != "unchanged" {
			t.Fatal("target mutated")
		}
	}
	if err := h.release(); err != nil {
		t.Fatal(err)
	}
	h.close()
	next, err := acquire(c, foreign)
	if err != nil {
		t.Fatal(err)
	}
	next.close() // kernel lock release, deliberately NO marker cleanup (crash).
	if err := verifyOwner(c, foreign); err == nil {
		t.Fatal("marker without live lock admitted")
	}
	if _, err = acquire(c, foreign); !errors.Is(err, ErrUncertain) {
		t.Fatalf("poison takeover: %v", err)
	}
	for _, target := range c.Targets {
		if _, err := os.Stat(filepath.Join(target.Mount, ".cnpg-backup", "owner.json")); err != nil {
			t.Fatal("marker erased", err)
		}
	}
}
func TestMarkerMismatchAndPartialSetFailClosed(t *testing.T) {
	c, owner := testConfig(t)
	h, err := acquire(c, owner)
	if err != nil {
		t.Fatal(err)
	}
	defer h.close()
	path := filepath.Join(c.Targets[0].Mount, ".cnpg-backup", "owner.json")
	if err := os.WriteFile(path, []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := h.release(); !errors.Is(err, ErrUncertain) {
		t.Fatalf("release: %v", err)
	}
	for _, target := range c.Targets {
		if _, err := os.Stat(filepath.Join(target.Mount, ".cnpg-backup", "owner.json")); err != nil {
			t.Fatal("partial release", err)
		}
	}
	h.close()
	// A marker in only one member still poisons the entire immutable set.
	for _, target := range c.Targets[1:] {
		if err := os.Remove(filepath.Join(target.Mount, ".cnpg-backup", "owner.json")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := acquire(c, owner); !errors.Is(err, ErrUncertain) {
		t.Fatal(err)
	}
}
func TestFenceRejectsLinksAndSpecialFiles(t *testing.T) {
	for _, kind := range []string{"directory-link", "lock-link", "lock-hardlink", "lock-fifo", "owner-link"} {
		t.Run(kind, func(t *testing.T) {
			c, owner := testConfig(t)
			target := c.sortedTargets()[0]
			dir := filepath.Join(target.Mount, ".cnpg-backup")
			outside := t.TempDir()
			file := filepath.Join(outside, "unchanged")
			if err := os.WriteFile(file, []byte("safe"), 0600); err != nil {
				t.Fatal(err)
			}
			if kind == "directory-link" {
				if err := os.Symlink(outside, dir); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
				var err error
				switch kind {
				case "lock-link":
					err = os.Symlink(file, filepath.Join(dir, "lock"))
				case "lock-hardlink":
					err = os.Link(file, filepath.Join(dir, "lock"))
				case "lock-fifo":
					err = unix.Mkfifo(filepath.Join(dir, "lock"), 0600)
				case "owner-link":
					err = os.Symlink(file, filepath.Join(dir, "owner.json"))
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if h, err := acquire(c, owner); err == nil {
				h.close()
				t.Fatal("accepted unsafe path")
			}
			b, err := os.ReadFile(file)
			if err != nil || string(b) != "safe" {
				t.Fatal("escaped target root")
			}
		})
	}
}
func TestCanonicalConfigAndArgv(t *testing.T) {
	c := Config{ClusterUID: NewUUID(), OperationUID: NewUUID(), Targets: []Target{{NewUUID(), "/var/lib/postgresql/data"}, {NewUUID(), "/var/lib/postgresql/wal"}, {NewUUID(), "/var/lib/postgresql/tablespaces/fast_space"}}}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	original := []string{"/controller/manager", "instance", "restore", "--pg-wal", "/var/lib/postgresql/wal/pg_wal"}
	got, err := WrapArgv(original[:3], original[3:], true)
	want := append([]string{HelperPath, "recovery-guard", "--"}, original...)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("%q: %v", got, err)
	}
	for _, argv := range [][]string{{"/bin/sh", "-c", "restore"}, {"/controller/manager", "instance", "run"}, append(append([]string(nil), original...), "--unsafe"), original[:3]} {
		if c.ValidateArgv(argv) == nil {
			t.Fatalf("accepted %q", argv)
		}
	}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Targets[1].PVCUID = c.Targets[0].PVCUID },
		func(c *Config) { c.Targets[1].Mount = c.Targets[0].Mount },
		func(c *Config) { c.Targets[2].Mount += "/../escape" },
		func(c *Config) { c.ClusterUID = "cluster-name" },
		func(c *Config) { c.Targets = c.Targets[1:] },
	} {
		broken := c
		broken.Targets = append([]Target(nil), c.Targets...)
		mutate(&broken)
		if broken.Validate() == nil {
			t.Fatal("accepted invalid config")
		}
	}
	if _, err := Run(context.Background(), c, NewUUID(), original); err == nil {
		t.Fatal("non-PID1 guard admitted")
	}
}
func TestInstallHelperAtomic(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	destination := filepath.Join(dir, "helper")
	content := bytes.Repeat([]byte("static executable fixture"), 4096)
	if err := os.WriteFile(source, content, 0600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := InstallHelper(source, destination); err != nil {
			t.Fatal(err)
		}
	}
	got, err := os.ReadFile(destination)
	if err != nil || !bytes.Equal(got, content) {
		t.Fatal("wrong helper bytes", err)
	}
	stat, err := os.Stat(destination)
	if err != nil || stat.Mode().Perm() != 0555 {
		t.Fatal("helper permissions", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 2 {
		t.Fatal("temporary helper leaked", err)
	}
}
func FuzzLocalControlDocument(f *testing.F) {
	f.Add([]byte(`{"kind":"drain","tuple":{}}`))
	f.Add([]byte(`{} {}`))
	f.Fuzz(func(t *testing.T, b []byte) { var msg controlMessage; _ = Decode(b, &msg) })
}
