package wal

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/s3store"
	"golang.org/x/sys/unix"
)

// A subprocess stops at real module I/O boundaries with open, allocated spools.
// SIGKILL (not a returned fake error) prevents all operation defers from running.
type crashStore struct {
	Storage
	stage   string
	barrier func()
}

func (s crashStore) PutFile(ctx context.Context, key string, f *os.File, want s3store.Integrity, c s3store.Condition, m map[string]string) (s3store.Info, error) {
	if s.stage == "upload" {
		s.barrier()
	}
	return s.Storage.PutFile(ctx, key, f, want, c, m)
}
func (s crashStore) Head(ctx context.Context, key string) (s3store.Info, error) {
	if s.stage == "verify" {
		s.barrier()
	}
	return s.Storage.Head(ctx, key)
}
func (s crashStore) DownloadWAL(ctx context.Context, key string, f *os.File, want s3store.Integrity) (s3store.Info, error) {
	info, err := s.Storage.DownloadWAL(ctx, key, f, want)
	if err == nil && (s.stage == "download" || s.stage == "restore") {
		s.barrier()
	}
	return info, err
}

func TestWALCrashChild(t *testing.T) {
	stage := os.Getenv("WAL_CRASH_STAGE")
	if stage == "" {
		return
	}
	w, _ := simulatedWAL(t)
	w.Compression = "none"
	w.Workspace = os.Getenv("WAL_CRASH_WORK")
	name := os.Getenv("WAL_CRASH_NAME")
	src := source(t, bytes.Repeat([]byte{37}, 1<<20))
	if stage == "restore" {
		if err := w.Archive(context.Background(), name, src); err != nil {
			t.Fatal(err)
		}
	}
	w.Store = crashStore{w.Store, stage, func() {
		fmt.Println("WAL-CRASH-READY")
		for {
			time.Sleep(time.Hour)
		}
	}}
	if stage == "restore" {
		root, err := os.OpenRoot(os.Getenv("WAL_CRASH_TARGET"))
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()
		err = w.Restore(context.Background(), name, root, "RECOVERYXLOG")
		t.Fatalf("unexpected restore return: %v", err)
	}
	t.Fatalf("unexpected archive return: %v", w.Archive(context.Background(), name, src))
}

func scratchUsage(t *testing.T, dir, prefix string) (int, int64) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	var allocated int64
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		var st unix.Stat_t
		if err := unix.Lstat(filepath.Join(dir, entry.Name()), &st); err != nil {
			t.Fatal(err)
		}
		count++
		allocated += st.Blocks * 512
	}
	return count, allocated
}

func TestRepeatedProcessDeathsDoNotAccumulateWALScratch(t *testing.T) {
	for _, name := range []string{"000000010000000000000001", "000000010000000000000001.partial"} {
		t.Run(name, func(t *testing.T) { testWALProcessDeaths(t, name) })
	}
}

func testWALProcessDeaths(t *testing.T, name string) {
	for _, stage := range []string{"upload", "verify", "download", "restore"} {
		t.Run(stage, func(t *testing.T) {
			work, target := t.TempDir(), t.TempDir()
			sentinel := filepath.Join(target, "RECOVERYXLOG")
			if err := os.WriteFile(sentinel, []byte("previous verified file"), 0600); err != nil {
				t.Fatal(err)
			}
			for death := 0; death < 3; death++ {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWALCrashChild$")
				cmd.Env = append(os.Environ(), "WAL_CRASH_STAGE="+stage, "WAL_CRASH_NAME="+name, "WAL_CRASH_WORK="+work, "WAL_CRASH_TARGET="+target, "TMPDIR="+t.TempDir())
				stdout, err := cmd.StdoutPipe()
				if err != nil {
					cancel()
					t.Fatal(err)
				}
				var diagnostics bytes.Buffer
				cmd.Stderr = &diagnostics
				if err := cmd.Start(); err != nil {
					cancel()
					t.Fatal(err)
				}
				scanner := bufio.NewScanner(stdout)
				ready := false
				for scanner.Scan() {
					if scanner.Text() == "WAL-CRASH-READY" {
						ready = true
						break
					}
				}
				cmd.Process.Kill()
				err = cmd.Wait()
				cancel()
				if !ready || err == nil {
					t.Fatalf("crash barrier not observed: %v %s", err, &diagnostics)
				}
				count, allocated := scratchUsage(t, work, "wal-")
				t.Logf("death=%d workspace files=%d allocated=%d", death+1, count, allocated)
				if count != 0 || allocated != 0 {
					t.Errorf("crash-persistent workspace scratch: %d files, %d bytes", count, allocated)
				}
				count, allocated = scratchUsage(t, target, ".cnpg-wal-")
				if count > 1 || allocated > 1<<20 {
					t.Errorf("unbounded restore scratch: %d files, %d bytes", count, allocated)
				}
				got, err := os.ReadFile(sentinel)
				if err != nil || string(got) != "previous verified file" {
					t.Fatal("death changed published destination", err)
				}
			}
			// A fresh process/module operation progresses and removes its bounded target
			// scratch. Remote admission/ambiguity is deliberately not a cleanup oracle.
			w, _ := simulatedWAL(t)
			w.Workspace = work
			raw := bytes.Repeat([]byte{37}, 1<<20)
			if err := w.Archive(context.Background(), name, source(t, raw)); err != nil {
				t.Fatal(err)
			}
			root, err := os.OpenRoot(target)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			if err := w.Restore(context.Background(), name, root, "RECOVERYXLOG"); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(sentinel)
			if err != nil || !bytes.Equal(got, raw) {
				t.Fatal("retry byte oracle", err)
			}
			if n, _ := scratchUsage(t, target, ".cnpg-wal-"); n != 0 {
				t.Error("successful retry retained abandoned target scratch", n)
			}
		})
	}
}

func TestRestoreScratchOwnership(t *testing.T) {
	w, _ := simulatedWAL(t)
	name := "00000002.history"
	raw := []byte("1\t0/100000\tfixture\n")
	if err := w.Archive(context.Background(), name, source(t, raw)); err != nil {
		t.Fatal(err)
	}
	root, dir := local(t)
	// Never sweep old random-prefix files or another owner's unrelated data.
	foreign := filepath.Join(dir, ".cnpg-wal-foreign")
	if err := os.WriteFile(foreign, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	first := w
	first.Store = crashStore{w.Store, "restore", func() { close(entered); <-release }}
	done := make(chan error, 1)
	go func() { done <- first.Restore(context.Background(), name, root, "RECOVERYXLOG") }()
	<-entered
	secondErr := w.Restore(context.Background(), name, root, name)
	close(release)
	firstErr := <-done
	if secondErr != ErrLocal {
		t.Errorf("concurrent writer was not fenced: %v", secondErr)
	}
	if firstErr != nil {
		t.Fatal("live writer damaged", firstErr)
	}
	got, err := os.ReadFile(filepath.Join(dir, "RECOVERYXLOG"))
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatal("live writer byte oracle", err)
	}
	got, err = os.ReadFile(foreign)
	if err != nil || string(got) != "foreign" {
		t.Fatal("foreign scratch deleted", err)
	}
}
