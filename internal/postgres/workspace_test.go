// Copyright 2026 cnpg_backup contributors. All rights reserved.
package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestWorkspaceProcessHelper(t *testing.T) {
	mode := os.Getenv("WORKSPACE_TEST_MODE")
	if mode == "" {
		return
	}
	dir := os.Getenv("WORKSPACE_TEST_DIR")
	switch mode {
	case "owner":
		f, err := acquireWorkspaceAt(dir, "pod-one")
		if err != nil {
			t.Fatal(err)
		}
		nativeWorkspaceLock.Store(f)
		_, err = runNative(context.Background(), []string{"WORKSPACE_TEST_MODE=native", "WORKSPACE_TEST_DIR=" + dir}, os.Args[0], "-test.run=^TestWorkspaceProcessHelper$")
		t.Fatal("unexpected native return", err)
	case "native":
		// This child gets fd3 from the actual managed command path. The deliberately
		// detached grandchild retains it across exec, like an approved native fork.
		f := os.NewFile(3, "inherited-workspace-lock")
		if _, err := f.Stat(); err != nil {
			t.Fatal(err)
		}
		child := exec.Command(os.Args[0], "-test.run=^TestWorkspaceProcessHelper$")
		child.Env = []string{"WORKSPACE_TEST_MODE=detached", "WORKSPACE_TEST_DIR=" + dir}
		child.ExtraFiles = []*os.File{f}
		child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := child.Start(); err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal([]int{os.Getpid(), child.Process.Pid})
		if err := os.WriteFile(filepath.Join(dir, "pids"), b, 0600); err != nil {
			t.Fatal(err)
		}
		for {
			time.Sleep(time.Second)
			runtime.KeepAlive(f)
		}
	case "detached":
		f := os.NewFile(3, "inherited-workspace-lock")
		if _, err := f.Stat(); err != nil {
			t.Fatal(err)
		}
		root := filepath.Join(dir, "native", "capture-test")
		if err := os.Mkdir(root, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "partial.tar"), bytes.Repeat([]byte{1}, 8<<20), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(dir, "native", "backup-repository-test"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "ready"), []byte("ready"), 0600); err != nil {
			t.Fatal(err)
		}
		for {
			time.Sleep(time.Second)
			runtime.KeepAlive(f)
		}
	}
}

func TestWorkspaceDeathReclaimAndDetachedExclusion(t *testing.T) {
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	// These stand for unrelated data, legacy scratch and target markers. None
	// belongs to the new marked native subtree; reclaim must never touch them.
	for _, name := range []string{"unrelated", "capture-legacy", ".cnpg-backup"} {
		if err := os.Mkdir(filepath.Join(dir, name), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name, "keep"), []byte("unchanged"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	waitFile := func(name string) {
		t.Helper()
		for i := 0; i < 500; i++ {
			if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("helper did not reach", name)
	}
	for attempt := 0; attempt < 3; attempt++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestWorkspaceProcessHelper$")
		cmd.Env = []string{"WORKSPACE_TEST_MODE=owner", "WORKSPACE_TEST_DIR=" + dir}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill() })
		waitFile("ready")
		waitFile("pids")
		b, err := os.ReadFile(filepath.Join(dir, "pids"))
		if err != nil {
			t.Fatal(err)
		}
		var pids []int
		if json.Unmarshal(b, &pids) != nil || len(pids) != 2 {
			t.Fatal("invalid helper pids")
		}
		alive := []bool{true, true}
		for i, pid := range pids {
			t.Cleanup(func() {
				if alive[i] {
					_ = unix.Kill(pid, unix.SIGKILL)
					_, _ = unix.Wait4(pid, nil, 0, nil)
				}
			})
		}
		var st unix.Stat_t
		partial := filepath.Join(dir, "native", "capture-test", "partial.tar")
		if unix.Stat(partial, &st) != nil || st.Blocks*512 < 8<<20 {
			t.Fatal("scratch was not actually allocated")
		}
		if f, err := acquireWorkspaceAt(dir, "pod-one"); err == nil {
			f.Close()
			t.Fatal("live owner replaced")
		}
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		// Kill/reap the entire managed group; the setsid grandchild is still alive.
		if err := unix.Kill(-pids[0], unix.SIGKILL); err != nil {
			t.Fatal(err)
		}
		if _, err := unix.Wait4(pids[0], nil, 0, nil); err != nil {
			t.Fatal(err)
		}
		alive[0] = false
		if f, err := acquireWorkspaceAt(dir, "pod-one"); err == nil {
			f.Close()
			t.Fatal("process-group death mistaken for complete lifetime")
		}
		if _, err := os.Stat(partial); err != nil {
			t.Fatal("live detached writer's scratch removed")
		}
		if err := unix.Kill(pids[1], unix.SIGKILL); err != nil {
			t.Fatal(err)
		}
		if _, err := unix.Wait4(pids[1], nil, 0, nil); err != nil {
			t.Fatal(err)
		}
		alive[1] = false
		f, err := acquireWorkspaceAt(dir, "pod-one")
		if err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(filepath.Join(dir, "native"))
		if err != nil || len(entries) != 0 {
			t.Fatal("dead scratch not reclaimed", entries, err)
		}
		if g, err := acquireWorkspaceAt(dir, "pod-one"); err == nil {
			g.Close()
			t.Fatal("replacement active owner not excluded")
		}
		f.Close()
		for _, name := range []string{"unrelated", "capture-legacy", ".cnpg-backup"} {
			b, err := os.ReadFile(filepath.Join(dir, name, "keep"))
			if err != nil || string(b) != "unchanged" {
				t.Fatal("unrelated data changed", name)
			}
		}
		os.Remove(filepath.Join(dir, "ready"))
		os.Remove(filepath.Join(dir, "pids"))
		t.Log("reclaimed actual 8MiB after complete native lifetime, attempt", strconv.Itoa(attempt+1))
	}
	if f, err := acquireWorkspaceAt(dir, "different-pod"); err == nil {
		f.Close()
		t.Fatal("workspace transferred across Pods")
	}
}

func TestWorkspaceRefusesUnmarkedAndSymlinkRoots(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "native"), 0700); err != nil {
		t.Fatal(err)
	}
	if f, err := acquireWorkspaceAt(dir, "pod-one"); err == nil {
		f.Close()
		t.Fatal("unmarked data adopted")
	}
	os.Remove(filepath.Join(dir, "native"))
	f, err := acquireWorkspaceAt(dir, "pod-one")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	os.Remove(filepath.Join(dir, "native"))
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "keep"), []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "native")); err != nil {
		t.Fatal(err)
	}
	if f, err := acquireWorkspaceAt(dir, "pod-one"); err == nil {
		f.Close()
		t.Fatal("symlink adopted")
	}
	if b, err := os.ReadFile(filepath.Join(outside, "keep")); err != nil || string(b) != "keep" {
		t.Fatal("escaped managed root")
	}
}
