// Copyright 2026 cnpg_backup contributors. All rights reserved.
package postgres

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

var subreaper struct {
	sync.Once
	err error
}

// runTool is the sole native execution seam. No shell, inherited libpq settings,
// unbounded diagnostics, or surviving WAL-streaming child is permitted.
func runTool(ctx context.Context, env []string, tool string, args ...string) ([]byte, error) {
	switch tool {
	case "psql", "pg_controldata", "pg_basebackup", "pg_verifybackup", "pg_waldump", "pg_combinebackup":
	default:
		return nil, errors.New("unsupported native executable")
	}
	return runNative(ctx, env, tools+tool, args...)
}

// runNative separates process ownership from the fixed runtime executable
// allowlist so tests can exercise real fork/cancel/reap with a test executable.
func runNative(ctx context.Context, env []string, executable string, args ...string) ([]byte, error) {
	subreaper.Do(func() { subreaper.err = unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) })
	if subreaper.err != nil {
		return nil, errors.New("native child ownership unavailable")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, args...)
	cmd.Env = env
	// ExtraFiles clears CLOEXEC in the child. Approved PostgreSQL tools retain
	// this descriptor across their native forks, even outside our process group.
	if lock := nativeWorkspaceLock.Load(); lock != nil {
		cmd.ExtraFiles = []*os.File{lock}
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 2 * time.Second
	out, diagnostic := commandOutput{cancel: cancel}, commandOutput{cancel: cancel}
	cmd.Stdout, cmd.Stderr = &out, &diagnostic
	if err := cmd.Start(); err != nil {
		return nil, errors.New("native executable could not start")
	}
	err := cmd.Wait()
	// The parent is waited by os/exec. Subreaper ownership lets this same caller
	// reap orphaned native grandchildren in this isolated process group only.
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	for {
		_, e := syscall.Wait4(-cmd.Process.Pid, nil, 0, nil)
		if e == syscall.EINTR {
			continue
		}
		if e == syscall.ECHILD {
			break
		}
		if e != nil {
			return nil, errors.New("native child drain uncertain")
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil || out.exceeded || diagnostic.exceeded || len(diagnostic.data) != 0 {
		return nil, errors.New("native command failed (authentication, input, capacity or output limit)")
	}
	return out.data, nil
}

type commandOutput struct {
	data     []byte
	exceeded bool
	cancel   context.CancelFunc
}

func (b *commandOutput) Write(p []byte) (int, error) {
	n := len(p)
	remaining := (1 << 20) - len(b.data)
	if n > remaining {
		b.exceeded = true
		b.cancel()
		p = p[:remaining]
	}
	b.data = append(b.data, p...)
	return n, nil
}
