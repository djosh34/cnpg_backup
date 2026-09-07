// Copyright 2026 cnpg_backup contributors. All rights reserved.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
)

// RestoreLayout contains manager-authorized, guarded CNPG physical destinations.
// Tablespaces maps native names, not historical source paths, to target /data.
type RestoreLayout struct {
	PGDATA       string
	WALDirectory string
	Tablespaces  map[string]string
}

var restoreSlot sync.Mutex

// RestoreFull materializes one authenticated full, synchronously draining every
// native child before returning. The caller owns the guard Task, persisted Plan,
// source lifetime and fresh process reader BEFORE this call and through replay.
// No PostgreSQL startup or archive lookup occurs here. Returned hashes describe
// final local bundles only; they are NOT evidence of archive availability.
func RestoreFull(ctx context.Context, hold *repository.Hold, plan repository.Plan, native configuration.Native, budgets []configuration.FilesystemBudget, layout RestoreLayout) (result map[string]s3store.Integrity, err error) {
	if len(plan.Chain) != 1 || plan.Chain[0].Kind != "full" {
		return nil, errors.New("native differential restore is not implemented; no full fallback")
	}
	if e := plan.Validate(); e != nil {
		return nil, e
	}
	if hold == nil || hold.ID() != plan.ReaderHoldID || nativeWorkspaceLock.Load() == nil {
		return nil, errors.New("restore requires source reader and exclusive native workspace")
	}
	if !restoreSlot.TryLock() {
		return nil, errors.New("native restore already active")
	}
	defer restoreSlot.Unlock()
	if e := validateRestoreLayout(layout, plan.Chain[0].Tablespaces); e != nil {
		return nil, e
	}
	phaseBudgets, e := restoreDownloadCapacity(plan.Chain[0], native, budgets, layout)
	if e != nil {
		return nil, e
	}
	if e = configuration.CheckCapacity(phaseBudgets); e != nil {
		return nil, e
	}
	// Resolve the actual root before creating scratch; never use source staging or
	// an alias to a memory-backed/unowned subtree.
	if p, e := filepath.EvalSymlinks(NativeWorkspace); e != nil || p != NativeWorkspace {
		return nil, ErrInput
	}
	duration, e := time.ParseDuration(native.CaptureTimeout)
	if e != nil || duration <= 0 || duration > 6*time.Hour {
		return nil, ErrInput
	}
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	done := make(chan struct{})
	monitored := make(chan error, 1)
	go func(done <-chan struct{}, result chan<- error, budgets []configuration.FilesystemBudget) {
		result <- monitorRestoreSpace(ctx, done, cancel, budgets)
	}(done, monitored, phaseBudgets)
	defer func() {
		close(done)
		if e := <-monitored; err == nil && e != nil {
			result, err = nil, e
		}
	}()
	scratch, e := os.MkdirTemp(NativeWorkspace, "restore-")
	if e != nil {
		return nil, e
	}
	defer os.RemoveAll(scratch) // ONLY this call's private scratch, never targets.
	c := plan.Chain[0]
	input, e := downloadFull(ctx, hold, c, scratch)
	if e != nil {
		return nil, fmt.Errorf("native download: %w", e)
	}
	if e = input.scan(ctx, c, plan.Source.WALSegmentBytes, native); e != nil {
		return nil, fmt.Errorf("native input scan: %w", e)
	}
	phaseBudgets, e = restoreOutputCapacity(input, c, native, budgets, layout)
	if e != nil {
		return nil, e
	}
	if e = configuration.CheckCapacity(phaseBudgets); e != nil {
		return nil, e
	}
	close(done)
	monitorErr := <-monitored
	done, monitored = make(chan struct{}), make(chan error, 1)
	go func(done <-chan struct{}, result chan<- error, budgets []configuration.FilesystemBudget) {
		result <- monitorRestoreSpace(ctx, done, cancel, budgets)
	}(done, monitored, phaseBudgets)
	if monitorErr != nil {
		return nil, monitorErr
	}
	run := func(ctx context.Context, tool string, args ...string) ([]byte, error) {
		return runTool(ctx, []string{"LANG=C", "LC_ALL=C", "HOME=/nonexistent"}, tool, args...)
	}
	for _, tool := range []string{"pg_verifybackup", "pg_waldump", "pg_controldata"} {
		b, e := run(ctx, tool, "--version")
		if e != nil || !nativeVersion(b, tool) {
			return nil, errors.New("native restore tool version mismatch")
		}
	}
	if e = input.verify(ctx, c, plan.Source.WALSegmentBytes, run); e != nil {
		return nil, fmt.Errorf("native original verification: %w", e)
	}
	result, err = input.materialize(ctx, c, layout, run)
	if err != nil {
		return nil, fmt.Errorf("native full materialization: %w", err)
	}
	if e = ctx.Err(); e != nil {
		return nil, e
	}
	return result, nil
}

// Polling protects spare margin during bounded Go writes and native reads. Hard
// finite filesystems (checked before any write) remain the allocation stop.
func monitorRestoreSpace(ctx context.Context, done <-chan struct{}, cancel context.CancelFunc, budgets []configuration.FilesystemBudget) error {
	margins := append([]configuration.FilesystemBudget(nil), budgets...)
	for i := range margins {
		margins[i].RequiredBytes = configuration.CapacityMargin(margins[i].RequiredBytes)
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if e := configuration.CheckCapacity(margins); e != nil {
				cancel()
				return e
			}
		}
	}
}
