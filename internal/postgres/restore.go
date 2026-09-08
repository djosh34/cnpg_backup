// Copyright 2026 cnpg_backup contributors. All rights reserved.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

// RestoreFull materializes an authenticated full or direct full+differential, draining every
// native child before returning. The caller owns the guard Task, persisted Plan,
// source lifetime and fresh process reader BEFORE this call and through replay.
// No PostgreSQL startup or archive lookup occurs here. Returned hashes describe
// final local bundles only; they are NOT evidence of archive availability.
func RestoreFull(ctx context.Context, hold *repository.Hold, plan repository.Plan, native configuration.Native, budgets []configuration.FilesystemBudget, layout RestoreLayout) (result map[string]s3store.Integrity, err error) {
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
	selected := plan.Chain[len(plan.Chain)-1]
	if e := validateRestoreLayout(layout, selected.Tablespaces); e != nil {
		return nil, e
	}
	phaseBudgets, e := restoreDownloadCapacity(plan.Chain[0], native, budgets, layout)
	if len(plan.Chain) == 2 {
		var unit int64
		for _, mount := range restoreMounts(layout) {
			var allocation int64
			allocation, e = restoreAllocationUnit(mount)
			if e != nil {
				break
			}
			unit = max(unit, allocation)
		}
		if e == nil {
			phaseBudgets, e = differentialRestoreBudget(plan.Chain, native, budgets, layout, unit)
		}
	}
	if e != nil {
		return nil, e
	}
	if e = configuration.CheckCapacity(phaseBudgets); e != nil {
		return nil, e
	}
	var transfer int64
	for _, c := range plan.Chain {
		transfer += c.ManifestBytes
		for _, a := range c.Artifacts {
			transfer += a.StoredBytes
		}
	}
	for _, b := range phaseBudgets {
		slog.Info("native restore capacity reserved", "mount", b.Mount, "required_bytes", b.RequiredBytes)
	}
	slog.Info("native restore transfer", "backup_type", selected.Kind, "stored_bytes", transfer)
	// Resolve the actual root before creating scratch; never use source staging or
	// an alias to a memory-backed/unowned subtree.
	if p, e := filepath.EvalSymlinks(NativeWorkspace); e != nil || p != NativeWorkspace {
		return nil, ErrInput
	}
	duration, e := time.ParseDuration(native.CaptureTimeout)
	if e != nil || duration <= 0 || duration > 6*time.Hour {
		return nil, ErrInput
	}
	// Transfer retains the caller's operation deadline (at most 24h), not
	// captureTimeout. Start the separate native-phase deadline after downloads.
	ctx, cancel := context.WithTimeout(ctx, 24*time.Hour)
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
	inputs := make([]*fullInput, len(plan.Chain))
	for i, c := range plan.Chain {
		dir := filepath.Join(scratch, fmt.Sprint(i))
		if e = os.Mkdir(dir, 0700); e != nil {
			return nil, e
		}
		inputs[i], e = downloadFull(ctx, hold, c, dir)
		if e != nil {
			return nil, fmt.Errorf("native download: %w", e)
		}
		if e = inputs[i].scan(ctx, c, plan.Source.WALSegmentBytes, native); e != nil {
			return nil, fmt.Errorf("native input scan: %w", e)
		}
	}
	if len(inputs) == 1 {
		phaseBudgets, e = restoreOutputCapacity(inputs[0], selected, native, budgets, layout)
		if e != nil {
			return nil, e
		}
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
	nativeCtx, nativeCancel := context.WithTimeout(ctx, duration)
	defer nativeCancel()
	run := func(ctx context.Context, tool string, args ...string) ([]byte, error) {
		return runTool(ctx, []string{"LANG=C", "LC_ALL=C", "HOME=/nonexistent"}, tool, args...)
	}
	for _, tool := range []string{"pg_verifybackup", "pg_waldump", "pg_controldata", "pg_combinebackup"} {
		b, e := run(nativeCtx, tool, "--version")
		if e != nil || !nativeVersion(b, tool) {
			return nil, errors.New("native restore tool version mismatch")
		}
	}
	for i, input := range inputs {
		if e = input.verify(nativeCtx, plan.Chain[i], plan.Source.WALSegmentBytes, run); e != nil {
			return nil, fmt.Errorf("native original verification: %w", e)
		}
	}
	if len(inputs) == 2 {
		result, err = combineOriginals(nativeCtx, inputs, plan.Chain, layout, native, run)
	} else {
		result, err = inputs[0].materialize(nativeCtx, selected, layout, run)
	}
	if err != nil {
		return nil, fmt.Errorf("native materialization: %w", err)
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
