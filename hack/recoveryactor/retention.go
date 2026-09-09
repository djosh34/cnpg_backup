package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/retention"
	"github.com/djosh34/cnpg_backup/internal/wal"
)

// Test-only explicit planner clock, NOT a lease/fencing clock. Real immutable
// native backups, product Run, admission, storage and tombstones are unchanged.
// This exercises aging without editing published metadata or sleeping an hour.
func retentionBatch() {
	if len(os.Args) != 4 || os.Getenv("POD_UID") == "" {
		panic("invalid disposable retention actor")
	}
	cutoff, e := time.Parse(time.RFC3339Nano, os.Args[2])
	must(e)
	if os.Args[3] != "dry-run" && os.Args[3] != "execute" {
		panic("invalid retention actor mode")
	}
	snap, e := configuration.LoadSnapshot("/cnpg-backup/projection", "destination")
	must(e)
	store, e := snap.Store()
	must(e)
	defer store.Close()
	dir, e := os.MkdirTemp("/cnpg-backup/work", "retention-actor-")
	must(e)
	defer os.RemoveAll(dir)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	r, e := repository.OpenSource(ctx, store, snap.Spec.RepositoryID, dir)
	must(e)
	result, e := retention.Run(ctx, r, wal.Files{Repository: r, Store: store, Workspace: dir, Compression: snap.Spec.Compression}, cutoff.Add(time.Hour), retention.Options{Enabled: true, DryRun: os.Args[3] == "dry-run", Window: time.Hour, MinimumFulls: 1})
	failure := ""
	if e != nil {
		switch {
		case errors.Is(e, repository.ErrBlocked):
			failure = "RepositoryAdmissionBlocked"
		case errors.Is(e, repository.ErrUncertain):
			failure = "UncertainOwner"
		case errors.Is(e, retention.ErrCoverage):
			failure = "RecoveryCoverage"
		default:
			failure = "RetentionFailed"
		}
	}
	must(json.NewEncoder(os.Stdout).Encode(map[string]any{"result": result, "error": failure, "policy_cutoff": cutoff, "clock": "explicit test planner clock; never admission fencing"}))
}
