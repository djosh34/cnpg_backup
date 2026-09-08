package repository

import (
	"context"
	"github.com/djosh34/cnpg_backup/internal/s3store"
	"os"
	"testing"
)

type noPayloadDownloads struct {
	Storage
	t *testing.T
}

func (s noPayloadDownloads) Download(context.Context, string, *os.File, s3store.Integrity) (s3store.Info, error) {
	s.t.Fatal("payload downloaded before selected-input capacity policy")
	return s3store.Info{}, ErrCorrupt
}

func TestRecoveryTargetChoosesEligibleBaseNotNewestShortcut(t *testing.T) {
	r, _ := setup(t)
	publish(t, r, backupID, "older full")
	newerID := UUID()
	producer, e := r.AdmitBackup(ctx, writerID, newerID)
	if e != nil {
		t.Fatal(e)
	}
	attempt, _, e := producer.Begin(ctx, request(newerID))
	if e != nil {
		t.Fatal(e)
	}
	c, m, files := capture(t, attempt, "newer full")
	c.StartedAt = "2026-09-07T02:00:00Z"
	c.StoppedAt = "2026-09-07T02:01:00Z"
	c.StartLSN = "0/3000028"
	c.StopLSN = "0/3000100"
	c.RedoLSN = c.StartLSN
	c.BundledWALStartLSN = c.StartLSN
	c.BundledWALEndLSN = c.StopLSN
	c.WALRanges = []WALRange{{1, c.StartLSN, c.StopLSN}}
	if _, e = attempt.Publish(ctx, c, m, files); e != nil {
		t.Fatal(e)
	}
	fp := digest([]byte("targets"))
	op, _ := RestoreOperationID(targetID, fp)
	h, e := r.AdmitRestore(ctx, targetID, op)
	if e != nil {
		t.Fatal(e)
	}
	catalog, e := h.Catalog(ctx, CatalogLimits{100, 4 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer catalog.Close()
	for _, tc := range []struct {
		name   string
		target Target
		uid    string
	}{
		{"latest", Target{Kind: "latest"}, newerID},
		{"strict LSN inference", Target{Kind: "lsn", Value: "0/2000000"}, backupID},
		{"strict time inference", Target{Kind: "time", Value: "2026-09-07T01:30:00Z"}, backupID},
		{"newest explicit base too new", Target{Kind: "time", Value: "2026-09-07T01:30:00Z", BackupUID: &newerID}, ""},
		{"exact older base", Target{Kind: "latest", BackupUID: ptrID(backupID)}, backupID},
		{"named point exact base", Target{Kind: "name", Value: "before_drop", BackupUID: ptrID(backupID)}, backupID},
		{"XID exact base and exclusive", Target{Kind: "xid", Value: "123", Exclusive: true, BackupUID: ptrID(backupID)}, backupID},
		{"immediate exact base", Target{Kind: "immediate", BackupUID: ptrID(backupID)}, backupID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, e := h.Resolve(ctx, catalog, Plan{DestinationRepositoryID: destID, BootstrapSHA256: fp, Target: tc.target}, nil, func(Commit) error { return nil }, func(context.Context, string) ([]byte, error) { t.Fatal("unexpected history"); return nil, nil })
			if tc.uid == "" {
				if e == nil {
					t.Fatal("explicit ID bypassed eligibility")
				}
				return
			}
			if e != nil || len(plan.Chain) != 1 || plan.Chain[0].BackupUID != tc.uid {
				t.Fatal(plan.Chain, e)
			}
		})
	}
	// Caller-native policy runs before any payload verification, without silently
	// dropping an oversized/unsupported newest selected base for an older one.
	r.store = noPayloadDownloads{r.store, t}
	_, e = h.Resolve(ctx, catalog, Plan{DestinationRepositoryID: destID, BootstrapSHA256: fp, Target: Target{Kind: "latest"}}, nil, func(c Commit) error {
		if c.BackupUID != newerID {
			t.Fatal("policy saw wrong base")
		}
		return ErrCapacity
	}, func(context.Context, string) ([]byte, error) { return nil, nil })
	if e != ErrCapacity {
		t.Fatal("input policy was ignored", e)
	}
}
