package repository

import (
	"context"
	"errors"
	"testing"
)

func TestDifferentialFrozenRootAndNoFallback(t *testing.T) {
	r, s := setup(t)
	_, full := publish(t, r, backupID, "full bytes")
	uid := "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
	req := request(uid)
	req.RequestedKind = "differential"
	h, e := r.AdmitBackup(ctx, writerID, uid)
	if e != nil {
		t.Fatal(e)
	}
	eligible := func(c Commit) error {
		if c.Kind != "full" {
			t.Fatal("D offered as parent")
		}
		return nil
	}
	a, w, root, e := h.BeginDifferential(ctx, req, eligible)
	if e != nil || w != nil || root.BackupUID != backupID {
		t.Fatal(a, w, root, e)
	}
	// A newly published full must never change this request's frozen root.
	_, _ = publish(t, r, "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee", "new full")
	a, _, root, e = h.BeginDifferential(ctx, req, func(Commit) error { t.Fatal("retry reselected root"); return nil })
	if e != nil || root.BackupUID != backupID {
		t.Fatal(root, e)
	}
	c, m, files := capture(t, a, "native differential bytes")
	c.Kind = "differential"
	c.ParentBackupUID = &root.BackupUID
	c.RootBackupUID = root.BackupUID
	c.RootManifestSHA256 = &root.ManifestSHA256
	c.StartedAt = "2026-09-07T02:00:00Z"
	c.StoppedAt = "2026-09-07T02:01:00Z"
	c.StartLSN = "0/2000028"
	c.StopLSN = "0/2000100"
	c.RedoLSN = c.StartLSN
	c.BundledWALStartLSN = c.StartLSN
	c.BundledWALEndLSN = c.StopLSN
	c.WALRanges = []WALRange{{1, c.StartLSN, c.StopLSN}}
	// Failure after native upload verification cannot publish or become full.
	_, e = a.PublishChecked(ctx, c, m, files, func(context.Context) error { return errors.New("promotion") })
	if e == nil {
		t.Fatal("postflight failure published")
	}
	if _, ok := s.objects[r.backup(uid)+"commit.json"]; ok {
		t.Fatal("failed D committed")
	}
	requireOracle(t, s)
	// Retry across process loss still uses F; deletion of F fails closed, not F2.
	delete(s.objects, r.backup(backupID)+"commit.json")
	_, _, _, e = h.BeginDifferential(ctx, req, eligible)
	if e == nil {
		t.Fatal("missing frozen parent accepted")
	}
	if full.Commit.Kind != "full" {
		t.Fatal("fixture")
	}
}

func TestDifferentialMissingOrIneligibleFullDoesNotBegin(t *testing.T) {
	for _, exists := range []bool{false, true} {
		r, s := setup(t)
		if exists {
			publish(t, r, backupID, "full")
		}
		uid := "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
		h, e := r.AdmitBackup(ctx, writerID, uid)
		if e != nil {
			t.Fatal(e)
		}
		req := request(uid)
		req.RequestedKind = "differential"
		_, _, _, e = h.BeginDifferential(ctx, req, func(Commit) error { return errors.New("checksum/summary/restart") })
		if e == nil {
			t.Fatal("invalid prerequisites accepted")
		}
		for key := range s.objects {
			if len(key) >= len(r.backup(uid)) && key[:len(r.backup(uid))] == r.backup(uid) {
				t.Fatal("failed prerequisites started backup namespace", key)
			}
		}
	}
}
