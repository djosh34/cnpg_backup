package repository

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

func archivedWAL(t *testing.T, r *Repository, s *fakeStore) Victim {
	t.Helper()
	name := "000000010000000000000001"
	key, e := r.WALKey(name)
	if e != nil {
		t.Fatal(e)
	}
	b := []byte("synthetic archive extending past backup EndLSN")
	i, e := s.mutate(key, b, s3store.Condition{Create: true}, false)
	if e != nil {
		t.Fatal(e)
	}
	o := s.objects[key]
	o.info.Metadata = map[string]string{"cnpg-format": "wal-v1", "cnpg-system-id": r.id.SystemIdentifier, "cnpg-raw-sha256": digest(b), "cnpg-raw-bytes": strconv.FormatInt(r.id.WALSegmentBytes, 10)}
	s.objects[key] = o
	return Victim{Kind: "retire-wal", WALName: &name, ExpectedETag: &i.ETag, SHA256: digest(b), RawBytes: r.id.WALSegmentBytes}
}

func TestGCBackupRetirementPrecedesWAL(t *testing.T) {
	for _, order := range []string{"wal-first", "backup-first-rejected", "backup-first"} {
		t.Run(order, func(t *testing.T) {
			r, s := setup(t)
			h, old := publish(t, r, backupID, "old full")
			if e := h.Close(ctx); e != nil {
				t.Fatal(e)
			}
			// Keep a newer full. The victim set is eligible with minimumFulls=1;
			// only ordering and interruption of execution are under test.
			uid := "77777777-7777-4777-8777-777777777777"
			h, e := r.AdmitBackup(ctx, writerID, uid)
			if e != nil {
				t.Fatal(e)
			}
			a, _, e := h.Begin(ctx, request(uid))
			if e != nil {
				t.Fatal(e)
			}
			c, m, fs := capture(t, a, "new full")
			c.StartedAt, c.StoppedAt = "2026-09-07T03:00:00Z", "2026-09-07T03:01:00Z"
			c.StartLSN, c.StopLSN = "0/3000028", "0/3000100"
			c.RedoLSN, c.BundledWALStartLSN, c.BundledWALEndLSN = c.StartLSN, c.StartLSN, c.StopLSN
			c.WALRanges = []WALRange{{1, c.StartLSN, c.StopLSN}}
			if _, e = a.Publish(ctx, c, m, fs); e != nil {
				t.Fatal(e)
			}
			if e = h.Close(ctx); e != nil {
				t.Fatal(e)
			}
			w := archivedWAL(t, r, s)
			key, _ := r.WALKey(*w.WALName)
			original := string(s.objects[key].b)
			g, e := r.AcquireGC(ctx)
			if e != nil {
				t.Fatal(e)
			}
			p := gcplan(g, retire(old), w)
			p.Policy.MinimumFulls = 1
			p.Cutoff = "2026-09-08T00:00:00Z"
			before := s.n
			switch order {
			case "wal-first":
				p.Victims = []Victim{w, retire(old)}
				// Before the fix this rejects backup retirement only AFTER WAL
				// has been destroyed; a definitive error then permits release.
				s.failAt, s.fault = before+3, "reject"
			case "backup-first-rejected":
				s.failAt, s.fault = before+2, "reject"
			}
			e = g.Execute(ctx, p)
			switch order {
			case "wal-first":
				if e != ErrInvalid || s.n != before {
					t.Errorf("invalid order: err=%v mutations=%d", e, s.n-before)
				}
			case "backup-first-rejected":
				if !s3store.Is(e, s3store.Auth) || s.n != before+2 {
					t.Errorf("interruption: err=%v mutations=%d", e, s.n-before)
				}
			case "backup-first":
				if e != nil {
					t.Fatal(e)
				}
			}
			// Do not let an unconsumed fault affect gate release/admission.
			s.failAt = 0
			if e = g.Close(ctx); e != nil {
				t.Fatal(e)
			}
			rh, e := r.AdmitRestore(ctx, targetID, UUID())
			if e != nil {
				t.Fatal(e)
			}
			cat, e := rh.Catalog(ctx, CatalogLimits{10, 32 << 20})
			if e != nil {
				t.Fatal(e)
			}
			defer cat.Close()
			if e = cat.Visit(ctx, func(en Entry) error {
				if en.Commit.BackupUID == old.Commit.BackupUID && en.Retired != (order == "backup-first") {
					t.Error("wrong backup exclusion")
				}
				return nil
			}); e != nil {
				t.Fatal(e)
			}
			if order != "backup-first" && string(s.objects[key].b) != original {
				t.Error("live backup lost remote coverage")
			}
			if order == "backup-first" {
				var tomb struct{ State string }
				if e = json.Unmarshal(s.objects[key].b, &tomb); e != nil || tomb.State != "retired" {
					t.Fatal("WAL not retired", e)
				}
			}
			requireOracle(t, s)
		})
	}
}

func TestOracleDetectsWALBeforeBackupRetirement(t *testing.T) {
	r, s := setup(t)
	h, old := publish(t, r, backupID, "full")
	if e := h.Close(ctx); e != nil {
		t.Fatal(e)
	}
	w := archivedWAL(t, r, s)
	g, e := r.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	// Negative control: bypass Execute and manufacture the exact unsafe
	// intermediate state. Oracle must not call production order validation.
	p := gcplan(g, w, retire(old))
	b, _ := json.Marshal(p)
	if _, e = r.put(ctx, r.root+"gc/"+p.OperationID+".json", b, s3store.Condition{Create: true}); e != nil {
		t.Fatal(e)
	}
	key, _ := r.WALKey(*w.WALName)
	b, _ = json.Marshal(WALRetirement{1, "retired", repoID, *w.WALName, w.RawBytes, w.SHA256, p.OperationID})
	if _, e = s.mutate(key, b, s3store.Condition{Match: *w.ExpectedETag}, true); e != nil {
		t.Fatal(e)
	}
	if independentOracle(s.objects) == nil || len(s.oracleErrors) == 0 {
		t.Fatal("oracle accepted WAL retirement while planned backup remained live")
	}
}
