package repository

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

func TestRetentionCleanupResumesOnlyInNewReleasedBatch(t *testing.T) {
	r, s := setup(t)
	h, res := publish(t, r, backupID, "retired payload")
	if e := h.Close(ctx); e != nil {
		t.Fatal(e)
	}
	g, e := r.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	before, e := g.Cleanup(ctx, 128)
	if e != nil || len(before) != 0 {
		t.Fatal("live winner eligible", before, e)
	}
	uid := backupID
	if e = g.Execute(ctx, gcplan(g, Victim{Kind: "retire-backup", BackupUID: &uid, SHA256: digest(res.Bytes)})); e != nil {
		t.Fatal(e)
	}
	if _, e = r.AdmitRestore(ctx, targetID, UUID()); e != ErrBlocked {
		t.Fatal("owner did not exclude restore", e)
	}
	if e = g.Close(ctx); e != nil {
		t.Fatal(e)
	}
	// New process can resume leaked bytes only after the old owner released.
	fresh, e := OpenSource(ctx, s, repoID, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	next, e := fresh.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	victims, e := next.Cleanup(ctx, 1)
	if e != nil || len(victims) != 1 {
		t.Fatal("bounded cleanup", victims, e)
	}
	if e = next.Execute(ctx, next.Plan(time.Now().UTC().Format(time.RFC3339Nano), Policy{3600, 1}, victims)); e != nil {
		t.Fatal(e)
	}
	if e = next.Close(ctx); e != nil {
		t.Fatal(e)
	}
	reader, e := fresh.AdmitRestore(ctx, targetID, UUID())
	if e != nil {
		t.Fatal(e)
	}
	cat, e := reader.Catalog(ctx, CatalogLimits{10, 32 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer cat.Close()
	if e = cat.Visit(ctx, func(en Entry) error {
		if !en.Retired {
			t.Fatal("expiration restored selection")
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	requireOracle(t, s)
}

func TestRetentionCompleteCleanupPrerequisites(t *testing.T) {
	r, s := setup(t)
	h, e := r.AdmitBackup(ctx, writerID, backupID)
	if e != nil {
		t.Fatal(e)
	}
	a, _, e := h.Begin(ctx, request(backupID))
	if e != nil {
		t.Fatal(e)
	}
	h.Close(ctx)
	listing := &uploadInventoryStore{Storage: s, uploads: []s3store.Upload{{Key: r.attempt(backupID, a.ID()) + "data/0.tar", ID: "lost-init"}}}
	r.store = listing
	g, e := r.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer g.Close(ctx)
	listing.partial = true
	if victims, e := g.Cleanup(ctx, 1); e == nil || victims != nil {
		t.Fatal("partial MPU list authorized destruction", victims, e)
	}
	listing.partial = false
	victims, e := g.Cleanup(ctx, 1)
	if e != nil || len(victims) != 1 || victims[0].Kind != "abort-artifact" {
		t.Fatal(victims, e)
	}
	listing.uploads[0].Key = r.root + "backups/" + backupID + "/attempts/" + UUID() + "/data/0.tar"
	if victims, e := g.Cleanup(ctx, 1); e == nil || victims != nil {
		t.Fatal("unclaimed MPU accepted", victims, e)
	}
	if s.deletes != 0 {
		t.Fatal("inventory destroyed data")
	}
}

// Original destructive responses, not repeat HEAD/DELETE, own drain. Exercise
// each retirement/payload boundary with both conclusively rejected and lost ack.
func TestRetentionInterruptedStepsNeverAdoptUncertainOwner(t *testing.T) {
	for _, fault := range []string{"reject", "lost", "delay"} {
		for step := 1; step <= 4; step++ {
			t.Run(fault+string(rune('0'+step)), func(t *testing.T) {
				r, s := setup(t)
				h, res := publish(t, r, backupID, "interrupt target")
				h.Close(ctx)
				g, e := r.AcquireGC(ctx)
				if e != nil {
					t.Fatal(e)
				}
				uid, attempt := backupID, res.Commit.AttemptID
				index := 0
				comp := "none"
				vs := []Victim{{Kind: "retire-backup", BackupUID: &uid, SHA256: digest(res.Bytes)},
					{Kind: "delete-manifest", BackupUID: &uid, AttemptID: &attempt, SHA256: res.Commit.ManifestSHA256},
					{Kind: "delete-artifact", BackupUID: &uid, AttemptID: &attempt, Index: &index, Compression: &comp, SHA256: res.Commit.Artifacts[0].StoredSHA256}}
				// One immutable plan write followed by retirement and two payload deletes.
				s.failAt, s.fault = s.n+step, fault
				e = g.Execute(ctx, gcplan(g, vs...))
				if e == nil {
					t.Fatal("fault did not fire")
				}
				closeErr := g.Close(ctx)
				fresh, oe := OpenSource(ctx, s, repoID, t.TempDir())
				if oe != nil {
					t.Fatal(oe)
				}
				if fault != "reject" {
					if !errors.Is(closeErr, ErrUncertain) {
						t.Fatal("ambiguous owner released", closeErr)
					}
					s.deliver()
					if _, e = fresh.AdmitRestore(ctx, targetID, UUID()); e != ErrBlocked {
						t.Fatal("fresh process adopted old owner", e)
					}
				} else if closeErr != nil {
					t.Fatal(closeErr)
				}
				requireOracle(t, s)
			})
		}
	}
}

func TestRetentionArchiveInventoryRejectsUnknownAndPartial(t *testing.T) {
	r, s := setup(t)
	name := "000000010000000000000001"
	key, _ := r.WALKey(name)
	ret := WALRetirement{1, "retired", repoID, name, identity().WALSegmentBytes, strings.Repeat("a", 64), UUID()}
	b, _ := json.Marshal(ret)
	s.objects[key] = object{b, s3store.Info{Key: key, ETag: "retired", Size: int64(len(b)), Metadata: map[string]string{"cnpg-format": "wal-retired-v1"}}}
	g, e := r.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer g.Close(ctx)
	count := 0
	if e = g.ArchiveInventory(ctx, func(o WALObject) error {
		count++
		if !o.Retired || o.Name != name {
			t.Fatal(o)
		}
		return nil
	}); e != nil || count != 1 {
		t.Fatal(count, e)
	}
	s.partialList = true
	if e = g.ArchiveInventory(ctx, func(WALObject) error { return nil }); e == nil {
		t.Fatal("partial WAL list succeeded")
	}
	s.partialList = false
	s.objects[key] = object{[]byte("unknown"), s3store.Info{Key: key, ETag: "bad", Size: 7}}
	if e = g.ArchiveInventory(context.Background(), func(WALObject) error { return nil }); e == nil {
		t.Fatal("unknown WAL metadata succeeded")
	}
}
