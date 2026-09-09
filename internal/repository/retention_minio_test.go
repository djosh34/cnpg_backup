package repository

import (
	"testing"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

// Uses the actual adapter/conditional MinIO requests in both signature modes.
// Synthetic native bytes qualify coordination, not SQL recovery (campaign owns it).
func minioRetentionBatches(t *testing.T, c s3store.Config) {
	s, e := s3store.New(c)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	r, e := Initialize(ctx, s, identity(), t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	h, res := publish(t, r, backupID, "actual retirement bytes")
	if e = h.Close(ctx); e != nil {
		t.Fatal(e)
	}
	reader, e := r.AdmitRestore(ctx, targetID, UUID())
	if e != nil {
		t.Fatal(e)
	}
	if _, e = r.AcquireGC(ctx); e != ErrBlocked {
		t.Fatal("actual holder did not exclude GC", e)
	}
	reader.Close(ctx)
	r.ReleaseLifetimeAfterTermination(ctx, targetID, reader.holder.OperationID)
	g, e := r.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if e = g.Inventory(ctx, CatalogLimits{10, 32 << 20}, func(Entry) error { return nil }); e != nil {
		t.Fatal(e)
	}
	if e = g.Execute(ctx, gcplan(g, retire(res))); e != nil {
		t.Fatal(e)
	}
	if e = g.Close(ctx); e != nil {
		t.Fatal(e)
	}
	// Reopen rather than reuse an old owner or plan. Permanent visibility stays
	// retired while each bounded batch removes a different remaining payload.
	for removed := 0; removed < 3; removed++ {
		next, e := OpenSource(ctx, s, repoID, t.TempDir())
		if e != nil {
			t.Fatal(e)
		}
		batch, e := next.AcquireGC(ctx)
		if e != nil {
			t.Fatal(e)
		}
		victims, e := batch.Cleanup(ctx, 1)
		if e != nil || len(victims) != 1 {
			t.Fatal("actual bounded leaked-payload inventory", victims, e)
		}
		if e = batch.Execute(ctx, gcplan(batch, victims...)); e != nil {
			t.Fatal(e)
		}
		if e = batch.Close(ctx); e != nil {
			t.Fatal(e)
		}
	}
	final, e := r.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	victims, e := final.Cleanup(ctx, 128)
	if e != nil || len(victims) != 0 {
		t.Fatal("actual cleanup incomplete", victims, e)
	}
	if e = final.Close(ctx); e != nil {
		t.Fatal(e)
	}
	rr, e := r.AdmitRestore(ctx, targetID, UUID())
	if e != nil {
		t.Fatal(e)
	}
	cat, e := rr.Catalog(ctx, CatalogLimits{10, 32 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer cat.Close()
	if e = cat.Visit(ctx, func(en Entry) error {
		if !en.Retired {
			t.Fatal("actual retired selection resurrected")
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if _, _, e = s.Read(ctx, r.backup(backupID)+"commit.json", commitLimit); e != nil {
		t.Fatal("permanent commit removed", e)
	}
	t.Log("PASS actual conditional retirement, holder exclusion, serial new-batch payload cleanup and permanent exclusion")
}
