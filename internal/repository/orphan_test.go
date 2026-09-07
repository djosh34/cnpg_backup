package repository

import (
	"context"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

type uploadInventoryStore struct {
	Storage
	uploads []s3store.Upload
	partial bool
}

func (s *uploadInventoryStore) ListUploads(_ context.Context, _ string, max int, visit func(s3store.Upload) error) error {
	for i, u := range s.uploads {
		if i >= max {
			return ErrCapacity
		}
		if e := visit(u); e != nil {
			return e
		}
		if s.partial {
			return ErrCorrupt
		}
	}
	return nil
}
func TestAdmittedOrphanUploadDiscoveryAndAbort(t *testing.T) {
	r, s := setup(t)
	h, e := r.AdmitBackup(ctx, writerID, backupID)
	if e != nil {
		t.Fatal(e)
	}
	a, _, e := h.Begin(ctx, request(backupID))
	if e != nil {
		t.Fatal(e)
	}
	// A producer that conclusively stopped before completion leaves a valid
	// claimed namespace. Enumeration, not object age, recovers unknown upload IDs.
	h.Close(ctx)
	listing := &uploadInventoryStore{Storage: s, uploads: []s3store.Upload{{Key: r.attempt(backupID, a.ID()) + "manifest.pg.json", ID: "unknown-initiation-id"}, {Key: r.attempt(backupID, a.ID()) + "data/0.tar.gz", ID: "orphan-artifact-id"}}}
	r.store = listing
	g, e := r.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	listing.partial = true
	if victims, e := g.OrphanUploads(ctx, backupID, a.ID()); e == nil || len(victims) != 0 {
		t.Fatal("partial upload inventory escaped", e)
	}
	if s.deletes != 0 {
		t.Fatal("enumeration performed cleanup")
	}
	listing.partial = false
	victims, e := g.OrphanUploads(ctx, backupID, a.ID())
	if e != nil || len(victims) != 2 {
		t.Fatal("complete orphan inventory", len(victims), e)
	}
	if e = g.Execute(ctx, gcplan(g, victims...)); e != nil {
		t.Fatal(e)
	}
	if s.deletes != 2 {
		t.Fatal("missing serial explicit aborts", s.deletes)
	}
	if e = g.Close(ctx); e != nil {
		t.Fatal(e)
	}
	requireOracle(t, s)
}
