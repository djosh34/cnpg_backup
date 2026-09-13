package repository

import (
	"context"
	"reflect"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

type uploadInventoryStore struct {
	Storage
	uploads []s3store.Upload
	aborted []s3store.Upload
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
func (s *uploadInventoryStore) Abort(ctx context.Context, u s3store.Upload) error {
	s.aborted = append(s.aborted, u)
	return s.Storage.Abort(ctx, u)
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
	if _, e := r.AcquireGC(ctx); e != ErrBlocked {
		t.Fatal("active producer did not block GC", e)
	}
	if e := h.Close(ctx); e != nil {
		t.Fatal(e)
	}
	listing := &uploadInventoryStore{Storage: s, uploads: []s3store.Upload{{Key: r.attempt(backupID, a.ID()) + "manifest.pg.json", ID: "unknown-initiation-id"}, {Key: r.attempt(backupID, a.ID()) + "data/0.tar.gz", ID: "orphan-artifact-id"}}}
	r.store = listing
	g, e := r.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := r.AdmitRestore(ctx, targetID, UUID()); e != ErrBlocked {
		t.Fatal("GC owner did not block restore", e)
	}
	s.partialList = true
	if victims, e := g.Cleanup(ctx, 128); e == nil || victims != nil {
		t.Fatal("partial object inventory escaped", victims, e)
	}
	s.partialList = false
	listing.partial = true
	if victims, e := g.Cleanup(ctx, 128); e == nil || victims != nil {
		t.Fatal("partial upload inventory escaped", e)
	}
	if s.deletes != 0 {
		t.Fatal("enumeration performed cleanup")
	}
	listing.partial = false
	victims, e := g.Cleanup(ctx, 128)
	if e != nil || len(victims) != 2 {
		t.Fatal("complete orphan inventory", len(victims), e)
	}
	if e = g.Execute(ctx, gcplan(g, victims...)); e != nil {
		t.Fatal(e)
	}
	if s.deletes != 2 {
		t.Fatal("missing serial explicit aborts", s.deletes)
	}
	if !reflect.DeepEqual(listing.aborted, listing.uploads) {
		t.Fatal("aborted different uploads", listing.aborted)
	}
	if e = g.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if victims, e := g.Cleanup(ctx, 128); e != ErrClosed || victims != nil {
		t.Fatal("closed owner inventoried uploads", victims, e)
	}
	requireOracle(t, s)
}
