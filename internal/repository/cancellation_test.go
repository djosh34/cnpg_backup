package repository

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
)

func TestCatalogCancellationStopsBetweenRecords(t *testing.T) {
	for _, records := range []int{1, 10000} {
		t.Run(strconv.Itoa(records), func(t *testing.T) {
			r, _ := setup(t)
			h, res := publish(t, r, backupID, "full")
			cat, e := h.Catalog(ctx, CatalogLimits{10, 32 << 20})
			if e != nil {
				t.Fatal(e)
			}
			defer cat.Close()
			// A large disk scan input, not 10,000 distinct remote backups.
			if e = cat.file.Truncate(0); e != nil {
				t.Fatal(e)
			}
			if _, e = cat.file.Seek(0, 0); e != nil {
				t.Fatal(e)
			}
			enc := json.NewEncoder(cat.file)
			for i := 0; i < records; i++ {
				if e = enc.Encode(Entry{Commit: res.Commit}); e != nil {
					t.Fatal(e)
				}
			}
			c, cancel := context.WithCancel(ctx)
			defer cancel()
			visited := 0
			e = cat.Visit(c, func(Entry) error { visited++; cancel(); return nil })
			if !errors.Is(e, context.Canceled) || visited != 1 {
				t.Errorf("Visit=%v callbacks=%d, want canceled after exactly one", e, visited)
			}
			if e = h.Close(ctx); e != nil {
				t.Fatalf("Close after canceled scan: %v", e)
			}
		})
	}
}

func TestGCInventoryCancellationStopsBetweenRecords(t *testing.T) {
	r, _ := setup(t)
	for _, uid := range []string{backupID, "77777777-7777-4777-8777-777777777777"} {
		h, _ := publish(t, r, uid, "full")
		if e := h.Close(ctx); e != nil {
			t.Fatal(e)
		}
	}
	g, e := r.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	c, cancel := context.WithCancel(ctx)
	defer cancel()
	visited := 0
	e = g.Inventory(c, CatalogLimits{10, 32 << 20}, func(Entry) error { visited++; cancel(); return nil })
	if !errors.Is(e, context.Canceled) || visited != 1 {
		t.Errorf("Inventory=%v callbacks=%d", e, visited)
	}
	if e = g.Close(ctx); e != nil {
		t.Fatalf("Close after canceled inventory: %v", e)
	}
}

func TestGCDependencyScanHonorsCancellation(t *testing.T) {
	r, s := setup(t)
	h, res := publish(t, r, backupID, "full")
	if e := h.Close(ctx); e != nil {
		t.Fatal(e)
	}
	g, e := r.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	c, cancel := context.WithCancel(ctx)
	defer cancel()
	// The fake's reads intentionally do not implement cancellation, isolating
	// the disk scan's own responsibility after the inventory completes.
	r.store = &workspaceFailureStore{Storage: s, afterList: cancel}
	before := s.n
	e = g.Execute(c, gcplan(g, retire(res)))
	if !errors.Is(e, context.Canceled) || s.n != before {
		t.Errorf("Execute=%v mutations=%d, want canceled before dispatch", e, s.n-before)
	}
	if e = g.Close(ctx); e != nil {
		t.Fatal(e)
	}
}
