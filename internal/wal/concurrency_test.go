package wal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"os"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

func TestWALRemainsServiceableUnderSaturatedArtifactTransfers(t *testing.T) {
	w, b := setup(t, 1<<20)
	b.partsStarted = make(chan struct{}, 4)
	b.releaseParts = make(chan struct{})
	defer close(b.releaseParts)
	store := w.Store.(*s3store.Store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 2)
	for _, key := range []string{"artifact-one", "artifact-two"} {
		f, e := os.CreateTemp(w.Workspace, "artifact-*")
		if e != nil {
			t.Fatal(e)
		}
		defer cleanup(f)
		const size = s3store.PartSize + 1
		if e = f.Truncate(size); e != nil {
			t.Fatal(e)
		}
		h := sha256.New()
		if _, e = io.Copy(h, io.NewSectionReader(f, 0, size)); e != nil {
			t.Fatal(e)
		}
		go func() {
			_, _, e := store.UploadFile(ctx, key, f, s3store.Integrity{Size: size, SHA256: sum(h)}, nil)
			done <- e
		}()
	}
	// Both real SDK artifact slots and all four part workers are now occupied.
	// The fake HTTP server holds actual part requests, not an invented semaphore.
	for i := 0; i < 4; i++ {
		select {
		case <-b.partsStarted:
		case e := <-done:
			t.Fatalf("artifact exited before saturation at worker %d: %v", i, e)
		case <-time.After(20 * time.Second):
			t.Fatal("artifact fault precondition not reached")
		}
	}
	callback, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	src := source(t, bytes.Repeat([]byte{9}, 1<<20))
	if e := w.Archive(callback, "000000010000000000000001", src); e != nil {
		t.Fatal("WAL starved by backup transfer", e)
	}
	root, _ := local(t)
	if e := w.Restore(callback, "000000010000000000000001", root, "RECOVERYXLOG"); e != nil {
		t.Fatal("exact-file WAL restore starved", e)
	}
	cancel()
	for i := 0; i < 2; i++ {
		select {
		case e := <-done:
			if e == nil {
				t.Fatal("unfinished artifact reported success")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("artifact workers did not drain")
		}
	}
}
