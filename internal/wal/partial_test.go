package wal

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

func TestPromotionPartialConcurrentIdenticalPublication(t *testing.T) {
	w, b := setup(t, 1<<20)
	const name = "000000010000000000000003.partial"
	raw := bytes.Repeat([]byte{73}, 1<<20)
	src := source(t, raw)
	gateKey := "v1/" + w.Repository.Identity().RepositoryID + "/gate.json"
	before := append([]byte(nil), b.objects[gateKey].b...)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results := make(chan error, 2)
	for _, compression := range []string{"none", "gzip"} {
		copy := w
		copy.Compression = compression
		go func() { results <- copy.Archive(ctx, name, src) }()
	}
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal("concurrent identical auxiliary publication", err)
		}
	}
	root, dir := local(t)
	if err := w.Restore(ctx, name, root, name); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatal("concurrent publication bytes", err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.objects) != 3 || !bytes.Equal(b.objects[gateKey].b, before) {
		t.Fatal("auxiliary publication added gate/holder writes or non-distinct objects")
	}
}

// Promotion can queue the old timeline's last segment as a distinct .partial
// archive file. Durable preservation must not publish it as a complete segment.
func TestPromotionPartialDurableDistinctSlot(t *testing.T) {
	const full = "000000010000000000000003"
	const partial = full + ".partial"
	data := bytes.Repeat([]byte{42}, 16<<20)
	if path := os.Getenv("CNPG_TEST_PROMOTION_PARTIAL"); path != "" {
		var err error
		data, err = os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if filepath.Base(path) != partial || len(data) != 16<<20 {
			t.Fatal("native fixture must be the observed promotion name and physical segment size")
		}
	}
	for _, compression := range []string{"none", "gzip"} {
		t.Run(compression, func(t *testing.T) {
			w, _ := setup(t, 16<<20)
			w.Compression = compression
			ctx := context.Background()
			src := source(t, data)
			if err := w.Archive(ctx, partial, src); err != nil {
				t.Fatalf("promotion archive queue cannot drain: Archive(%q): %v", partial, err)
			}
			w.Compression = "none"
			if err := w.Archive(ctx, partial, src); err != nil {
				t.Fatal("identical retry", err)
			}
			dst, dir := local(t)
			if err := w.Restore(ctx, partial, dst, partial); err != nil {
				t.Fatal("exact auxiliary retrieval", err)
			}
			got, err := os.ReadFile(filepath.Join(dir, partial))
			if err != nil || !bytes.Equal(got, data) {
				t.Fatal("durable partial byte oracle", err)
			}
			if err := w.Restore(ctx, full, dst, full); !s3store.Is(err, s3store.NotFound) {
				t.Fatal("partial must not substitute for full segment", err)
			}
			other := append([]byte(nil), data...)
			other[0] ^= 1
			if err := w.Archive(ctx, partial, source(t, other)); err != ErrConflict {
				t.Fatal("different partial content must conflict", err)
			}
			if err := w.Archive(ctx, full, source(t, other)); err != nil {
				t.Fatal("full filename is an independent slot", err)
			}
			if err := w.Restore(ctx, partial, dst, partial); err != nil {
				t.Fatal(err)
			}
			got, err = os.ReadFile(filepath.Join(dir, partial))
			if err != nil || !bytes.Equal(got, data) {
				t.Fatal("full publication changed partial", err)
			}
		})
	}
}
