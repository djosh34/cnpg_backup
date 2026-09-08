package wal

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

func TestPromotionPartialExactPhysicalSize(t *testing.T) {
	const name = "000000010000000000000001.partial"
	for _, size := range []int64{1 << 20, 16 << 20, 64 << 20, 1 << 30} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			w, b := setup(t, size)
			key, max, err := w.Limits(name)
			fullKey, _, fullErr := w.Limits(name[:24])
			if err != nil || fullErr != nil || max != size || key != fullKey+".partial" {
				t.Fatal("partial lost physical bounds or distinct key", key, max, err)
			}
			if _, historyMax, err := w.Limits("00000002.history"); err != nil || historyMax != 1<<20 {
				t.Fatal("history limit changed", historyMax, err)
			}
			for _, n := range []int64{0, size - 1, size + 1} {
				f := source(t, nil)
				// Sparse invalid inputs fail before reading or allocating payload.
				if err := f.Truncate(n); err != nil {
					t.Fatal(err)
				}
				if err := w.Archive(context.Background(), name, f); err != ErrInvalid {
					t.Fatal("accepted short/oversized promotion source", n, err)
				}
			}
			if b.put != 0 {
				t.Fatal("invalid size reached PUT")
			}
			info := s3store.Info{Size: size, Metadata: map[string]string{"cnpg-format": "wal-v1", "cnpg-system-id": w.Repository.Identity().SystemIdentifier, "cnpg-compression": "none", "cnpg-raw-sha256": hash(nil), "cnpg-stored-sha256": hash(nil)}}
			for _, n := range []int64{0, size - 1, size, size + 1} {
				info.Metadata["cnpg-raw-bytes"] = strconv.FormatInt(n, 10)
				_, err := w.metadata(name, info)
				if (err == nil) != (n == size) {
					t.Fatal("retrieved partial metadata bypassed exact physical size", n, err)
				}
			}
		})
	}
}

func TestPromotionPartialFailureAndIntegrity(t *testing.T) {
	const name = "000000010000000000000001.partial"
	for _, fault := range []string{"lost", "killed", "transient", "auth", "canceled", "corrupt", "truncated", "extra-member", "retired"} {
		t.Run(fault, func(t *testing.T) {
			w, b := setup(t, 1<<20)
			raw := bytes.Repeat([]byte{61}, 1<<20)
			src := source(t, raw)
			ctx := context.Background()
			switch fault {
			case "lost", "killed", "transient", "auth":
				b.setFault(fault)
				err := w.Archive(ctx, name, src)
				if (err == nil) != (fault == "lost") {
					t.Fatal("only verified durable lost response may acknowledge", err)
				}
				if fault == "auth" && !s3store.Is(err, s3store.Auth) {
					t.Fatal("authentication error became success/missing", err)
				}
				b.setFault("")
			case "canceled":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				if err := w.Archive(canceled, name, src); err == nil {
					t.Fatal("canceled archive acknowledged")
				}
			}
			if err := w.Archive(ctx, name, src); err != nil {
				t.Fatal("retry after cleared fault", err)
			}
			key, _ := w.Repository.WALKey(name)
			b.mu.Lock()
			o := b.objects[key]
			switch fault {
			case "corrupt":
				o.b[len(o.b)-1] ^= 1
			case "truncated":
				o.b = o.b[:len(o.b)-4]
			case "extra-member":
				o.b = append(o.b, o.b...)
			case "retired":
				o.metadata["cnpg-format"] = "wal-retired-v1"
			}
			// Keep transport hash correct to exercise gzip/raw integrity too.
			o.metadata["cnpg-stored-sha256"] = hash(o.b)
			b.objects[key] = o
			b.mu.Unlock()
			root, dir := local(t)
			if err := os.WriteFile(filepath.Join(dir, name), []byte("previous"), 0600); err != nil {
				t.Fatal(err)
			}
			err := w.Restore(ctx, name, root, name)
			invalid := fault == "corrupt" || fault == "truncated" || fault == "extra-member" || fault == "retired"
			if invalid {
				want := ErrCorrupt
				if fault == "retired" {
					want = ErrExpired
				}
				if err != want {
					t.Fatal("damaged/retired auxiliary became usable or missing", err)
				}
				if err := w.Archive(ctx, name, src); err != want {
					t.Fatal("damaged/retired slot acknowledged or recreated", err)
				}
			}
			got, readErr := os.ReadFile(filepath.Join(dir, name))
			want := raw
			if invalid {
				want = []byte("previous")
			}
			if !invalid && err != nil || readErr != nil || !bytes.Equal(got, want) {
				t.Fatal("partial publication byte oracle", err, readErr)
			}
		})
	}
}
