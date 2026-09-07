package wal

import (
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func FuzzVerifiedGzipPublication(f *testing.F) {
	w, b := simulatedWAL(f)
	name := "00000002.history"
	raw := []byte("1\t0/100000\tfixture\n")
	key, _ := w.Repository.WALKey(name)
	var seed bytes.Buffer
	gz := gzip.NewWriter(&seed)
	gz.Write(raw)
	gz.Close()
	f.Add(seed.Bytes())
	f.Add(seed.Bytes()[:len(seed.Bytes())-4])
	f.Add([]byte("truncated"))
	f.Add(append(append([]byte(nil), seed.Bytes()...), seed.Bytes()...))
	directory := f.TempDir()
	root, e := os.OpenRoot(directory)
	if e != nil {
		f.Fatal(e)
	}
	f.Cleanup(func() { root.Close() })
	f.Fuzz(func(t *testing.T, compressed []byte) {
		if len(compressed) > 1<<20 {
			return
		}
		b.objects[key] = object{b: compressed, metadata: map[string]string{"cnpg-format": "wal-v1", "cnpg-system-id": w.Repository.Identity().SystemIdentifier, "cnpg-raw-bytes": strconv.Itoa(len(raw)), "cnpg-raw-sha256": hash(raw), "cnpg-stored-sha256": hash(compressed), "cnpg-compression": "gzip"}}
		original := []byte("not a verified output")
		if e := os.WriteFile(filepath.Join(directory, name), original, 0600); e != nil {
			t.Fatal(e)
		}
		e := w.Restore(context.Background(), name, root, name)
		actual, re := os.ReadFile(filepath.Join(directory, name))
		if re != nil {
			t.Fatal(re)
		}
		if e == nil {
			if !bytes.Equal(actual, raw) {
				t.Fatal("published wrong raw bytes")
			}
		} else if !bytes.Equal(actual, original) {
			t.Fatal("failed verification modified destination")
		}
	})
}
