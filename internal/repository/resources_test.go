package repository

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"runtime"
	"testing"
)

// Measure the ACTUAL production integrity reader, not fake-store buffering.
// A sparse disk file is intentional: it exercises all streamed bytes without
// indiscriminately exhausting the runner's filesystem.
func TestPayloadVerificationBoundedAllocation(t *testing.T) {
	f, e := os.CreateTemp(t.TempDir(), "large-disk-spool")
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	const size int64 = 64 << 20
	if e = f.Truncate(size); e != nil {
		t.Fatal(e)
	}
	h := sha256.New()
	if _, e = io.Copy(h, io.NewSectionReader(f, 0, size)); e != nil {
		t.Fatal(e)
	}
	hash := hex.EncodeToString(h.Sum(nil))
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if e = verifyFile(ctx, f, size, hash, "none", size, hash); e != nil {
		t.Fatal(e)
	}
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	if allocated > 4<<20 {
		t.Fatalf("64 MiB integrity verification allocated %d bytes", allocated)
	}
	t.Logf("actual verifyFile bytes=%d allocations=%d (4 MiB ceiling)", size, allocated)
}
