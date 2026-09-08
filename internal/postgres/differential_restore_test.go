package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
)

func TestOriginalFailureNeverCombines(t *testing.T) {
	for _, failureAt := range []int{1, 2, 3, 4} {
		t.Run(string(rune('0'+failureAt)), func(t *testing.T) {
			f, fc, n := syntheticRestore(t, true)
			d, dc, _ := syntheticRestore(t, true)
			dc.Kind = "differential"
			for i, in := range []*fullInput{f, d} {
				if e := in.scan(context.Background(), []repository.Commit{fc, dc}[i], 1<<20, n); e != nil {
					t.Fatal(e)
				}
			}
			calls := 0
			failure := errors.New("original verification rejected")
			run := func(_ context.Context, tool string, args ...string) ([]byte, error) {
				if tool == "pg_combinebackup" {
					t.Fatal("combine ran after original failure")
				}
				calls++
				if tool == "pg_verifybackup" && !slices.Contains(args, "--no-parse-wal") {
					t.Fatal("shell-requiring verifier")
				}
				if calls == failureAt {
					return nil, failure
				}
				return nil, nil
			}
			out := t.TempDir()
			l := RestoreLayout{PGDATA: out + "/data", WALDirectory: out + "/wal", Tablespaces: map[string]string{"fast": out + "/ts"}}
			if _, e := combineOriginals(context.Background(), []*fullInput{f, d}, []repository.Commit{fc, dc}, l, n, run); !errors.Is(e, failure) {
				t.Fatal(e)
			}
			if _, e := os.Stat(l.PGDATA); !os.IsNotExist(e) {
				t.Fatal("output touched before originals verified", e)
			}
		})
	}
}

func TestCombineMapsLastInputAndOrdinaryCopy(t *testing.T) {
	c := repository.Commit{Tablespaces: []repository.Tablespace{{Name: "fast", OID: 42}}}
	args := combineArgs(inputLayout("/F", c), inputLayout("/D2", c), c, RestoreLayout{PGDATA: "/out", Tablespaces: map[string]string{"fast": "/out=ts"}})
	want := []string{"--copy", "--manifest-checksums=SHA256", "--output=/out", `--tablespace-mapping=/D2/ts-42=/out\=ts`, "/F/data", "/D2/data"}
	if !slices.Equal(args, want) {
		t.Fatal(args)
	}
}

func TestSyntheticManifestWALExceptionIsOutputOnly(t *testing.T) {
	in, _, _ := syntheticRestore(t, false)
	b, e := os.ReadFile(filepath.Join(in.directory, "backup_manifest"))
	if e != nil {
		t.Fatal(e)
	}
	var m map[string]any
	if e = json.Unmarshal(b, &m); e != nil {
		t.Fatal(e)
	}
	for _, test := range []struct {
		path string
		size int
		want bool
	}{
		{"pg_wal/000000010000000000000001", 1 << 20, true},
		{"pg_wal/archive_status/000000010000000000000001.done", 0, true},
		{"pg_wal/archive_status/000000010000000000000001.done", 1, false},
		{"pg_wal/archive_status/000000010000000000000001.ready", 0, false},
		{"pg_wal/archive_status/evil.done", 0, false},
		{"base/123", 1 << 20, false},
		{"pg_wal/evil", 1 << 20, false},
	} {
		original := m["Files"]
		m["Files"] = append(original.([]any), map[string]any{"Path": test.path, "Size": test.size, "Last-Modified": "2026-09-08 00:00:00 GMT"})
		b, _ := json.Marshal(m)
		m["Files"] = original
		if _, e = ScanManifest(bytes.NewReader(b)); e == nil {
			t.Fatal("original allowed missing checksum")
		}
		_, e = scanManifest(bytes.NewReader(b), true)
		if (e == nil) != test.want {
			t.Fatal(test, e)
		}
	}
}

func TestDifferentialCapacityIncludesBothOriginalsOutputAndWAL(t *testing.T) {
	_, f, n := syntheticRestore(t, true)
	d := f
	d.Kind = "differential"
	n.MaxReferenceAge = "192h"
	n.CaptureTimeout = "6h"
	l := RestoreLayout{PGDATA: liveData, WALDirectory: "/var/lib/postgresql/wal/pg_wal", Tablespaces: map[string]string{"fast": "/var/lib/postgresql/tablespaces/fast/data"}}
	budgets := []configuration.FilesystemBudget{}
	for _, p := range restoreMounts(l) {
		budgets = append(budgets, configuration.FilesystemBudget{Mount: p, LimitBytes: 8 << 30})
	}
	got, e := differentialRestoreBudget([]repository.Commit{f, d}, n, budgets, l, 8192)
	if e != nil {
		t.Fatal(e)
	}
	var scratch int64
	for _, b := range got {
		if b.Mount == workspace {
			scratch = b.RequiredBytes
		} else if b.RequiredBytes <= configuration.GiB {
			t.Fatal("missing output/WAL allocation", b)
		}
	}
	single, e := restoreDownloadBudget(f, n, budgets, l, 8192)
	if e != nil {
		t.Fatal(e)
	}
	for _, b := range single {
		if b.Mount == workspace && scratch <= 2*b.RequiredBytes {
			t.Fatal("both extracted originals not reserved")
		}
	}
	if _, e = differentialRestoreBudget([]repository.Commit{f, d, d}, n, budgets, l, 8192); e == nil {
		t.Fatal("arbitrary chain budget accepted")
	}
	// Per-volume conservatism must not bypass the aggregate supported ceiling.
	n.MaxRestoredBytes = 1024 * configuration.GiB
	for i := 0; i < 9; i++ {
		name := fmt.Sprint("extra", i)
		l.Tablespaces[name] = "/var/lib/postgresql/tablespaces/" + name + "/data"
	}
	budgets = nil
	for _, p := range restoreMounts(l) {
		budgets = append(budgets, configuration.FilesystemBudget{Mount: p, LimitBytes: 8192 * configuration.GiB})
	}
	if _, e = differentialRestoreBudget([]repository.Commit{f, d}, n, budgets, l, 8192); !errors.Is(e, repository.ErrCapacity) {
		t.Fatal("aggregate ceiling bypassed", e)
	}
}

func TestCombineCancellationDrainsBeforeReturn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	err := runCombine(ctx, RestoreLayout{}, configuration.Native{}, func(ctx context.Context, tool string, args ...string) ([]byte, error) {
		called = true
		return nil, ctx.Err()
	}, nil)
	if !called || !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if strings.Contains(err.Error(), "full") {
		t.Fatal("fallback")
	}
}
