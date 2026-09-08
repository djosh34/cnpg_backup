// Copyright 2026 cnpg_backup contributors. All rights reserved.
package postgres

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
)

// Exercise the scan -> materialization path used by RestoreFull, without its
// dedicated PVC preflight or actual PostgreSQL processes. Only native execution
// is substituted: the combine stand-in copies the selected extracted tree, as
// when a relation present in F has been dropped by D2.
func TestRestoreHistoricalInputExceedsOutputBudget(t *testing.T) {
	for _, oversizedOutput := range []bool{false, true} {
		name := "dropped-relation-fits"
		if oversizedOutput {
			name = "oversized-synthetic-rejected"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			full, fc, n := syntheticRestoreWithFiles(t, false, map[string][]byte{"base/5/42": make([]byte, 3<<20)})
			differential, dc, _ := syntheticRestore(t, false)
			dc.Kind = "differential"
			n.MaxRestoredBytes = 2 << 20 // F > 4 MiB; selected result just over 1 MiB.
			inputs := []*fullInput{full, differential}
			chain := []repository.Commit{fc, dc}
			// Use the unchanged destination configuration, not an inflated output
			// cap that would also inflate every target filesystem reservation.
			for i, in := range inputs {
				if e := in.scan(ctx, chain[i], 1<<20, n); e != nil {
					t.Fatal("original scan", i, e)
				}
			}
			out := t.TempDir()
			layout := RestoreLayout{PGDATA: out + "/data", WALDirectory: out + "/wal", Tablespaces: map[string]string{}}
			originalsVerified := 0
			combined := false
			run := func(_ context.Context, tool string, args ...string) ([]byte, error) {
				switch tool {
				case "pg_verifybackup":
					if !combined {
						originalsVerified++
					}
				case "pg_combinebackup":
					if originalsVerified != 2 {
						t.Fatal("combine before both extracted originals verified")
					}
					combined = true
					source := args[len(args)-1] // D2: dropped relation absent.
					if oversizedOutput {
						source = args[len(args)-2] // F: output exceeds the same cap.
					}
					return nil, os.CopyFS(layout.PGDATA, os.DirFS(source))
				}
				return nil, nil
			}
			got, e := combineOriginals(ctx, inputs, chain, layout, n, run)
			if !combined {
				t.Fatal("did not reach native reconstruction", e)
			}
			if oversizedOutput {
				if e == nil || !strings.Contains(e.Error(), "native synthetic output budget exceeded") {
					t.Fatal("oversized synthetic output not rejected", e)
				}
				if st, err := os.Lstat(layout.PGDATA + "/pg_wal"); err != nil || !st.IsDir() {
					t.Fatal("output transformed before budget rejection", err)
				}
				return
			}
			if e != nil {
				t.Fatal("smaller F+D2 result rejected", e)
			}
			if _, e = os.Stat(layout.PGDATA + "/base/5/42"); !os.IsNotExist(e) {
				t.Fatal("dropped historical relation retained", e)
			}
			if len(got) != 1 || got["000000010000000000000001"].Size != 1<<20 {
				t.Fatal("missing restored WAL", got)
			}
		})
	}
}

func TestRestoreFullOnlyStillRejectsOutputBudget(t *testing.T) {
	in, c, n := syntheticRestoreWithFiles(t, false, map[string][]byte{"base/5/42": make([]byte, 3<<20)})
	n.MaxRestoredBytes = 2 << 20
	if e := in.scan(context.Background(), c, 1<<20, n); e != nil {
		t.Fatal("original scan", e)
	}
	layout := RestoreLayout{PGDATA: liveData, WALDirectory: liveData + "/pg_wal", Tablespaces: map[string]string{}}
	var budgets []configuration.FilesystemBudget
	units := map[string]int64{}
	for _, p := range restoreMounts(layout) {
		budgets = append(budgets, configuration.FilesystemBudget{Mount: p, LimitBytes: 8 << 30})
		units[p] = 8192
	}
	// This is RestoreFull's mandatory full-only admission, before native tools
	// or target writes. Adequate filesystem space cannot override the output cap.
	if _, e := restoreOutputBudget(in, c, n, budgets, layout, units); e == nil {
		t.Fatal("oversized full-only output admitted")
	}
	n.MaxRestoredBytes = 8 << 20
	if _, e := restoreOutputBudget(in, c, n, budgets, layout, units); e != nil {
		t.Fatal("full-only control rejected with adequate output budget", e)
	}
}
