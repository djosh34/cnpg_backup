// Copyright 2026 cnpg_backup contributors. All rights reserved.
package postgres

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
)

// Keep both compressed/raw download peaks, both extracted originals and their
// verification WAL. No incremental-header parsing or compression-ratio estimate.
// Each output filesystem conservatively reserves the complete output budget:
// PostgreSQL may move relation data between tablespaces without a size oracle.
func differentialRestoreBudget(chain []repository.Commit, n configuration.Native, budgets []configuration.FilesystemBudget, l RestoreLayout, unit int64) ([]configuration.FilesystemBudget, error) {
	if len(chain) != 2 || chain[0].Kind != "full" || chain[1].Kind != "differential" {
		return nil, ErrInput
	}
	set, e := restoreBudgetSet(budgets, l)
	if e != nil {
		return nil, e
	}
	need := map[string]int64{}
	for _, c := range chain {
		b, e := restoreDownloadBudget(c, n, budgets, l, unit)
		if e != nil {
			return nil, e
		}
		for _, v := range b {
			if v.Mount == workspace {
				need[workspace] += v.RequiredBytes
			}
		}
		raw := c.ManifestBytes
		for _, a := range c.Artifacts {
			raw += a.RawBytes
		}
		need[workspace] += raw + unit*(maxEntries+256)
	}
	for _, p := range append([]string{l.PGDATA}, mapValues(l.Tablespaces)...) {
		need[filepath.Dir(p)] = n.MaxRestoredBytes + unit*(maxEntries+256)
	}
	if l.WALDirectory != l.PGDATA+"/pg_wal" {
		need[filepath.Dir(l.WALDirectory)] = n.MaxBootstrapWALBytes + unit*(maxEntries+256)
	}
	return finishRestoreBudgets(set, need), nil
}

func inputLayout(directory string, c repository.Commit) RestoreLayout {
	l := RestoreLayout{PGDATA: directory + "/data", WALDirectory: directory + "/data/pg_wal", Tablespaces: map[string]string{}}
	for _, ts := range c.Tablespaces {
		l.Tablespaces[ts.Name] = directory + "/ts-" + strconv.FormatUint(uint64(ts.OID), 10)
	}
	return l
}

func combineArgs(full, differential RestoreLayout, c repository.Commit, output RestoreLayout) []string {
	args := []string{"--copy", "--manifest-checksums=SHA256", "--output=" + output.PGDATA}
	escape := func(s string) string { return strings.ReplaceAll(s, "=", `\=`) }
	for _, ts := range c.Tablespaces {
		args = append(args, "--tablespace-mapping="+escape(differential.Tablespaces[ts.Name])+"="+escape(output.Tablespaces[ts.Name]))
	}
	return append(args, full.PGDATA, differential.PGDATA)
}

func combineOriginals(ctx context.Context, inputs []*fullInput, chain []repository.Commit, l RestoreLayout, n configuration.Native, run restoreRunner) (map[string]s3store.Integrity, error) {
	if len(inputs) != 2 || len(chain) != 2 {
		return nil, ErrInput
	}
	layouts := make([]RestoreLayout, 2)
	for i, in := range inputs {
		layouts[i] = inputLayout(filepath.Dir(in.directory), chain[i])
		// These exact original bytes are verified before combine, including WAL.
		if e := in.extractOriginal(ctx, chain[i], layouts[i], run); e != nil {
			return nil, e
		}
	}
	for _, p := range append([]string{l.PGDATA, l.WALDirectory}, mapValues(l.Tablespaces)...) {
		if p == l.PGDATA+"/pg_wal" {
			continue
		}
		if e := emptyRestoreRoot(p); e != nil {
			return nil, e
		}
	}
	if e := runCombine(ctx, l, n, run, combineArgs(layouts[0], layouts[1], chain[1], l)); e != nil {
		return nil, e
	}
	if e := checkSyntheticSize(ctx, l, n); e != nil {
		return nil, e
	}
	f, e := os.Open(l.PGDATA + "/backup_manifest")
	if e != nil {
		return nil, e
	}
	m, e := scanManifest(contextInput{ctx, f}, true)
	f.Close()
	if e != nil {
		return nil, e
	}
	c := chain[1]
	if strconv.FormatUint(m.SystemIdentifier, 10) != c.SystemIdentifier || len(m.Ranges) != 1 || m.Ranges[0] != c.WALRanges[0] {
		return nil, ErrInput
	}
	if _, e = run(ctx, "pg_verifybackup", "--exit-on-error", "--no-parse-wal", l.PGDATA); e != nil {
		return nil, e
	}
	if e = verifyRestoreWAL(ctx, l.PGDATA+"/pg_wal", m.Ranges, run); e != nil {
		return nil, e
	}
	return inputs[1].finish(ctx, c, l)
}

// Poll the aggregate output cap as well as per-filesystem free margins. The
// finite PVC is still the hard writer stop between observations. Cancel waits
// for the same managed subprocess group before any transform or target release.
func runCombine(ctx context.Context, l RestoreLayout, n configuration.Native, run restoreRunner, args []string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done, monitored := make(chan struct{}), make(chan error, 1)
	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-done:
				monitored <- nil
				return
			case <-ctx.Done():
				monitored <- ctx.Err()
				return
			case <-tick.C:
				if e := checkSyntheticSize(ctx, l, n); e != nil {
					cancel()
					monitored <- e
					return
				}
			}
		}
	}()
	_, e := run(ctx, "pg_combinebackup", args...)
	close(done)
	me := <-monitored
	if e != nil {
		return e
	}
	return me
}

func checkSyntheticSize(ctx context.Context, l RestoreLayout, n configuration.Native) error {
	var bytes int64
	nodes := 0
	for _, root := range append([]string{l.PGDATA}, mapValues(l.Tablespaces)...) {
		e := filepath.WalkDir(root, func(p string, d os.DirEntry, e error) error {
			if e != nil {
				return e
			}
			if e = ctx.Err(); e != nil {
				return e
			}
			nodes++
			if nodes > maxEntries {
				return ErrInput
			}
			if d.Type()&os.ModeSymlink != 0 {
				// Only combine's explicitly mapped tablespace links may occur.
				for _, target := range l.Tablespaces {
					actual, e := os.Readlink(p)
					if e == nil && filepath.Dir(p) == l.PGDATA+"/pg_tblspc" && actual == target {
						return nil
					}
				}
				return ErrInput
			}
			if d.IsDir() {
				return nil
			}
			if !d.Type().IsRegular() {
				return ErrInput
			}
			st, e := d.Info()
			if e != nil {
				return e
			}
			bytes += st.Size()
			if st.Size() > maxFileBytes || bytes > n.MaxRestoredBytes {
				return errors.New("native synthetic output budget exceeded")
			}
			return nil
		})
		if e != nil {
			return e
		}
	}
	return nil
}
