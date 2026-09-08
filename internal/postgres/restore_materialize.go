// Copyright 2026 cnpg_backup contributors. All rights reserved.
package postgres

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
)

func (in *fullInput) materialize(ctx context.Context, c repository.Commit, l RestoreLayout, run restoreRunner) (map[string]s3store.Integrity, error) {
	if e := in.extractOriginal(ctx, c, l, run); e != nil {
		return nil, e
	}
	return in.finish(ctx, c, l)
}

func (in *fullInput) extractOriginal(ctx context.Context, c repository.Commit, l RestoreLayout, run restoreRunner) error {
	targets := []string{l.PGDATA}
	if l.WALDirectory != l.PGDATA+"/pg_wal" {
		targets = append(targets, l.WALDirectory)
	}
	targets = append(targets, mapValues(l.Tablespaces)...)
	sort.Strings(targets)
	for _, p := range targets {
		if e := emptyRestoreRoot(p); e != nil {
			return e
		}
	}
	if e := in.extractArchive(ctx, c, "base.tar", l.PGDATA); e != nil {
		return e
	}
	root, e := os.OpenRoot(l.PGDATA)
	if e != nil {
		return e
	}
	defer root.Close()
	if e = root.MkdirAll("pg_wal", 0700); e != nil {
		return e
	}
	if e = root.MkdirAll("pg_tblspc", 0700); e != nil {
		return e
	}
	if e = in.extractArchive(ctx, c, "pg_wal.tar", l.PGDATA+"/pg_wal"); e != nil {
		return e
	}
	for _, ts := range c.Tablespaces {
		oid := strconv.FormatUint(uint64(ts.OID), 10)
		target := l.Tablespaces[ts.Name]
		if e = in.extractArchive(ctx, c, oid+".tar", target); e != nil {
			return e
		}
		// Only trusted destination links; preserve the original map for verification.
		if e = root.Symlink(target, "pg_tblspc/"+oid); e != nil {
			return e
		}
	}
	manifest, e := os.Open(filepath.Join(in.directory, "backup_manifest"))
	if e != nil {
		return e
	}
	e = copyMember(root, "backup_manifest", c.ManifestBytes, contextInput{ctx, manifest})
	manifest.Close()
	if e != nil {
		return e
	}
	if _, e = run(ctx, "pg_verifybackup", "--exit-on-error", "--no-parse-wal", l.PGDATA); e != nil {
		return e
	}
	return verifyRestoreWAL(ctx, l.PGDATA+"/pg_wal", in.manifest.Ranges, run)
}

func (in *fullInput) finish(ctx context.Context, c repository.Commit, l RestoreLayout) (map[string]s3store.Integrity, error) {
	root, e := os.OpenRoot(l.PGDATA)
	if e != nil {
		return nil, e
	}
	defer root.Close()
	targets := append([]string{l.PGDATA, l.WALDirectory}, mapValues(l.Tablespaces)...)
	// Only now are sanctioned bootstrap transforms permitted. Never rewrite the
	// original manifest to make a damaged input pass native verification.
	if e = root.Remove("tablespace_map"); e != nil && (len(c.Tablespaces) > 0 || !os.IsNotExist(e)) {
		return nil, e
	}
	expected, e := bundleIntegrity(ctx, l.PGDATA+"/pg_wal", in.archives["pg_wal.tar"])
	if e != nil {
		return nil, e
	}
	if l.WALDirectory != l.PGDATA+"/pg_wal" {
		if e = relocateRestoreWAL(ctx, root, l.WALDirectory, in.archives["pg_wal.tar"], expected); e != nil {
			return nil, e
		}
	}
	for _, p := range targets {
		if e = syncRestoreTree(ctx, p); e != nil {
			return nil, e
		}
	}
	actual, e := bundleIntegrity(ctx, l.WALDirectory, in.archives["pg_wal.tar"])
	if e != nil {
		return nil, e
	}
	if !sameBundles(actual, expected) {
		return nil, ErrInput
	}
	if e = ctx.Err(); e != nil {
		return nil, e
	}
	return actual, nil
}

func bundleIntegrity(ctx context.Context, directory string, inv archiveInventory) (map[string]s3store.Integrity, error) {
	root, e := os.OpenRoot(directory)
	if e != nil {
		return nil, e
	}
	defer root.Close()
	result := map[string]s3store.Integrity{}
	for name, size := range inv.files {
		if len(name) != 24 || !repository.ValidWALFilename(name) {
			continue
		} // .done is preserved, not a bundle.
		st, e := root.Lstat(name)
		if e != nil || !st.Mode().IsRegular() || st.Size() != size {
			return nil, ErrInput
		}
		f, e := root.Open(name)
		if e != nil {
			return nil, e
		}
		hash, e := fileHash(ctx, f)
		f.Close()
		if e != nil {
			return nil, e
		}
		result[name] = s3store.Integrity{Size: size, SHA256: hash}
	}
	if len(result) == 0 {
		return nil, ErrInput
	}
	return result, nil
}
func sameBundles(a, b map[string]s3store.Integrity) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if other, ok := b[k]; !ok || other != v {
			return false
		}
	}
	return true
}

// Ordinary cross-filesystem copies: fsync every file, compare actual copied WAL
// hashes, and sync the destination directories BEFORE removing our old output.
// No rename-across-PVC assumption, reflink/hardlink or source-path cleanup.
func relocateRestoreWAL(ctx context.Context, pg *os.Root, target string, inv archiveInventory, expected map[string]s3store.Integrity) error {
	dst, e := os.OpenRoot(target)
	if e != nil {
		return e
	}
	defer dst.Close()
	for name, size := range inv.files {
		src, e := pg.Open("pg_wal/" + name)
		if e != nil {
			return e
		}
		e = copyMember(dst, name, size, contextInput{ctx, src})
		src.Close()
		if e != nil {
			return e
		}
	}
	dirs, e := nativeDirectories(inv)
	if e != nil {
		return e
	}
	// The base tar also supplies empty pg_wal bootstrap directories. Ensure both
	// survive relocation even if absent from the WAL tar's explicit headers.
	dirs["archive_status"], dirs["summaries"] = true, true
	for d := range dirs {
		if e = dst.MkdirAll(d, 0700); e != nil {
			return e
		}
	}
	copied, e := bundleIntegrity(ctx, target, inv)
	if e != nil {
		return e
	}
	if !sameBundles(copied, expected) {
		return ErrInput
	}
	if e = syncRestoreTree(ctx, target); e != nil {
		return e
	}
	if e = ctx.Err(); e != nil {
		return e
	}
	if e = pg.RemoveAll("pg_wal"); e != nil {
		return e
	}
	if e = pg.Symlink(target, "pg_wal"); e != nil {
		return e
	}
	return nil
}
