// Copyright 2026 cnpg_backup contributors. All rights reserved.
package postgres

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"golang.org/x/sys/unix"
)

var tablespaceName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]{0,62}$`)

func validateRestoreLayout(l RestoreLayout, tables []repository.Tablespace) error {
	if l.PGDATA != liveData || (l.WALDirectory != liveData+"/pg_wal" && l.WALDirectory != "/var/lib/postgresql/wal/pg_wal") || len(l.Tablespaces) != len(tables) {
		return ErrInput
	}
	seen := map[uint32]bool{}
	for _, t := range tables {
		if !tablespaceName.MatchString(t.Name) || t.OID == 0 || seen[t.OID] || l.Tablespaces[t.Name] != "/var/lib/postgresql/tablespaces/"+t.Name+"/data" {
			return ErrInput
		}
		seen[t.OID] = true
	}
	return nil
}

func restoreMounts(l RestoreLayout) []string {
	mounts := []string{workspace, filepath.Dir(l.PGDATA)}
	if l.WALDirectory != l.PGDATA+"/pg_wal" {
		mounts = append(mounts, filepath.Dir(l.WALDirectory))
	}
	for _, p := range l.Tablespaces {
		mounts = append(mounts, filepath.Dir(p))
	}
	sort.Strings(mounts)
	return mounts
}

func restoreBudgetSet(budgets []configuration.FilesystemBudget, l RestoreLayout) (map[string]configuration.FilesystemBudget, error) {
	expected := restoreMounts(l)
	if len(expected) != len(budgets) {
		return nil, ErrInput
	}
	byMount := map[string]configuration.FilesystemBudget{}
	for _, b := range budgets {
		if b.LimitBytes <= 0 || b.LimitBytes > 8192*configuration.GiB {
			return nil, ErrInput
		}
		if _, ok := byMount[b.Mount]; ok {
			return nil, ErrInput
		}
		b.RequiredBytes = 0
		byMount[b.Mount] = b
	}
	for _, p := range expected {
		if _, ok := byMount[p]; !ok {
			return nil, ErrInput
		}
	}
	return byMount, nil
}

// Before downloads, reserve workspace using exact committed stored+raw lengths,
// duplicated WAL verification, and worst-supported metadata. Targets have no
// writes yet: their exact inventory reservation is checked after the scan.
func restoreDownloadCapacity(c repository.Commit, n configuration.Native, budgets []configuration.FilesystemBudget, l RestoreLayout) ([]configuration.FilesystemBudget, error) {
	unit, e := restoreAllocationUnit(workspace)
	if e != nil {
		return nil, e
	}
	return restoreDownloadBudget(c, n, budgets, l, unit)
}

func restoreDownloadBudget(c repository.Commit, n configuration.Native, budgets []configuration.FilesystemBudget, l RestoreLayout, unit int64) ([]configuration.FilesystemBudget, error) {
	if _, e := configuration.CaptureCapacity(n); e != nil {
		return nil, e
	}
	if n.MaxRestoredBytes < 1 || n.MaxRestoredBytes > 1024*configuration.GiB {
		return nil, ErrInput
	}
	set, e := restoreBudgetSet(budgets, l)
	if e != nil {
		return nil, e
	}
	raw, stored, wal := c.ManifestBytes, c.ManifestBytes, int64(0)
	for _, a := range c.Artifacts {
		if a.RawBytes <= 0 || a.RawBytes > n.MaxBackupBytes || a.StoredBytes <= 0 || a.StoredBytes > repository.MaxArtifactBytes {
			return nil, ErrInput
		}
		raw += a.RawBytes
		stored += a.StoredBytes
		if a.Role == "wal" {
			wal += a.RawBytes
		}
	}
	if raw > n.MaxBackupBytes || wal > n.MaxBootstrapWALBytes || stored > n.MaxBackupBytes+max(configuration.GiB, (n.MaxBackupBytes+99)/100) {
		return nil, ErrInput
	}
	if unit < 8192 || unit > 1<<20 {
		return nil, ErrInput
	}
	need := map[string]int64{workspace: raw + stored + wal + unit*(maxEntries+256)}
	// Preserve each target's spare margin while downloading. No target output has
	// been allocated and no native tool has yet been invoked.
	return finishRestoreBudgets(set, need), nil
}

func restoreAllocationUnit(mount string) (int64, error) {
	var fs unix.Statfs_t
	if e := unix.Statfs(mount, &fs); e != nil {
		return 0, e
	}
	if fs.Bsize <= 0 || fs.Bsize > 1<<20 {
		return 0, ErrInput
	}
	return max(8192, fs.Bsize), nil
}

func finishRestoreBudgets(set map[string]configuration.FilesystemBudget, need map[string]int64) []configuration.FilesystemBudget {
	result := make([]configuration.FilesystemBudget, 0, len(set))
	for p, b := range set {
		b.RequiredBytes = need[p] + configuration.CapacityMargin(need[p])
		result = append(result, b)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Mount < result[j].Mount })
	return result
}

func inventoryAllocation(inv archiveInventory, unit int64) (int64, int, error) {
	dirs, e := nativeDirectories(inv)
	if e != nil {
		return 0, 0, e
	}
	nodes := len(dirs) + len(inv.files)
	bytes := int64(nodes) * unit
	for _, n := range inv.files {
		bytes += ((n + unit - 1) / unit) * unit
	}
	return bytes, nodes, nil
}

func restoreOutputCapacity(in *fullInput, c repository.Commit, n configuration.Native, budgets []configuration.FilesystemBudget, l RestoreLayout) ([]configuration.FilesystemBudget, error) {
	units := map[string]int64{}
	for _, mount := range restoreMounts(l) {
		unit, e := restoreAllocationUnit(mount)
		if e != nil {
			return nil, e
		}
		units[mount] = unit
	}
	return restoreOutputBudget(in, c, n, budgets, l, units)
}

func restoreOutputBudget(in *fullInput, c repository.Commit, n configuration.Native, budgets []configuration.FilesystemBudget, l RestoreLayout, units map[string]int64) ([]configuration.FilesystemBudget, error) {
	for _, mount := range restoreMounts(l) {
		if unit := units[mount]; unit < 8192 || unit > 1<<20 {
			return nil, ErrInput
		}
	}
	initial, e := restoreDownloadBudget(c, n, budgets, l, units[workspace])
	if e != nil {
		return nil, e
	}
	set, e := restoreBudgetSet(budgets, l)
	if e != nil {
		return nil, e
	}
	need := map[string]int64{}
	// Keep the conservative scratch peak; it includes raw downloads, remaining
	// compressed spools, scratch WAL and metadata until the function returns.
	for _, b := range initial {
		if b.Mount == workspace {
			set[b.Mount] = b
		}
	}
	oidMount := map[string]string{}
	for _, ts := range c.Tablespaces {
		oidMount[strconv.FormatUint(uint64(ts.OID), 10)+".tar"] = filepath.Dir(l.Tablespaces[ts.Name])
	}
	pgMount := filepath.Dir(l.PGDATA)
	nodes, total := 0, c.ManifestBytes
	for name, inv := range in.archives {
		mount := pgMount
		if name != "base.tar" && name != "pg_wal.tar" {
			mount = oidMount[name]
			if mount == "" {
				return nil, ErrInput
			}
		}
		unit := units[mount]
		bytes, count, e := inventoryAllocation(inv, unit)
		if e != nil {
			return nil, e
		}
		need[mount] += bytes
		nodes += count
		total += inv.bytes
		if name == "pg_wal.tar" && l.WALDirectory != l.PGDATA+"/pg_wal" {
			walMount := filepath.Dir(l.WALDirectory)
			unit = units[walMount]
			bytes, _, e = inventoryAllocation(inv, unit)
			if e != nil {
				return nil, e
			}
			need[walMount] += bytes + 4*unit // bootstrap dirs and the physical destination root
		}
	}
	if nodes > maxEntries || total > n.MaxRestoredBytes {
		return nil, ErrInput
	}
	unit := units[pgMount]
	need[pgMount] += ((c.ManifestBytes+unit-1)/unit)*unit + unit*int64(len(c.Tablespaces)+8)
	result := finishRestoreBudgets(set, need)
	for i := range result {
		if result[i].Mount == workspace {
			result[i] = set[workspace]
		}
	}
	return result, nil
}

// New/empty physical data roots only. The guard's markers and plan live in
// siblings, never here. Refuse partial outputs instead of destructively retrying.
func emptyRestoreRoot(directory string) error {
	parent := filepath.Dir(directory)
	if resolved, e := filepath.EvalSymlinks(parent); e != nil || resolved != parent {
		return ErrInput
	}
	if e := os.Mkdir(directory, 0700); e != nil && !os.IsExist(e) {
		return e
	}
	st, e := os.Lstat(directory)
	if e != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return ErrInput
	}
	entries, e := os.ReadDir(directory)
	if e != nil {
		return e
	}
	if len(entries) != 0 {
		return ErrInput
	}
	return os.Chmod(directory, 0700)
}

// Sync bottom-up after every final transform, including empty bootstrap dirs.
// Do not follow the only allowed links; their physical targets are synced apart.
func syncRestoreTree(ctx context.Context, directory string) error {
	var dirs []string
	e := filepath.WalkDir(directory, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if e = ctx.Err(); e != nil {
			return e
		}
		if d.IsDir() {
			dirs = append(dirs, p)
		}
		return nil
	})
	if e != nil {
		return e
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		if e = syncRestoreDir(dirs[i]); e != nil {
			return e
		}
	}
	return syncRestoreDir(filepath.Dir(directory))
}
func syncRestoreDir(directory string) error {
	f, e := os.Open(directory)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
