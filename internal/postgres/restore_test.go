// Copyright 2026 cnpg_backup contributors. All rights reserved.
package postgres

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
)

func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func writeFixtureTar(t *testing.T, directory, name string, files map[string][]byte, dirs ...string) repository.Artifact {
	t.Helper()
	var b bytes.Buffer
	tw := tar.NewWriter(&b)
	for _, d := range dirs {
		if e := tw.WriteHeader(&tar.Header{Name: d + "/", Typeflag: tar.TypeDir, Mode: 0700, Format: tar.FormatUSTAR}); e != nil {
			t.Fatal(e)
		}
	}
	names := make([]string, 0, len(files))
	for p := range files {
		names = append(names, p)
	}
	sort.Strings(names)
	for _, p := range names {
		v := files[p]
		if e := tw.WriteHeader(&tar.Header{Name: p, Typeflag: tar.TypeReg, Size: int64(len(v)), Mode: 0600, Format: tar.FormatUSTAR}); e != nil {
			t.Fatal(e)
		}
		if _, e := tw.Write(v); e != nil {
			t.Fatal(e)
		}
	}
	if e := tw.Close(); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(directory, name), b.Bytes(), 0600); e != nil {
		t.Fatal(e)
	}
	return repository.Artifact{RawBytes: int64(b.Len()), StoredBytes: int64(b.Len()), RawSHA256: digest(b.Bytes()), StoredSHA256: digest(b.Bytes()), Compression: "none"}
}

func syntheticRestore(t *testing.T, tables bool) (*fullInput, repository.Commit, configuration.Native) {
	t.Helper()
	return syntheticRestoreWithFiles(t, tables, nil)
}

func syntheticRestoreWithFiles(t *testing.T, tables bool, extra map[string][]byte) (*fullInput, repository.Commit, configuration.Native) {
	t.Helper()
	scratch := t.TempDir()
	dir := filepath.Join(scratch, "tar")
	if e := os.Mkdir(dir, 0700); e != nil {
		t.Fatal(e)
	}
	label := "START WAL LOCATION: 0/100028 (file 000000010000000000000001)\nCHECKPOINT LOCATION: 0/100060\nBACKUP METHOD: streamed\nBACKUP FROM: primary\nSTART TIME: 2026-09-07 00:00:00 UTC\nLABEL: fixture\nSTART TIMELINE: 1\n"
	c := repository.Commit{Kind: "full", SystemIdentifier: "1234", Timeline: 1, StartLSN: "0/100028", StopLSN: "0/100120", BackupLabel: label, Tablespaces: []repository.Tablespace{}, WALRanges: []repository.WALRange{{Timeline: 1, StartLSN: "0/100028", EndLSN: "0/100120"}}}
	base := map[string][]byte{"PG_VERSION": []byte("18\n"), "global/pg_control": []byte("control"), "backup_label": []byte(label)}
	for p, b := range extra {
		base[p] = b
	}
	files := map[string][]byte{}
	for p, b := range base {
		files[p] = b
	}
	if tables {
		c.Tablespaces = []repository.Tablespace{{OID: 123, Name: "fast"}}
		c.TablespaceMap = "123 /var/lib/postgresql/tablespaces/fast/data\n"
		base["tablespace_map"] = []byte(c.TablespaceMap)
		files["tablespace_map"] = []byte(c.TablespaceMap)
		ts := map[string][]byte{"PG_18_202506291/5/42": []byte("tablespace payload")}
		for p, b := range ts {
			files["pg_tblspc/123/"+p] = b
		}
		a := writeFixtureTar(t, dir, "123.tar", ts, "PG_18_202506291/empty")
		a.Role = "tablespace"
		a.TablespaceOID = &c.Tablespaces[0].OID
		c.Artifacts = append(c.Artifacts, a)
	}
	a := writeFixtureTar(t, dir, "base.tar", base, "pg_wal/archive_status", "pg_wal/summaries", "pg_tblspc", "pg_stat_tmp")
	a.Role = "base"
	c.Artifacts = append(c.Artifacts, a)
	a = writeFixtureTar(t, dir, "pg_wal.tar", map[string][]byte{"000000010000000000000001": make([]byte, 1<<20), "archive_status/000000010000000000000001.done": {}}, "archive_status")
	a.Role = "wal"
	c.Artifacts = append(c.Artifacts, a)
	records := []manifestFile{}
	for p, b := range files {
		records = append(records, manifestFile{Path: p, Size: int64(len(b)), LastModified: "2026-09-07 00:00:00 UTC", ChecksumAlgorithm: "SHA256", Checksum: digest(b)})
	}
	manifest, e := json.Marshal(map[string]any{"PostgreSQL-Backup-Manifest-Version": 2, "System-Identifier": 1234, "Files": records, "WAL-Ranges": []map[string]any{{"Timeline": 1, "Start-LSN": "0/100028", "End-LSN": "0/100120"}}, "Manifest-Checksum": strings.Repeat("0", 64)})
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, "backup_manifest"), manifest, 0600); e != nil {
		t.Fatal(e)
	}
	c.ManifestBytes = int64(len(manifest))
	c.ManifestSHA256 = digest(manifest)
	for i := range c.Artifacts {
		c.Artifacts[i].Index = i
	}
	return &fullInput{directory: dir}, c, configuration.Native{MaxBackupBytes: 16 << 20, MaxBootstrapWALBytes: 4 << 20, MaxRestoredBytes: 16 << 20}
}

func TestRestoreOriginalDecodeExactLimits(t *testing.T) {
	raw := bytes.Repeat([]byte("compressible native bytes"), 1000)
	var gz bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&gz, 1)
	_, _ = zw.Write(raw)
	_ = zw.Close()
	for _, tc := range []struct {
		name, compression string
		stored            []byte
		rawSize           int64
		hash              string
		valid             bool
	}{
		{"none", "none", raw, int64(len(raw)), digest(raw), true},
		{"gzip", "gzip", gz.Bytes(), int64(len(raw)), digest(raw), true},
		{"expansion", "gzip", gz.Bytes(), int64(len(raw) - 1), digest(raw), false},
		{"wrong-raw-hash", "gzip", gz.Bytes(), int64(len(raw)), strings.Repeat("0", 64), false},
		{"truncated", "gzip", gz.Bytes()[:gz.Len()-1], int64(len(raw)), digest(raw), false},
		{"trailing", "gzip", append(append([]byte{}, gz.Bytes()...), 0), int64(len(raw)), digest(raw), false},
		{"concatenated", "gzip", append(append([]byte{}, gz.Bytes()...), gz.Bytes()...), int64(len(raw)), digest(raw), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src, e := os.CreateTemp(t.TempDir(), "source")
			if e != nil {
				t.Fatal(e)
			}
			defer src.Close()
			_, _ = src.Write(tc.stored)
			dst, e := os.CreateTemp(t.TempDir(), "raw")
			if e != nil {
				t.Fatal(e)
			}
			defer dst.Close()
			e = decodeOriginal(context.Background(), src, dst, s3store.Integrity{Size: int64(len(tc.stored)), SHA256: digest(tc.stored)}, s3store.Integrity{Size: tc.rawSize, SHA256: tc.hash}, tc.compression)
			if (e == nil) != tc.valid {
				t.Fatal(e)
			}
			st, _ := dst.Stat()
			if st.Size() > tc.rawSize+1 {
				t.Fatal("unbounded expansion")
			}
		})
	}
}

func TestRestoreScansOriginalInventoryAndBudgets(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*fullInput, *repository.Commit, *configuration.Native)
	}{
		{"valid", func(*fullInput, *repository.Commit, *configuration.Native) {}},
		{"wrong-label", func(_ *fullInput, c *repository.Commit, _ *configuration.Native) { c.BackupLabel += "unexpected" }},
		{"wrong-map", func(_ *fullInput, c *repository.Commit, _ *configuration.Native) { c.TablespaceMap = "123 /outside\n" }},
		{"wrong-system", func(_ *fullInput, c *repository.Commit, _ *configuration.Native) { c.SystemIdentifier = "9876" }},
		{"raw-budget", func(_ *fullInput, _ *repository.Commit, n *configuration.Native) { n.MaxBackupBytes = 1 }},
		{"wal-budget", func(_ *fullInput, _ *repository.Commit, n *configuration.Native) { n.MaxBootstrapWALBytes = 1 }},
		{"unmanifested", func(in *fullInput, c *repository.Commit, _ *configuration.Native) {
			a := writeFixtureTar(t, in.directory, "base.tar", map[string][]byte{"unexpected": []byte("not in native manifest")})
			a.Role = "base"
			for i := range c.Artifacts {
				if c.Artifacts[i].Role == "base" {
					a.Index = i
					c.Artifacts[i] = a
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in, c, n := syntheticRestore(t, true)
			tc.change(in, &c, &n)
			e := in.scan(context.Background(), c, 1<<20, n)
			if (e == nil) != (tc.name == "valid") {
				t.Fatal(e)
			}
		})
	}
}

func TestRestoreConfinedExtractionAttemptsEscapes(t *testing.T) {
	for _, h := range []*tar.Header{
		{Name: "../sentinel", Typeflag: tar.TypeReg, Size: 1},
		{Name: "/absolute", Typeflag: tar.TypeReg, Size: 1},
		{Name: "escape", Typeflag: tar.TypeSymlink, Linkname: "../"},
		{Name: "escape", Typeflag: tar.TypeLink, Linkname: "../sentinel"},
		{Name: "device", Typeflag: tar.TypeChar},
		{Name: "fifo", Typeflag: tar.TypeFifo},
		{Name: "sparse", Typeflag: tar.TypeGNUSparse},
		{Name: "a/./b", Typeflag: tar.TypeReg, Size: 1},
	} {
		t.Run(h.Name+string(h.Typeflag), func(t *testing.T) {
			parent := t.TempDir()
			sentinel := filepath.Join(parent, "sentinel")
			_ = os.WriteFile(sentinel, []byte("unchanged"), 0600)
			dst := filepath.Join(parent, "target")
			_ = os.Mkdir(dst, 0700)
			data := makeTar(t, h)
			dir := t.TempDir()
			_ = os.WriteFile(filepath.Join(dir, "base.tar"), data, 0600)
			in := fullInput{directory: dir}
			c := repository.Commit{Artifacts: []repository.Artifact{{Role: "base", RawBytes: int64(len(data))}}}
			if e := in.extractArchive(context.Background(), c, "base.tar", dst); e == nil {
				t.Fatal("escape accepted")
			}
			b, _ := os.ReadFile(sentinel)
			if string(b) != "unchanged" {
				t.Fatal("outside destination changed")
			}
		})
	}
	t.Run("existing-link", func(t *testing.T) {
		outside := t.TempDir()
		_ = os.WriteFile(filepath.Join(outside, "sentinel"), []byte("unchanged"), 0600)
		dst := t.TempDir()
		_ = os.Symlink(outside, filepath.Join(dst, "link"))
		root, e := os.OpenRoot(dst)
		if e != nil {
			t.Fatal(e)
		}
		defer root.Close()
		if e = copyMember(root, "link/sentinel", 1, strings.NewReader("x")); e == nil {
			t.Fatal("existing escape link followed")
		}
		b, _ := os.ReadFile(filepath.Join(outside, "sentinel"))
		if string(b) != "unchanged" {
			t.Fatal("outside changed")
		}
	})
}

func TestRestorePreservesDirectoriesAndCopiesWAL(t *testing.T) {
	in, c, n := syntheticRestore(t, true)
	ctx := context.Background()
	if e := in.scan(ctx, c, 1<<20, n); e != nil {
		t.Fatal(e)
	}
	target := t.TempDir()
	layout := RestoreLayout{PGDATA: filepath.Join(target, "pgdata"), WALDirectory: filepath.Join(target, "wal"), Tablespaces: map[string]string{"fast": filepath.Join(target, "ts")}}
	calls := []string{}
	run := func(ctx context.Context, tool string, args ...string) ([]byte, error) {
		calls = append(calls, tool+" "+strings.Join(args, " "))
		if tool == "pg_verifybackup" {
			b, e := os.ReadFile(filepath.Join(layout.PGDATA, "tablespace_map"))
			if e != nil || string(b) != c.TablespaceMap {
				t.Fatal("map changed before original verification", e)
			}
			if st, e := os.Lstat(layout.PGDATA + "/pg_wal"); e != nil || !st.IsDir() {
				t.Fatal("input WAL must be directory")
			}
		}
		return nil, nil
	}
	got, e := in.materialize(ctx, c, layout, run)
	if e != nil {
		t.Fatal(e)
	}
	if len(calls) != 2 || !strings.Contains(calls[0], "--no-parse-wal") || !strings.HasPrefix(calls[1], "pg_waldump ") {
		t.Fatal(calls)
	}
	if len(got) != 1 || got["000000010000000000000001"].SHA256 != digest(make([]byte, 1<<20)) {
		t.Fatal(got)
	}
	for _, p := range []string{layout.PGDATA + "/pg_stat_tmp", layout.WALDirectory + "/archive_status", layout.WALDirectory + "/summaries", layout.Tablespaces["fast"] + "/PG_18_202506291/empty"} {
		if st, e := os.Stat(p); e != nil || !st.IsDir() {
			t.Fatal("lost empty directory", p, e)
		}
	}
	if p, e := os.Readlink(layout.PGDATA + "/pg_wal"); e != nil || p != layout.WALDirectory {
		t.Fatal(p, e)
	}
	if p, e := os.Readlink(layout.PGDATA + "/pg_tblspc/123"); e != nil || p != layout.Tablespaces["fast"] {
		t.Fatal(p, e)
	}
	if _, e := os.Stat(layout.PGDATA + "/tablespace_map"); !os.IsNotExist(e) {
		t.Fatal("historical map retained")
	}
	b, e := os.ReadFile(layout.Tablespaces["fast"] + "/PG_18_202506291/5/42")
	if e != nil || string(b) != "tablespace payload" {
		t.Fatal(e)
	}
	if _, e = in.materialize(ctx, c, layout, run); e == nil {
		t.Fatal("partial target destructively reused")
	}
}

func TestRestoreNativeFailureAndCancellationLeaveTargets(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		in, c, n := syntheticRestore(t, true)
		if e := in.scan(context.Background(), c, 1<<20, n); e != nil {
			t.Fatal(e)
		}
		dir := t.TempDir()
		layout := RestoreLayout{PGDATA: dir + "/pgdata", WALDirectory: dir + "/wal", Tablespaces: map[string]string{"fast": dir + "/ts"}}
		ctx, cancel := context.WithCancel(context.Background())
		failure := errors.New("original native input corrupt")
		run := func(context.Context, string, ...string) ([]byte, error) {
			if cancelled {
				cancel()
				return nil, ctx.Err()
			}
			return nil, failure
		}
		_, e := in.materialize(ctx, c, layout, run)
		cancel()
		if cancelled && !errors.Is(e, context.Canceled) || !cancelled && !errors.Is(e, failure) {
			t.Fatal(e)
		}
		b, e := os.ReadFile(layout.PGDATA + "/tablespace_map")
		if e != nil || string(b) != c.TablespaceMap {
			t.Fatal("failure removed original map")
		}
		if st, e := os.Lstat(layout.PGDATA + "/pg_wal"); e != nil || !st.IsDir() {
			t.Fatal("failed verification transformed WAL")
		}
	}
}

func TestRestoreLayoutCapacityAndInvalidChain(t *testing.T) {
	l := RestoreLayout{PGDATA: liveData, WALDirectory: "/var/lib/postgresql/wal/pg_wal", Tablespaces: map[string]string{"fast": "/var/lib/postgresql/tablespaces/fast/data"}}
	tables := []repository.Tablespace{{OID: 123, Name: "fast"}}
	if e := validateRestoreLayout(l, tables); e != nil {
		t.Fatal(e)
	}
	l.Tablespaces["fast"] = "/outside/data"
	if e := validateRestoreLayout(l, tables); e == nil {
		t.Fatal("unmanaged target accepted")
	}
	if _, e := RestoreFull(context.Background(), nil, repository.Plan{Chain: []repository.Commit{{Kind: "full"}, {Kind: "differential"}}}, configuration.Native{}, nil, l); e == nil {
		t.Fatal(e)
	}
	inv := archiveInventory{files: map[string]int64{"a/b/file": 8193}, dirs: []string{"empty"}}
	allocated, nodes, e := inventoryAllocation(inv, 8192)
	if e != nil || nodes != 5 || allocated != 5*8192+16384 {
		t.Fatal(allocated, nodes, e)
	}
	set := map[string]configuration.FilesystemBudget{"/workspace": {Mount: "/workspace", LimitBytes: 16 << 30}, "/wal": {Mount: "/wal", LimitBytes: 2 << 30}}
	budgets := finishRestoreBudgets(set, map[string]int64{"/workspace": 10 << 30, "/wal": 1 << 30})
	for _, b := range budgets {
		if b.RequiredBytes != (map[string]int64{"/workspace": 11 << 30, "/wal": 2 << 30})[b.Mount] {
			t.Fatal(b)
		}
	}
}

func TestRestoreRefusesExistingTargetsWithoutCleanup(t *testing.T) {
	p := t.TempDir()
	target := p + "/data"
	_ = os.Mkdir(target, 0700)
	_ = os.WriteFile(target+"/sentinel", []byte("keep"), 0600)
	if e := emptyRestoreRoot(target); e == nil {
		t.Fatal("nonempty target accepted")
	}
	_ = os.Symlink(target, p+"/alias")
	if e := emptyRestoreRoot(p + "/alias"); e == nil {
		t.Fatal("target alias accepted")
	}
	_ = os.Symlink(p, p+"/parent-link")
	if e := emptyRestoreRoot(p + "/parent-link/new"); e == nil {
		t.Fatal("parent alias accepted")
	}
	b, _ := os.ReadFile(target + "/sentinel")
	if string(b) != "keep" {
		t.Fatal("partial target cleaned")
	}
}

func TestRestoreRejectsControlMismatchBeforeNativeVerifier(t *testing.T) {
	in, c, n := syntheticRestore(t, false)
	if e := in.scan(context.Background(), c, 1<<20, n); e != nil {
		t.Fatal(e)
	}
	good := "pg_control version number: 1800\nCatalog version number: 202506291\nDatabase block size: 8192\nBlocks per segment of large relation: 131072\nWAL block size: 8192\nDatabase system identifier: 1234\nBytes per WAL segment: 1048576\nData page checksum version: 0\nLatest checkpoint's TimeLineID: 1\n"
	for _, output := range []string{
		strings.ReplaceAll(good, "identifier: 1234", "identifier: 9999"),
		strings.ReplaceAll(good, "segment: 1048576", "segment: 16777216"),
		strings.ReplaceAll(good, "TimeLineID: 1", "TimeLineID: 2"),
		strings.ReplaceAll(good, "checksum version: 0", "checksum version: 1"),
		strings.ReplaceAll(good, "number: 1800", "number: 1700"),
	} {
		calls := 0
		run := func(_ context.Context, tool string, _ ...string) ([]byte, error) {
			calls++
			if tool != "pg_controldata" {
				t.Fatal("mismatch reached verifier")
			}
			return []byte(output), nil
		}
		if e := in.verify(context.Background(), c, 1<<20, run); e == nil || calls != 1 {
			t.Fatal(e, calls)
		}
	}
}

func TestRestorePerFilesystemPeakAccounting(t *testing.T) {
	in, c, n := syntheticRestore(t, true)
	if e := in.scan(context.Background(), c, 1<<20, n); e != nil {
		t.Fatal(e)
	}
	layout := RestoreLayout{PGDATA: liveData, WALDirectory: "/var/lib/postgresql/wal/pg_wal", Tablespaces: map[string]string{"fast": "/var/lib/postgresql/tablespaces/fast/data"}}
	var budgets []configuration.FilesystemBudget
	units := map[string]int64{}
	for _, mount := range restoreMounts(layout) {
		budgets = append(budgets, configuration.FilesystemBudget{Mount: mount, LimitBytes: 4 << 30})
		units[mount] = 8192
	}
	units["/var/lib/postgresql/wal"] = 65536
	// Completed compressed bytes are independently counted; high compression
	// never discounts raw downloads, extracted originals or the second WAL copy.
	raw, stored, wal := c.ManifestBytes, c.ManifestBytes, int64(0)
	for i := range c.Artifacts {
		c.Artifacts[i].Compression = "gzip"
		c.Artifacts[i].StoredBytes = 2048
		raw += c.Artifacts[i].RawBytes
		stored += 2048
		if c.Artifacts[i].Role == "wal" {
			wal = c.Artifacts[i].RawBytes
		}
	}
	result, e := restoreOutputBudget(in, c, n, budgets, layout, units)
	if e != nil {
		t.Fatal(e)
	}
	// Independent counts for this fixture: base has 4 files+7 dirs, WAL has
	// 2 files+2 dirs, tablespace has 1 file+4 dirs. All tiny files round up.
	want := map[string]int64{
		workspace:                              raw + stored + wal + 8192*(maxEntries+256) + configuration.GiB,
		"/var/lib/postgresql/data":             1286144 + configuration.GiB,
		"/var/lib/postgresql/wal":              1572864 + configuration.GiB,
		"/var/lib/postgresql/tablespaces/fast": 49152 + configuration.GiB,
	}
	for _, b := range result {
		if b.RequiredBytes != want[b.Mount] {
			t.Fatalf("%s: got %d want %d", b.Mount, b.RequiredBytes, want[b.Mount])
		}
	}
	for _, bad := range []configuration.Native{
		{MaxBackupBytes: 1, MaxBootstrapWALBytes: 1, MaxRestoredBytes: 1},
		{MaxBackupBytes: n.MaxBackupBytes, MaxBootstrapWALBytes: 1, MaxRestoredBytes: n.MaxRestoredBytes},
		{MaxBackupBytes: n.MaxBackupBytes, MaxBootstrapWALBytes: n.MaxBootstrapWALBytes, MaxRestoredBytes: 1},
	} {
		if _, e = restoreOutputBudget(in, c, bad, budgets, layout, units); e == nil {
			t.Fatal("native hard size budget ignored")
		}
	}
	if _, e = restoreOutputBudget(in, c, n, budgets[:len(budgets)-1], layout, units); e == nil {
		t.Fatal("missing physical volume budget accepted")
	}
	duplicate := append([]configuration.FilesystemBudget(nil), budgets...)
	duplicate[1] = duplicate[0]
	if _, e = restoreOutputBudget(in, c, n, duplicate, layout, units); e == nil {
		t.Fatal("duplicate physical volume accepted")
	}
}

func TestRestoreWALCopyFailurePreservesOriginalDirectory(t *testing.T) {
	in, c, n := syntheticRestore(t, false)
	ctx := context.Background()
	if e := in.scan(ctx, c, 1<<20, n); e != nil {
		t.Fatal(e)
	}
	pg := t.TempDir()
	if e := os.Mkdir(pg+"/pg_wal", 0700); e != nil {
		t.Fatal(e)
	}
	if e := in.extractArchive(ctx, c, "pg_wal.tar", pg+"/pg_wal"); e != nil {
		t.Fatal(e)
	}
	root, e := os.OpenRoot(pg)
	if e != nil {
		t.Fatal(e)
	}
	defer root.Close()
	inv := in.archives["pg_wal.tar"]
	expected, e := bundleIntegrity(ctx, pg+"/pg_wal", inv)
	if e != nil {
		t.Fatal(e)
	}
	// A failed exclusive copy must not remove or replace our verified original.
	dst := t.TempDir()
	if e = os.WriteFile(dst+"/000000010000000000000001", []byte("existing"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = relocateRestoreWAL(ctx, root, dst, inv, expected); e == nil {
		t.Fatal("overwrote WAL destination")
	}
	st, e := os.Lstat(pg + "/pg_wal")
	if e != nil || !st.IsDir() {
		t.Fatal("removed original before complete copy")
	}
	b, _ := os.ReadFile(dst + "/000000010000000000000001")
	if string(b) != "existing" {
		t.Fatal("partial target clobbered")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if e = relocateRestoreWAL(cancelled, root, t.TempDir(), inv, expected); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if st, e = os.Lstat(pg + "/pg_wal"); e != nil || !st.IsDir() {
		t.Fatal("cancellation removed original WAL")
	}
}
