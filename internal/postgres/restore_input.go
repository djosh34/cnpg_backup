// Copyright 2026 cnpg_backup contributors. All rights reserved.
package postgres

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
)

type fullInput struct {
	directory string
	manifest  NativeManifest
	archives  map[string]archiveInventory
	walCheck  string
	metadata  string
}

func artifactName(a repository.Artifact) string {
	switch a.Role {
	case "base":
		return "base.tar"
	case "wal":
		return "pg_wal.tar"
	case "tablespace":
		if a.TablespaceOID != nil {
			return strconv.FormatUint(uint64(*a.TablespaceOID), 10) + ".tar"
		}
	}
	return ""
}

func downloadFull(ctx context.Context, hold *repository.Hold, c repository.Commit, scratch string) (*fullInput, error) {
	dir := filepath.Join(scratch, "tar")
	if e := os.Mkdir(dir, 0700); e != nil {
		return nil, e
	}
	download := func(index int, name string, stored, raw s3store.Integrity, compression string) error {
		f, e := os.CreateTemp(scratch, "download-")
		if e != nil {
			return e
		}
		defer f.Close()
		// Open inode survives but a crashed process cannot accumulate stored spools.
		if e = os.Remove(f.Name()); e != nil {
			return e
		}
		if e = hold.DownloadInput(ctx, c.BackupUID, index, f); e != nil {
			return e
		}
		dst, e := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		e = decodeOriginal(ctx, f, dst, stored, raw, compression)
		ce := dst.Close()
		if e != nil {
			return e
		}
		return ce
	}
	m := s3store.Integrity{Size: c.ManifestBytes, SHA256: c.ManifestSHA256}
	if e := download(-1, "backup_manifest", m, m, "none"); e != nil {
		return nil, e
	}
	for i, a := range c.Artifacts {
		name := artifactName(a)
		if name == "" {
			return nil, ErrInput
		}
		if e := download(i, name, s3store.Integrity{Size: a.StoredBytes, SHA256: a.StoredSHA256}, s3store.Integrity{Size: a.RawBytes, SHA256: a.RawSHA256}, a.Compression); e != nil {
			return nil, e
		}
	}
	return &fullInput{directory: dir}, nil
}

// Bind the actual download to the persisted plan as well as the repository's
// current winning commit. Consume the entire single gzip stream including CRC;
// no trailing member/payload or expansion past committed length is accepted.
func decodeOriginal(ctx context.Context, src, dst *os.File, stored, raw s3store.Integrity, compression string) error {
	st, e := src.Stat()
	if e != nil || st.Size() != stored.Size {
		return ErrInput
	}
	hash, e := fileHash(ctx, src)
	if e != nil {
		return e
	}
	if hash != stored.SHA256 {
		return ErrInput
	}
	br := bufio.NewReader(contextInput{ctx, io.NewSectionReader(src, 0, stored.Size)})
	var r io.Reader = br
	var gz *gzip.Reader
	switch compression {
	case "none":
	case "gzip":
		gz, e = gzip.NewReader(br)
		if e != nil {
			return ErrInput
		}
		defer gz.Close()
		gz.Multistream(false)
		r = gz
	default:
		return ErrInput
	}
	h := sha256.New()
	n, e := io.CopyBuffer(io.MultiWriter(dst, h), io.LimitReader(r, raw.Size+1), make([]byte, 128<<10))
	if e != nil {
		return e
	}
	if n != raw.Size || hex.EncodeToString(h.Sum(nil)) != raw.SHA256 {
		return ErrInput
	}
	if _, e = br.ReadByte(); e != io.EOF {
		return ErrInput
	}
	return dst.Sync()
}

// scan bounds authenticated originals by their raw input budgets. Historical F/D
// inputs may exceed the output cap after DROP/TRUNCATE; callers enforce that cap
// on full-only output admission or on the reconstructed synthetic tree.
func (in *fullInput) scan(ctx context.Context, c repository.Commit, segment int64, n configuration.Native) (err error) {
	phase := "manifest"
	defer func() {
		if err != nil {
			err = fmt.Errorf("%s: %w", phase, err)
		}
	}()
	f, e := os.Open(filepath.Join(in.directory, "backup_manifest"))
	if e != nil {
		return e
	}
	st, e := f.Stat()
	if e != nil || st.Size() != c.ManifestBytes {
		f.Close()
		return ErrInput
	}
	hash, e := fileHash(ctx, f)
	if e != nil || hash != c.ManifestSHA256 {
		f.Close()
		return ErrInput
	}
	in.manifest, e = ScanManifest(contextInput{ctx, f})
	f.Close()
	if e != nil {
		return e
	}
	if strconv.FormatUint(in.manifest.SystemIdentifier, 10) != c.SystemIdentifier || !reflect.DeepEqual(in.manifest.Ranges, c.WALRanges) {
		return ErrInput
	}
	in.archives = map[string]archiveInventory{}
	actual := map[string]int64{}
	entries, raw := 0, c.ManifestBytes
	for _, a := range c.Artifacts {
		phase = "archive headers and inventory"
		name := artifactName(a)
		if name == "" {
			return ErrInput
		}
		f, e := os.Open(filepath.Join(in.directory, name))
		if e != nil {
			return e
		}
		st, e := f.Stat()
		if e != nil || st.Size() != a.RawBytes {
			f.Close()
			return ErrInput
		}
		hash, e := fileHash(ctx, f)
		if e != nil || hash != a.RawSHA256 {
			f.Close()
			return ErrInput
		}
		inv, e := scanArchive(ctx, f, a.RawBytes, func(p string, size int64, r io.Reader) error {
			if a.Role == "wal" {
				if strings.HasPrefix(p, "archive_status/") && strings.HasSuffix(p, ".done") {
					base := strings.TrimSuffix(strings.TrimPrefix(p, "archive_status/"), ".done")
					if size != 0 || len(base) != 24 || !repository.ValidWALFilename(base) {
						return ErrInput
					}
					return nil
				}
				if len(p) != 24 || !repository.ValidWALFilename(p) || size != segment {
					return ErrInput
				}
				return nil
			}
			full := p
			if a.Role == "tablespace" {
				full = "pg_tblspc/" + strings.TrimSuffix(name, ".tar") + "/" + p
			}
			// Base cannot supply anything inside the trusted tablespace/WAL roots.
			if a.Role == "base" && (strings.HasPrefix(p, "pg_tblspc/") || strings.HasPrefix(p, "pg_wal/")) {
				return ErrInput
			}
			if _, exists := actual[full]; exists {
				return ErrInput
			}
			actual[full] = size
			return nil
		})
		f.Close()
		if e != nil {
			return e
		}
		// Empty directories are preserved, but never let a base archive precreate an
		// OID destination later replaced by a trusted link.
		for _, d := range inv.dirs {
			if a.Role == "base" && strings.HasPrefix(d, "pg_tblspc/") {
				return ErrInput
			}
			if a.Role == "base" && strings.HasPrefix(d, "pg_wal/") && d != "pg_wal/archive_status" && d != "pg_wal/summaries" {
				return ErrInput
			}
			if a.Role == "wal" && d != "archive_status" && d != "summaries" {
				return ErrInput
			}
		}
		entries += inv.entries
		raw += a.RawBytes
		if entries > maxEntries || raw > n.MaxBackupBytes || a.Role == "wal" && a.RawBytes > n.MaxBootstrapWALBytes {
			return ErrInput
		}
		if _, e = nativeDirectories(inv); e != nil {
			return e
		}
		in.archives[name] = inv
	}
	phase = "manifest inventory equality"
	for p, size := range actual {
		if p == "backup_label" || p == "tablespace_map" {
			if size > 64<<10 {
				return ErrInput
			}
			continue
		}
		if size2, ok := in.manifest.Files[p]; !ok || size2 != size {
			return ErrInput
		}
	}
	for p, size := range in.manifest.Files {
		if size2, ok := actual[p]; !ok || size2 != size {
			return ErrInput
		}
	}
	// Check layout metadata using bounded Go reads, BEFORE any native invocation.
	phase = "metadata extraction"
	in.metadata = filepath.Join(filepath.Dir(in.directory), "metadata")
	in.walCheck = filepath.Join(filepath.Dir(in.directory), "walcheck")
	if e = os.Mkdir(in.metadata, 0700); e != nil {
		return e
	}
	if e = os.Mkdir(in.walCheck, 0700); e != nil {
		return e
	}
	meta, e := os.OpenRoot(in.metadata)
	if e != nil {
		return e
	}
	defer meta.Close()
	f, e = os.Open(filepath.Join(in.directory, "base.tar"))
	if e != nil {
		return e
	}
	_, e = scanArchive(ctx, f, in.archiveBytes(c, "base.tar"), func(p string, size int64, r io.Reader) error {
		if p == "backup_label" || p == "tablespace_map" || p == "global/pg_control" {
			if size > 64<<10 {
				return ErrInput
			}
			return copyMember(meta, p, size, r)
		}
		return nil
	})
	f.Close()
	if e != nil {
		return e
	}
	phase = "backup label"
	label, e := os.ReadFile(filepath.Join(in.metadata, "backup_label"))
	if e != nil || string(label) != c.BackupLabel {
		return ErrInput
	}
	if _, e = parseLabel(&c, segment); e != nil {
		return e
	}
	phase = "tablespace map"
	tableMap, e := os.ReadFile(filepath.Join(in.metadata, "tablespace_map"))
	if len(c.Tablespaces) == 0 {
		if (e != nil && !os.IsNotExist(e)) || len(tableMap) != 0 || c.TablespaceMap != "" {
			return ErrInput
		}
	} else {
		if e != nil || string(tableMap) != c.TablespaceMap {
			return ErrInput
		}
		conn := Connection{Tablespaces: map[string]string{}}
		for _, ts := range c.Tablespaces {
			conn.Tablespaces[ts.Name] = "/var/lib/postgresql/tablespaces/" + ts.Name + "/data"
		}
		if e = validateTablespaceMap(c.TablespaceMap, c.Tablespaces, conn); e != nil {
			return e
		}
	}
	phase = "WAL extraction"
	return in.extractArchive(ctx, c, "pg_wal.tar", in.walCheck)
}

func (in *fullInput) archiveBytes(c repository.Commit, name string) int64 {
	for _, a := range c.Artifacts {
		if artifactName(a) == name {
			return a.RawBytes
		}
	}
	return 0
}

// Every archive has already been scanned in full. All writes remain confined
// even if an archive/path were changed unexpectedly; links are not accepted.
func (in *fullInput) extractArchive(ctx context.Context, c repository.Commit, name, directory string) error {
	root, e := os.OpenRoot(directory)
	if e != nil {
		return e
	}
	defer root.Close()
	f, e := os.Open(filepath.Join(in.directory, name))
	if e != nil {
		return e
	}
	defer f.Close()
	inv, e := scanArchive(ctx, f, in.archiveBytes(c, name), func(p string, size int64, r io.Reader) error { return copyMember(root, p, size, r) })
	if e != nil {
		return e
	}
	for _, d := range inv.dirs {
		if e = ctx.Err(); e != nil {
			return e
		}
		if e = root.MkdirAll(d, 0700); e != nil {
			return e
		}
	}
	return nil
}

type restoreRunner func(context.Context, string, ...string) ([]byte, error)

func nativeVersion(b []byte, tool string) bool {
	return strings.HasPrefix(string(b), tool+" (PostgreSQL) 18.6 ") || string(b) == tool+" (PostgreSQL) 18.6\n"
}

func verifyRestoreWAL(ctx context.Context, dir string, ranges []repository.WALRange, run restoreRunner) error {
	if len(ranges) != 1 {
		return ErrInput
	}
	for _, r := range ranges {
		a, e := repository.ParseLSN(r.StartLSN)
		b, e2 := repository.ParseLSN(r.EndLSN)
		if e != nil || e2 != nil || r.Timeline == 0 || a >= b {
			return ErrInput
		}
		if _, e = run(ctx, "pg_waldump", "--quiet", "--path="+dir, "--timeline="+strconv.FormatUint(uint64(r.Timeline), 10), "--start="+r.StartLSN, "--end="+r.EndLSN); e != nil {
			return e
		}
	}
	return nil
}
func (in *fullInput) verify(ctx context.Context, c repository.Commit, segment int64, run restoreRunner) error {
	b, e := run(ctx, "pg_controldata", in.metadata)
	if e != nil {
		return e
	}
	control, e := parseCaptureControl(string(b))
	if e != nil {
		return e
	}
	if control != (captureControl{System: c.SystemIdentifier, Segment: segment, Timeline: c.Timeline, Checksum: c.ChecksumVersion}) {
		return ErrInput
	}
	if _, e = run(ctx, "pg_verifybackup", "--exit-on-error", "--no-parse-wal", in.directory); e != nil {
		return e
	}
	return verifyRestoreWAL(ctx, in.walCheck, in.manifest.Ranges, run)
}

// nativeDirectories accounts for implicit parents as well as explicit empty
// directories. Limit materialized nodes too, not only the tar header count.
func nativeDirectories(inv archiveInventory) (map[string]bool, error) {
	dirs := map[string]bool{".": true}
	add := func(p string) error {
		for p != "." {
			dirs[p] = true
			if len(dirs)+len(inv.files) > maxEntries {
				return ErrInput
			}
			p = path.Dir(p)
		}
		return nil
	}
	for _, d := range inv.dirs {
		if e := add(d); e != nil {
			return nil, e
		}
	}
	for p := range inv.files {
		if e := add(path.Dir(p)); e != nil {
			return nil, e
		}
	}
	return dirs, nil
}
