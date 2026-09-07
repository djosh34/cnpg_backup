// Copyright 2026 cnpg_backup contributors. All rights reserved.
package postgres

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"golang.org/x/sys/unix"
)

const workspace = "/cnpg-backup/work"
const liveData = "/var/lib/postgresql/data/pgdata"

// Capture owns one immutable certificate/config snapshot and private operation
// directory. The RPC owns the per-instance slot and the repository holder.
type Capture struct {
	Snapshot   *configuration.CaptureSnapshot
	Connection Connection
	Directory  string
	env        []string
	before     serverState
	control    captureControl
	budgets    []configuration.FilesystemBudget
	peak       int64
}
type captureControl struct {
	System   string
	Segment  int64
	Timeline uint32
	Checksum int
}

func controlAt(ctx context.Context, directory string) (captureControl, error) {
	b, e := command(ctx, []string{"LANG=C", "LC_ALL=C"}, "pg_controldata", directory)
	if e != nil {
		return captureControl{}, e
	}
	if e = validateControl(string(b)); e != nil {
		return captureControl{}, e
	}
	fields := map[string]string{}
	for _, line := range strings.Split(string(b), "\n") {
		p := strings.SplitN(line, ":", 2)
		if len(p) == 2 {
			fields[strings.TrimSpace(p[0])] = strings.TrimSpace(p[1])
		}
	}
	if fields["pg_control version number"] != "1800" || fields["Catalog version number"] != "202506291" {
		return captureControl{}, ErrInput
	}
	c := captureControl{System: fields["Database system identifier"]}
	c.Segment, _ = strconv.ParseInt(fields["Bytes per WAL segment"], 10, 64)
	t, e := strconv.ParseUint(fields["Latest checkpoint's TimeLineID"], 10, 32)
	if e != nil || t == 0 {
		return c, ErrInput
	}
	c.Timeline = uint32(t)
	c.Checksum, _ = strconv.Atoi(fields["Data page checksum version"])
	return c, nil
}

// OpenCapture rejects unsupported mounts and reserves native peak PLUS a
// separate readback spool for repository.Publish's serial remote verification.
// WAL callbacks retain their two independent spool budgets throughout capture.
func OpenCapture(ctx context.Context, root *os.Root) (*Capture, error) {
	s, e := configuration.LoadCaptureSnapshotFromRoot(root)
	if e != nil {
		return nil, e
	}
	c := &Capture{Snapshot: s}
	b, e := configuration.Read(root, "native/connection.json", 64<<10)
	if e != nil {
		return nil, e
	}
	if configuration.StrictJSON(b, &c.Connection) != nil || !validHost(c.Connection.Host) || len(c.Connection.Tablespaces) > 64 {
		return nil, ErrInput
	}
	b, e = configuration.Read(root, "capacity.json", 64<<10)
	if e != nil {
		return nil, e
	}
	if configuration.StrictJSON(b, &c.budgets) != nil {
		return nil, ErrInput
	}
	// The first control read is local only; authenticated SQL below must agree.
	c.control, e = controlAt(ctx, liveData)
	if e != nil {
		return nil, e
	}
	c.peak, e = configuration.CaptureCapacity(s.Repository.Spec.Native)
	if e != nil {
		return nil, e
	}
	a := s.Repository.Spec.Native.MaxBackupBytes
	c.peak += a + max(configuration.GiB, (a+99)/100) + 6*(c.control.Segment+(1<<20)) + (16 << 20)
	found := false
	wal := liveData + "/pg_wal"
	for i := range c.budgets {
		c.budgets[i].RequiredBytes = 0
		if c.budgets[i].Mount == workspace {
			c.budgets[i].RequiredBytes = c.peak
			found = true
		}
		if c.budgets[i].Mount == "/var/lib/postgresql/wal" {
			wal = "/var/lib/postgresql/wal/pg_wal"
		}
	}
	if !found {
		return nil, ErrInput
	}
	if e = configuration.CheckCapacity(c.budgets); e != nil {
		return nil, e
	}
	if actual, e := filepath.EvalSymlinks(liveData + "/pg_wal"); e != nil || actual != wal {
		return nil, ErrInput
	}
	for _, p := range append([]string{liveData, wal}, mapValues(c.Connection.Tablespaces)...) {
		if actual, e := filepath.EvalSymlinks(p); e != nil || actual != p {
			return nil, ErrInput
		}
	}
	c.Directory, e = os.MkdirTemp(workspace, "capture-")
	if e != nil {
		return nil, e
	}
	success := false
	defer func() {
		if !success {
			c.Close()
		}
	}()
	for name, value := range map[string][]byte{"client.crt": s.Certificate, "client.key": s.Key, "server-ca.crt": s.ServerCA} {
		if e = os.WriteFile(filepath.Join(c.Directory, name), value, 0600); e != nil {
			return nil, e
		}
	}
	service := "[local]\nhost=" + c.Connection.Host + "\nhostaddr=127.0.0.1\nport=5432\nuser=streaming_replica\ndbname=postgres\nsslmode=verify-full\nconnect_timeout=10\nsslcert=" + c.Directory + "/client.crt\nsslkey=" + c.Directory + "/client.key\nsslrootcert=" + c.Directory + "/server-ca.crt\n"
	if e = os.WriteFile(filepath.Join(c.Directory, "service.conf"), []byte(service), 0600); e != nil {
		return nil, e
	}
	c.env = []string{"LANG=C", "LC_ALL=C", "HOME=/nonexistent", "PGSERVICE=local", "PGSERVICEFILE=" + c.Directory + "/service.conf", "PGPASSFILE=/nonexistent", "PGOPTIONS=-c search_path=pg_catalog -c statement_timeout=30000", "PGAPPNAME=cnpg-backup"}
	for _, tool := range []string{"pg_basebackup", "pg_verifybackup", "pg_waldump", "pg_controldata", "psql"} {
		b, e := command(ctx, c.env, tool, "--version")
		if e != nil || !strings.HasPrefix(string(b), tool+" (PostgreSQL) 18.6") {
			return nil, errors.New("native tool version mismatch")
		}
	}
	c.before, e = c.state(ctx)
	if e != nil {
		return nil, e
	}
	after, e := controlAt(ctx, liveData)
	if e != nil || after != c.control {
		return nil, errors.New("source control changed during preflight")
	}
	success = true
	return c, nil
}
func mapValues(m map[string]string) []string {
	v := make([]string, 0, len(m))
	for _, s := range m {
		v = append(v, s)
	}
	return v
}
func (c *Capture) Close() {
	if c.Directory != "" {
		_ = os.RemoveAll(c.Directory)
	}
}
func (c *Capture) Identity(repositoryID, clusterUID string) repository.Identity {
	return repository.Identity{Schema: 1, RepositoryID: repositoryID, PostgresMajor: 18, SystemIdentifier: c.control.System, WALSegmentBytes: c.control.Segment, WriterClusterUID: clusterUID}
}
func (c *Capture) state(ctx context.Context) (serverState, error) {
	b, e := command(ctx, c.env, "psql", "-X", "-A", "-t", "--no-password", "-v", "ON_ERROR_STOP=1", "-c", preflightSQL)
	if e != nil {
		return serverState{}, e
	}
	var s serverState
	if json.Unmarshal(b, &s) != nil {
		return s, ErrInput
	}
	if e = validateState(s, c.Connection, c.Snapshot.Repository.Spec.Native); e != nil {
		return s, e
	}
	pm, e := time.Parse(time.RFC3339Nano, s.Postmaster)
	now, e2 := time.Parse(time.RFC3339Nano, s.Clock)
	if e != nil || e2 != nil || now.Before(pm) || len(s.TablespaceOIDs) != len(s.Tablespaces) {
		return s, ErrInput
	}
	s.Postmaster = pm.UTC().Format(time.RFC3339Nano)
	s.Clock = now.UTC().Format(time.RFC3339Nano)
	return s, nil
}
func (c *Capture) Postflight(ctx context.Context) (string, error) {
	s, e := c.state(ctx)
	if e != nil {
		return "", e
	}
	clock := s.Clock
	s.Clock = c.before.Clock
	if !reflect.DeepEqual(s, c.before) {
		return "", errors.New("capture source changed (restart, role, settings or tablespaces)")
	}
	control, e := controlAt(ctx, liveData)
	if e != nil || control != c.control {
		return "", errors.New("capture physical identity changed")
	}
	before, _ := time.Parse(time.RFC3339Nano, c.before.Clock)
	after, _ := time.Parse(time.RFC3339Nano, clock)
	if after.Before(before) {
		return "", errors.New("source clock moved backwards")
	}
	return clock, nil
}

type Captured struct {
	Commit   repository.Commit
	Manifest *os.File
	Files    []*os.File
}

func (r *Captured) Close() {
	if r.Manifest != nil {
		_ = r.Manifest.Close()
	}
	for _, f := range r.Files {
		_ = f.Close()
	}
}

func (c *Capture) Full(ctx context.Context, podUID string) (result *Captured, err error) {
	phase := "basebackup"
	defer func() {
		if err != nil {
			slog.Warn("native full failed", "phase", phase)
		}
	}()
	duration, _ := time.ParseDuration(c.Snapshot.Repository.Spec.Native.CaptureTimeout)
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	done := make(chan struct{})
	monitored := make(chan error, 1)
	go func() { monitored <- c.monitor(ctx, done, cancel) }()
	defer func() {
		close(done)
		me := <-monitored
		if err == nil && me != nil {
			if result != nil {
				result.Close()
				result = nil
			}
			err = me
		}
	}()
	dir := filepath.Join(c.Directory, "tar")
	if _, err = runTool(ctx, c.env, "pg_basebackup", "--no-password", "--pgdata="+dir, "--format=tar", "--wal-method=stream", "--checkpoint=spread", "--manifest-checksums=SHA256"); err != nil {
		return nil, err
	}
	phase = "postflight"
	stopped, err := c.Postflight(ctx)
	if err != nil {
		return nil, err
	}
	phase = "manifest"
	r := &Captured{}
	defer func() {
		if result == nil {
			r.Close()
		}
	}()
	r.Manifest, err = os.Open(filepath.Join(dir, "backup_manifest"))
	if err != nil {
		return nil, err
	}
	st, err := r.Manifest.Stat()
	if err != nil || st.Size() > repository.MaxManifestBytes {
		return nil, ErrInput
	}
	manifest, err := ScanManifest(contextInput{ctx, r.Manifest})
	if err != nil || strconv.FormatUint(manifest.SystemIdentifier, 10) != c.control.System || manifest.Ranges[0].Timeline != c.control.Timeline {
		return nil, ErrInput
	}
	range0 := manifest.Ranges[0]
	cm := repository.Commit{Schema: 1, Kind: "full", SystemIdentifier: c.control.System, PostgresMajor: 18, ToolVersion: "18.6", Timeline: c.control.Timeline, ChecksumVersion: c.control.Checksum, CaptureInstanceUID: podUID, PostmasterStartedAt: c.before.Postmaster, StoppedAt: stopped, StartLSN: range0.StartLSN, StopLSN: range0.EndLSN, RedoLSN: range0.StartLSN, BundledWALStartLSN: range0.StartLSN, BundledWALEndLSN: range0.EndLSN, WALRanges: manifest.Ranges, Tablespaces: []repository.Tablespace{}, Artifacts: []repository.Artifact{}, ManifestBytes: st.Size()}
	cm.ManifestSHA256, err = fileHash(ctx, r.Manifest)
	if err != nil {
		return nil, err
	}
	names := []string{"base.tar", "pg_wal.tar"}
	for name, oid := range c.before.TablespaceOIDs {
		if oid == 0 {
			return nil, ErrInput
		}
		cm.Tablespaces = append(cm.Tablespaces, repository.Tablespace{OID: oid, Name: name})
	}
	sort.Slice(cm.Tablespaces, func(i, j int) bool { return cm.Tablespaces[i].OID < cm.Tablespaces[j].OID })
	for _, ts := range cm.Tablespaces {
		names = append(names, strconv.FormatUint(uint64(ts.OID), 10)+".tar")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != len(names)+1 {
		return nil, ErrInput
	}
	for _, entry := range entries {
		if entry.Type() != 0 {
			return nil, ErrInput
		}
	}
	walDir := filepath.Join(c.Directory, "walcheck")
	metaDir := filepath.Join(c.Directory, "metadata")
	if os.Mkdir(walDir, 0700) != nil || os.Mkdir(metaDir, 0700) != nil {
		return nil, ErrInput
	}
	walRoot, err := os.OpenRoot(walDir)
	if err != nil {
		return nil, err
	}
	defer walRoot.Close()
	metaRoot, err := os.OpenRoot(metaDir)
	if err != nil {
		return nil, err
	}
	defer metaRoot.Close()
	phase = "archive-scan"
	total, entryCount := cm.ManifestBytes, 0
	actual := map[string]int64{}
	for i, name := range names {
		f, e := os.Open(filepath.Join(dir, name))
		if e != nil {
			return nil, e
		}
		st, e := f.Stat()
		if e != nil {
			f.Close()
			return nil, e
		}
		total += st.Size()
		limit := c.Snapshot.Repository.Spec.Native.MaxBackupBytes
		if total > limit {
			f.Close()
			return nil, ErrInput
		}
		if i == 1 {
			limit = c.Snapshot.Repository.Spec.Native.MaxBootstrapWALBytes
			if st.Size() > limit {
				f.Close()
				return nil, ErrInput
			}
		}
		inv, e := scanArchive(ctx, f, st.Size(), func(p string, size int64, input io.Reader) error {
			if i == 1 {
				if p == "archive_status" || strings.HasPrefix(p, "archive_status/") {
					if size != 0 || !strings.HasSuffix(p, ".done") {
						return ErrInput
					}
					return nil
				}
				if !repository.ValidWALFilename(p) || strings.Contains(p, "/") || len(p) != 24 || size != c.control.Segment {
					return ErrInput
				}
				return copyMember(walRoot, p, size, input)
			}
			full := p
			if i >= 2 {
				full = "pg_tblspc/" + strings.TrimSuffix(name, ".tar") + "/" + p
			}
			if _, exists := actual[full]; exists {
				return ErrInput
			}
			actual[full] = size
			if i == 0 && (p == "backup_label" || p == "tablespace_map" || p == "global/pg_control") {
				if size > 64<<10 {
					return ErrInput
				}
				return copyMember(metaRoot, p, size, input)
			}
			return nil
		})
		f.Close()
		if e != nil {
			return nil, e
		}
		entryCount += inv.entries
		if entryCount > maxEntries {
			return nil, ErrInput
		}
	}
	// The native verifier ignores backup_label and WAL. Admit only the exact
	// sanctioned metadata exceptions; all other regular data must be manifested.
	phase = "manifest-inventory"
	for p, size := range actual {
		if p == "backup_label" || p == "tablespace_map" {
			continue
		}
		if n, ok := manifest.Files[p]; !ok || n != size {
			return nil, ErrInput
		}
	}
	for p, size := range manifest.Files {
		if n, ok := actual[p]; !ok || n != size {
			return nil, ErrInput
		}
	}
	phase = "label-control"
	label, err := os.ReadFile(filepath.Join(metaDir, "backup_label"))
	if err != nil {
		return nil, err
	}
	cm.BackupLabel = string(label)
	if err = parseLabel(&cm, c.control.Segment); err != nil {
		return nil, err
	}
	if len(cm.Tablespaces) > 0 {
		b, e := os.ReadFile(filepath.Join(metaDir, "tablespace_map"))
		if e != nil {
			return nil, e
		}
		cm.TablespaceMap = string(b)
		if e = validateTablespaceMap(cm.TablespaceMap, cm.Tablespaces, c.Connection); e != nil {
			return nil, e
		}
	} else if _, e := os.Stat(filepath.Join(metaDir, "tablespace_map")); !os.IsNotExist(e) {
		return nil, ErrInput
	}
	captured, e := controlAt(ctx, metaDir)
	if e != nil || captured != c.control {
		return nil, errors.New("captured control identity mismatch")
	}
	phase = "native-verify"
	if _, err = runTool(ctx, c.env, "pg_verifybackup", "--exit-on-error", "--no-parse-wal", dir); err != nil {
		return nil, err
	}
	phase = "wal-verify"
	if err = VerifyWAL(ctx, walDir, manifest.Ranges); err != nil {
		return nil, err
	}
	phase = "compression"
	for i, name := range names {
		raw, e := os.Open(filepath.Join(dir, name))
		if e != nil {
			return nil, e
		}
		f, ar, e := spool(ctx, raw, c.Directory, c.Snapshot.Repository.Spec.Compression, c.Snapshot.Repository.Spec.Native.MaxBackupBytes+max(configuration.GiB, (c.Snapshot.Repository.Spec.Native.MaxBackupBytes+99)/100))
		raw.Close()
		if e != nil {
			return nil, e
		}
		ar.Index = i
		ar.Role = "base"
		if i == 1 {
			ar.Role = "wal"
		}
		if i >= 2 {
			ar.Role = "tablespace"
			oid := cm.Tablespaces[i-2].OID
			ar.TablespaceOID = &oid
		}
		r.Files = append(r.Files, f)
		cm.Artifacts = append(cm.Artifacts, ar)
	}
	var compressed int64
	for _, a := range cm.Artifacts {
		compressed += a.StoredBytes
	}
	maxRaw := c.Snapshot.Repository.Spec.Native.MaxBackupBytes
	if compressed > maxRaw+max(configuration.GiB, (maxRaw+99)/100) {
		return nil, ErrInput
	}
	if _, err = c.Postflight(ctx); err != nil {
		return nil, err
	}
	r.Commit = cm
	return r, nil
}

// VerifyWAL never asks pg_verifybackup to invoke system(). Every accepted range
// is passed directly to the matching parser, even in a shell-free image.
func VerifyWAL(ctx context.Context, directory string, ranges []repository.WALRange) error {
	if len(ranges) != 1 {
		return ErrInput
	}
	for _, r := range ranges {
		a, e := repository.ParseLSN(r.StartLSN)
		b, e2 := repository.ParseLSN(r.EndLSN)
		if e != nil || e2 != nil || r.Timeline == 0 || a >= b {
			return ErrInput
		}
		if _, e = runTool(ctx, []string{"LANG=C", "LC_ALL=C"}, "pg_waldump", "--quiet", "--path="+directory, "--timeline="+strconv.FormatUint(uint64(r.Timeline), 10), "--start="+r.StartLSN, "--end="+r.EndLSN); e != nil {
			return e
		}
	}
	return nil
}
func fileHash(ctx context.Context, f *os.File) (string, error) {
	h := sha256.New()
	st, e := f.Stat()
	if e != nil {
		return "", e
	}
	_, e = io.CopyBuffer(h, contextInput{ctx, io.NewSectionReader(f, 0, st.Size())}, make([]byte, 128<<10))
	return hex.EncodeToString(h.Sum(nil)), e
}

type cappedWriter struct {
	w    io.Writer
	left int64
}

func (w *cappedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.left {
		return 0, ErrInput
	}
	n, e := w.w.Write(p)
	w.left -= int64(n)
	return n, e
}
func spool(ctx context.Context, raw *os.File, dir, compression string, cap int64) (f *os.File, a repository.Artifact, err error) {
	f, err = os.CreateTemp(dir, "spool-")
	if err != nil {
		return nil, a, err
	}
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(f.Name())
		}
	}()
	rh, sh := sha256.New(), sha256.New()
	cw := &cappedWriter{io.MultiWriter(f, sh), cap}
	var output io.Writer = cw
	var gz *gzip.Writer
	if compression == "gzip" {
		gz, err = gzip.NewWriterLevel(cw, 1)
		if err != nil {
			return f, a, err
		}
		output = gz
	} else if compression != "none" {
		return f, a, ErrInput
	}
	a.RawBytes, err = io.CopyBuffer(io.MultiWriter(output, rh), contextInput{ctx, raw}, make([]byte, 128<<10))
	if err != nil {
		return f, a, err
	}
	if gz != nil {
		if err = gz.Close(); err != nil {
			return f, a, err
		}
	}
	if err = f.Sync(); err != nil {
		return f, a, err
	}
	a.StoredBytes = cap - cw.left
	a.RawSHA256 = hex.EncodeToString(rh.Sum(nil))
	a.StoredSHA256 = hex.EncodeToString(sh.Sum(nil))
	a.Compression = compression
	return f, a, nil
}
func parseLabel(c *repository.Commit, segment int64) error {
	fields := map[string]string{}
	for _, line := range strings.Split(strings.TrimSuffix(c.BackupLabel, "\n"), "\n") {
		p := strings.SplitN(line, ": ", 2)
		if len(p) != 2 || fields[p[0]] != "" {
			return ErrInput
		}
		fields[p[0]] = p[1]
	}
	lsn, _ := repository.ParseLSN(c.StartLSN)
	if fields["START WAL LOCATION"] != c.StartLSN+" (file "+WALFilename(c.Timeline, lsn, segment)+")" || fields["BACKUP METHOD"] != "streamed" || fields["BACKUP FROM"] != "primary" || fields["START TIMELINE"] != strconv.FormatUint(uint64(c.Timeline), 10) {
		return ErrInput
	}
	checkpoint, e := repository.ParseLSN(fields["CHECKPOINT LOCATION"])
	end, _ := repository.ParseLSN(c.StopLSN)
	if e != nil || checkpoint < lsn || checkpoint >= end {
		return ErrInput
	}
	started, e := time.Parse("2006-01-02 15:04:05 MST", fields["START TIME"])
	if e != nil || !strings.HasSuffix(fields["START TIME"], " UTC") {
		return ErrInput
	}
	c.StartedAt = started.UTC().Format(time.RFC3339Nano)
	stop, e := time.Parse(time.RFC3339Nano, c.StoppedAt)
	if e != nil || stop.Before(started) {
		return ErrInput
	}
	return nil
}
func validateTablespaceMap(text string, tables []repository.Tablespace, connection Connection) error {
	expected := map[string]string{}
	for _, t := range tables {
		expected[strconv.FormatUint(uint64(t.OID), 10)] = connection.Tablespaces[t.Name]
	}
	lines := strings.Split(strings.TrimSuffix(text, "\n"), "\n")
	if len(lines) != len(expected) {
		return ErrInput
	}
	seen := map[string]bool{}
	for _, line := range lines {
		p := strings.SplitN(line, " ", 2)
		if len(p) != 2 || seen[p[0]] || expected[p[0]] == "" || expected[p[0]] != p[1] {
			return ErrInput
		}
		seen[p[0]] = true
	}
	return nil
}
func (c *Capture) monitor(ctx context.Context, done <-chan struct{}, cancel context.CancelFunc) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			var used, raw, wal, spools int64
			count := 0
			e := filepath.WalkDir(c.Directory, func(p string, d os.DirEntry, e error) error {
				if e != nil {
					return e
				}
				count++
				if count > maxEntries+1000 {
					return ErrInput
				}
				if !d.Type().IsRegular() && !d.IsDir() {
					return ErrInput
				}
				if d.IsDir() {
					return nil
				}
				st, e := d.Info()
				if e != nil {
					return e
				}
				size := st.Size()
				used += size
				rel, _ := filepath.Rel(c.Directory, p)
				if strings.HasPrefix(rel, "tar/") {
					raw += size
				}
				if rel == "tar/pg_wal.tar" {
					wal = size
				}
				if strings.HasPrefix(rel, "spool-") {
					spools += size
				}
				return nil
			})
			n := c.Snapshot.Repository.Spec.Native
			var fs unix.Statfs_t
			se := unix.Statfs(workspace, &fs)
			if e != nil || se != nil || used > c.peak || raw > n.MaxBackupBytes || wal > n.MaxBootstrapWALBytes || spools > n.MaxBackupBytes+max(configuration.GiB, (n.MaxBackupBytes+99)/100) || fs.Bsize <= 0 || fs.Bavail < uint64(configuration.GiB/int64(fs.Bsize)) {
				cancel()
				return fmt.Errorf("native capture capacity exceeded")
			}
		}
	}
}
