// Copyright 2026 cnpg_backup contributors. All rights reserved.
// Package wal owns synchronous, exact-file WAL publication and retrieval.
package wal

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strconv"

	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
	"golang.org/x/sys/unix"
)

var (
	ErrInvalid  = errors.New("invalid WAL input")
	ErrConflict = errors.New("WALContentConflict")
	ErrExpired  = errors.New("WALAlreadyExpired")
	ErrCorrupt  = errors.New("WAL integrity failure")
	ErrLocal    = errors.New("WAL local I/O failure")
)

// Storage is the concrete file I/O seam. Implementations must preserve the
// adapter's authenticated-GET absence and single-attempt conditional writes.
type Storage interface {
	Head(context.Context, string) (s3store.Info, error)
	Read(context.Context, string, int64) ([]byte, s3store.Info, error)
	PutFile(context.Context, string, *os.File, s3store.Integrity, s3store.Condition, map[string]string) (s3store.Info, error)
	DownloadWAL(context.Context, string, *os.File, s3store.Integrity) (s3store.Info, error)
}

type Files struct {
	Repository  *repository.Repository
	Store       Storage
	Workspace   string // private, finite disk; caller reserves capacity before work
	Compression string
}

// Limits validates the original PG filename using the repository's actual
// segment size. History and backup-history files are independently bounded.
func (w Files) Limits(name string) (key string, rawMax int64, err error) {
	if w.Repository == nil || w.Store == nil || (w.Compression != "none" && w.Compression != "gzip") {
		return "", 0, ErrInvalid
	}
	key, err = w.Repository.WALKey(name)
	if err != nil {
		return "", 0, ErrInvalid
	}
	rawMax = 1 << 20
	if len(name) == 24 {
		rawMax = w.Repository.Identity().WALSegmentBytes
	}
	return
}

func ambiguous(e error) bool                       { var se *s3store.Error; return !errors.As(e, &se) || se.Ambiguous }
func sum(h interface{ Sum([]byte) []byte }) string { return hex.EncodeToString(h.Sum(nil)) }

// The file interfaces consume FDs, never names. Unlink before any data is
// written so Linux releases scratch even on SIGKILL; no startup sweep needed.
func spoolFile(workspace, pattern string) (*os.File, error) {
	f, err := os.CreateTemp(workspace, pattern)
	if err != nil {
		return nil, err
	}
	if err = os.Remove(f.Name()); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

type contextReader struct {
	ctx context.Context
	io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.Reader.Read(p)
}
func copyBounded(ctx context.Context, out io.Writer, in io.Reader, max int64) (int64, error) {
	return io.CopyBuffer(out, contextReader{ctx, io.LimitReader(in, max+1)}, make([]byte, 128<<10))
}

// Archive never acknowledges local staging. Even an identical retry downloads
// and verifies the existing actual raw bytes, not just its metadata or ETag.
// The caller owns source confinement; source must be a private open regular FD.
func (w Files) Archive(ctx context.Context, name string, source *os.File) error {
	key, max, e := w.Limits(name)
	if e != nil {
		return e
	}
	if source == nil {
		return ErrInvalid
	}
	st, e := source.Stat()
	if e != nil {
		return ErrLocal
	}
	if !st.Mode().IsRegular() || st.Size() < 1 || st.Size() > max || len(name) == 24 && st.Size() != max {
		return ErrInvalid
	}
	spool, e := spoolFile(w.Workspace, "wal-upload-*")
	if e != nil {
		return ErrLocal
	}
	defer spool.Close()
	raw, stored := sha256.New(), sha256.New()
	out := io.MultiWriter(spool, stored)
	var compressed *gzip.Writer
	if w.Compression == "gzip" {
		compressed, e = gzip.NewWriterLevel(out, gzip.BestSpeed)
		if e != nil {
			return ErrInvalid
		}
		out = compressed
	}
	n, e := copyBounded(ctx, io.MultiWriter(out, raw), io.NewSectionReader(source, 0, st.Size()+1), max)
	if compressed != nil {
		ce := compressed.Close()
		if e == nil {
			e = ce
		}
	}
	if e != nil {
		return ErrLocal
	}
	after, e := source.Stat()
	if e != nil || n != st.Size() || after.Size() != st.Size() || !after.ModTime().Equal(st.ModTime()) {
		return ErrInvalid
	}
	size, e := spool.Stat()
	if e != nil || size.Size() > max+(1<<20) {
		return ErrLocal
	}
	if spool.Sync() != nil {
		return ErrLocal
	}
	metadata := map[string]string{"cnpg-format": "wal-v1", "cnpg-system-id": w.Repository.Identity().SystemIdentifier, "cnpg-raw-bytes": strconv.FormatInt(n, 10), "cnpg-raw-sha256": sum(raw), "cnpg-stored-sha256": sum(stored), "cnpg-compression": w.Compression}
	_, putErr := w.Store.PutFile(ctx, key, spool, s3store.Integrity{Size: size.Size(), SHA256: sum(stored)}, s3store.Condition{Create: true}, metadata)
	if putErr != nil && !s3store.Is(putErr, s3store.Precondition) && !s3store.Is(putErr, s3store.Conflict) && !ambiguous(putErr) {
		return putErr
	}
	// This read also verifies newly accepted publication; metadata is never an
	// independent byte oracle. A canceled/failed upload without verified remote
	// contents is not success. No mutation is retried here.
	verified, e := spoolFile(w.Workspace, "wal-verify-*")
	if e != nil {
		return ErrLocal
	}
	defer verified.Close()
	got, e := w.retrieve(ctx, name, verified)
	if e != nil {
		if putErr != nil && s3store.Is(e, s3store.NotFound) {
			return putErr
		}
		return e
	}
	if got.Size != n || got.SHA256 != sum(raw) {
		return ErrConflict
	}
	return nil
}

func hashValid(v string) bool {
	b, e := hex.DecodeString(v)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == v
}
func (w Files) metadata(name string, info s3store.Info) (s3store.Integrity, error) {
	_, max, e := w.Limits(name)
	if e != nil {
		return s3store.Integrity{}, e
	}
	m := info.Metadata
	if m["cnpg-format"] == "wal-retired-v1" {
		return s3store.Integrity{}, ErrExpired
	}
	raw, e := strconv.ParseInt(m["cnpg-raw-bytes"], 10, 64)
	if e != nil || strconv.FormatInt(raw, 10) != m["cnpg-raw-bytes"] || raw < 1 || raw > max || len(name) == 24 && raw != max || info.Size < 1 || info.Size > max+(1<<20) || m["cnpg-format"] != "wal-v1" || m["cnpg-system-id"] != w.Repository.Identity().SystemIdentifier || !hashValid(m["cnpg-raw-sha256"]) || !hashValid(m["cnpg-stored-sha256"]) || (m["cnpg-compression"] != "none" && m["cnpg-compression"] != "gzip") {
		return s3store.Integrity{}, ErrCorrupt
	}
	return s3store.Integrity{Size: raw, SHA256: m["cnpg-raw-sha256"]}, nil
}

// Retrieve verifies one exact source file into an owned private spool. Recovery
// callers must keep their repository reader admitted through this whole call.
func (w Files) Retrieve(ctx context.Context, name string, out *os.File) (s3store.Integrity, error) {
	return w.retrieve(ctx, name, out)
}
func (w Files) retrieve(ctx context.Context, name string, out *os.File) (s3store.Integrity, error) {
	key, _, e := w.Limits(name)
	if e != nil {
		return s3store.Integrity{}, e
	}
	info, e := w.Store.Head(ctx, key)
	if e != nil {
		// Preserve the FIRST non-absence failure. A later GET miss must never
		// turn an earlier TLS/auth/transport/corruption error into archive EOF.
		if !s3store.Is(e, s3store.HeadMissing) {
			return s3store.Integrity{}, e
		}
		// HEAD404 has no authenticated error body. Only a consumed GET NoSuchKey
		// can become absence. A race to a large live file fails closed, never EOF.
		_, _, getErr := w.Store.Read(ctx, key, 64<<10)
		if getErr != nil {
			return s3store.Integrity{}, getErr
		}
		return s3store.Integrity{}, e
	}
	raw, e := w.metadata(name, info)
	if e != nil {
		return raw, e
	}
	spool, e := spoolFile(w.Workspace, "wal-download-*")
	if e != nil {
		return raw, ErrLocal
	}
	defer spool.Close()
	got, e := w.Store.DownloadWAL(ctx, key, spool, s3store.Integrity{Size: info.Size, SHA256: info.Metadata["cnpg-stored-sha256"]})
	if e != nil {
		return raw, e
	}
	gotRaw, e := w.metadata(name, got)
	if e != nil {
		return raw, e
	}
	if gotRaw != raw || got.Metadata["cnpg-compression"] != info.Metadata["cnpg-compression"] || got.Metadata["cnpg-stored-sha256"] != info.Metadata["cnpg-stored-sha256"] {
		return raw, ErrCorrupt
	}
	input := bufio.NewReader(io.NewSectionReader(spool, 0, info.Size))
	var reader io.Reader = input
	var gz *gzip.Reader
	if info.Metadata["cnpg-compression"] == "gzip" {
		gz, e = gzip.NewReader(input)
		if e != nil {
			return raw, ErrCorrupt
		}
		defer gz.Close()
		gz.Multistream(false)
		reader = gz
	}
	if out.Truncate(0) != nil {
		return raw, ErrLocal
	}
	if _, e = out.Seek(0, 0); e != nil {
		return raw, ErrLocal
	}
	hash := sha256.New()
	writer := &fileWriter{out, nil}
	n, e := copyBounded(ctx, io.MultiWriter(writer, hash), reader, raw.Size)
	if writer.err != nil {
		return raw, ErrLocal
	}
	if e != nil || n != raw.Size || sum(hash) != raw.SHA256 {
		return raw, ErrCorrupt
	}
	if _, e = input.ReadByte(); e != io.EOF {
		return raw, ErrCorrupt
	} // one gzip member, no trailing bytes
	if ctx.Err() != nil {
		return raw, ctx.Err()
	}
	if out.Sync() != nil {
		return raw, ErrLocal
	}
	return raw, nil
}

type fileWriter struct {
	f   *os.File
	err error
}

func (w *fileWriter) Write(p []byte) (int, error) { n, e := w.f.Write(p); w.err = e; return n, e }

// Restore publishes only into an already-open, caller-owned physical WAL root.
// A single basename is accepted; os.Root confinement and O_EXCL private temp
// creation avoid path/symlink escapes. Rename replaces a symlink, never follows
// it. Existing final files are replaced only with fully verified durable bytes.
func (w Files) Restore(ctx context.Context, name string, root *os.Root, destination string) error {
	if _, _, e := w.Limits(name); e != nil {
		return e
	}
	if destination != name && destination != "RECOVERYXLOG" && destination != "RECOVERYHISTORY" {
		return ErrInvalid
	}
	// One fixed publication temporary per physical root bounds crash leftovers.
	// Lock the directory inode, not the replaceable temp/final inode. Only the
	// lock holder may reclaim this name; a concurrent callback fails/retries
	// without touching the active writer. This is not recovery-guard admission.
	dir, e := root.Open(".")
	if e != nil {
		return ErrLocal
	}
	defer dir.Close() // releases flock after temp cleanup, including on process death
	if unix.Flock(int(dir.Fd()), unix.LOCK_EX|unix.LOCK_NB) != nil {
		return ErrLocal
	}
	const tmp = ".cnpg-wal-restore"
	if e = root.Remove(tmp); e != nil && !errors.Is(e, os.ErrNotExist) {
		return ErrLocal
	}
	f, e := root.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return ErrLocal
	}
	defer func() { f.Close(); root.Remove(tmp) }()
	if _, e = w.retrieve(ctx, name, f); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return ErrLocal
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if e = root.Rename(tmp, destination); e != nil {
		return ErrLocal
	}
	if dir.Sync() != nil {
		return ErrLocal
	}
	return nil
}
