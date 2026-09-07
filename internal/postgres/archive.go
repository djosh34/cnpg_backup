// Copyright 2026 cnpg_backup contributors. All rights reserved.
package postgres

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
)

const maxEntries = 100000
const maxFileBytes int64 = (1 << 30) + (1 << 20)

var ErrInput = errors.New("invalid or over-budget native input")

type NativeManifest struct {
	SystemIdentifier uint64
	Files            map[string]int64
	Ranges           []repository.WALRange
}
type manifestFile struct {
	Path              string `json:"Path,omitempty"`
	EncodedPath       string `json:"Encoded-Path,omitempty"`
	Size              int64  `json:"Size"`
	LastModified      string `json:"Last-Modified"`
	ChecksumAlgorithm string `json:"Checksum-Algorithm"`
	Checksum          string `json:"Checksum"`
}

func nativePath(p string) bool {
	if p == "" || len(p) > 1023 || path.Clean(p) != p || path.IsAbs(p) || strings.ContainsAny(p, "\x00\\\r\n") {
		return false
	}
	for _, c := range strings.Split(p, "/") {
		if c == "" || c == "." || c == ".." {
			return false
		}
	}
	return true
}
func hashText(s string) bool { b, e := hex.DecodeString(s); return e == nil && len(b) == 32 }

// ScanManifest streams bounded file records instead of decoding a database-sized
// JSON graph twice. The original bytes and native self-checksum are never edited.
// Initial primary capture supports exactly one WAL range: multiple ranges fail,
// never succeed after checking only the first.
func ScanManifest(r io.Reader) (NativeManifest, error) {
	m := NativeManifest{Files: map[string]int64{}}
	d := json.NewDecoder(io.LimitReader(r, repository.MaxManifestBytes+1))
	tok, e := d.Token()
	if e != nil || tok != json.Delim('{') {
		return m, ErrInput
	}
	seen := map[string]bool{}
	for d.More() {
		tok, e = d.Token()
		key, ok := tok.(string)
		if e != nil || !ok || seen[key] {
			return m, ErrInput
		}
		seen[key] = true
		switch key {
		case "PostgreSQL-Backup-Manifest-Version":
			var v int
			if d.Decode(&v) != nil || v != 2 {
				return m, ErrInput
			}
		case "System-Identifier":
			if d.Decode(&m.SystemIdentifier) != nil || m.SystemIdentifier == 0 {
				return m, ErrInput
			}
		case "Manifest-Checksum":
			var s string
			if d.Decode(&s) != nil || !hashText(s) {
				return m, ErrInput
			}
		case "Files":
			t, e := d.Token()
			if e != nil || t != json.Delim('[') {
				return m, ErrInput
			}
			for d.More() {
				var raw json.RawMessage
				if d.Decode(&raw) != nil || len(raw) > 16<<10 || len(m.Files) >= maxEntries {
					return m, ErrInput
				}
				var f manifestFile
				if configuration.StrictJSON(raw, &f) != nil || (f.Path == "") == (f.EncodedPath == "") || f.Size < 0 || f.Size > maxFileBytes || f.ChecksumAlgorithm != "SHA256" || !hashText(f.Checksum) || f.LastModified == "" {
					return m, ErrInput
				}
				p := f.Path
				if f.EncodedPath != "" {
					b, e := hex.DecodeString(f.EncodedPath)
					if e != nil {
						return m, ErrInput
					}
					p = string(b)
				}
				if !nativePath(p) {
					return m, ErrInput
				}
				if _, ok := m.Files[p]; ok {
					return m, ErrInput
				}
				m.Files[p] = f.Size
			}
			if t, e := d.Token(); e != nil || t != json.Delim(']') {
				return m, ErrInput
			}
		case "WAL-Ranges":
			var raw json.RawMessage
			if d.Decode(&raw) != nil || len(raw) > 4096 {
				return m, ErrInput
			}
			var ranges []struct {
				Timeline uint32 `json:"Timeline"`
				Start    string `json:"Start-LSN"`
				End      string `json:"End-LSN"`
			}
			if configuration.StrictJSON(raw, &ranges) != nil || len(ranges) != 1 {
				return m, ErrInput
			}
			for _, v := range ranges {
				a, e := repository.ParseLSN(v.Start)
				b, e2 := repository.ParseLSN(v.End)
				if e != nil || e2 != nil || v.Timeline == 0 || a >= b {
					return m, ErrInput
				}
				m.Ranges = append(m.Ranges, repository.WALRange{Timeline: v.Timeline, StartLSN: v.Start, EndLSN: v.End})
			}
		default:
			return m, ErrInput
		}
	}
	if t, e := d.Token(); e != nil || t != json.Delim('}') || len(seen) != 5 || len(m.Files) == 0 {
		return m, ErrInput
	}
	if _, e := d.Token(); e != io.EOF || d.InputOffset() > repository.MaxManifestBytes {
		return m, ErrInput
	}
	return m, nil
}

type archiveInventory struct {
	files   map[string]int64
	entries int
	bytes   int64
}

// scanArchive visits only regular files after validating their headers. The
// visitor can copy to an os.Root, never an archive-selected destination root.
// PAX/GNU/sparse/link/device headers and nonzero trailing material fail closed.
func scanArchive(ctx context.Context, r io.Reader, limit int64, visit func(string, int64, io.Reader) error) (archiveInventory, error) {
	inv := archiveInventory{files: map[string]int64{}}
	lr := &io.LimitedReader{R: contextInput{ctx, r}, N: limit + 1}
	tr := tar.NewReader(lr)
	seen := map[string]byte{}
	var padding int64
	for {
		before := lr.N
		h, e := tr.Next()
		if e == io.EOF {
			// Drain member content below so this read is exactly the two
			// terminator blocks, not an unmarked physical EOF.
			if before-lr.N != 1024+padding {
				return inv, ErrInput
			}
			break
		}
		if e != nil {
			return inv, ErrInput
		}
		name := h.Name
		if h.Typeflag == tar.TypeDir {
			name = strings.TrimSuffix(name, "/")
			// PG18's server emits these two empty built-in directories with
			// a literal ./ prefix. Normalize only these known native names
			// before duplicate/ancestor checks; arbitrary dot paths fail.
			if name == "./pg_wal/archive_status" || name == "./pg_wal/summaries" {
				name = strings.TrimPrefix(name, "./")
			}
		}
		if !nativePath(name) || (h.Format != tar.FormatUSTAR && h.Format != tar.FormatUnknown) || len(h.PAXRecords) != 0 || h.Linkname != "" || h.Size < 0 || h.Size > maxFileBytes || (h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeDir) {
			return inv, ErrInput
		}
		if _, ok := seen[name]; ok {
			return inv, ErrInput
		}
		seen[name] = h.Typeflag
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if t, ok := seen[parent]; ok && t != tar.TypeDir {
				return inv, ErrInput
			}
		}
		inv.entries++
		inv.bytes += h.Size
		if inv.entries > maxEntries || inv.bytes > limit || h.Typeflag == tar.TypeDir && h.Size != 0 {
			return inv, ErrInput
		}
		if h.Typeflag == tar.TypeReg {
			inv.files[name] = h.Size
			if visit != nil {
				if e = visit(name, h.Size, tr); e != nil {
					return inv, e
				}
			}
		}
		if _, e := io.Copy(io.Discard, tr); e != nil {
			return inv, ErrInput
		}
		padding = (512 - h.Size%512) % 512
	}
	// archive/tar consumes the two terminator blocks. Native tar writers may pad
	// with zero blocks; neither another archive nor unbounded padding is accepted.
	tail, e := io.ReadAll(io.LimitReader(lr, 10241))
	if e != nil || len(tail) > 10240 || !bytes.Equal(tail, make([]byte, len(tail))) || lr.N <= 0 {
		return inv, ErrInput
	}
	// Check reverse-order ancestor conflicts without materializing up to 511
	// implicit prefixes for each of 100,000 paths.
	for name := range seen {
		if e := ctx.Err(); e != nil {
			return inv, e
		}
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if t, ok := seen[parent]; ok && t != tar.TypeDir {
				return inv, ErrInput
			}
		}
	}
	return inv, nil
}

type contextInput struct {
	ctx context.Context
	r   io.Reader
}

func (r contextInput) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.r.Read(p)
}

// WALFilename uses interval arithmetic, including the final partial segment.
func WALFilename(timeline uint32, lsn uint64, segmentBytes int64) string {
	n := lsn / uint64(segmentBytes)
	per := uint64(1<<32) / uint64(segmentBytes)
	return strings.ToUpper(leftHex(uint64(timeline)) + leftHex(n/per) + leftHex(n%per))
}
func leftHex(n uint64) string {
	s := strconv.FormatUint(n, 16)
	return strings.Repeat("0", 8-len(s)) + s
}

func copyMember(root *os.Root, name string, size int64, r io.Reader) error {
	if !nativePath(name) {
		return ErrInput
	}
	if e := root.MkdirAll(path.Dir(name), 0700); e != nil {
		return e
	}
	f, e := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	n, e := io.CopyBuffer(f, io.LimitReader(r, size+1), make([]byte, 128<<10))
	if e == nil && n != size {
		e = ErrInput
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e != nil {
		return e
	}
	return ce
}
