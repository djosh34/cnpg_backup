package s3store

import (
	"context"
	"crypto/md5" // S3 transport checksum only, not content identity.
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

// Info carries opaque ETag CAS identity separately from application SHA256.
type Info struct {
	Key, ETag string
	Size      int64
	Modified  time.Time
	Metadata  map[string]string
}
type Integrity struct {
	Size   int64
	SHA256 string
}

func info(v minio.ObjectInfo) Info {
	metadata := make(map[string]string, len(v.UserMetadata))
	for k, value := range v.UserMetadata {
		metadata[strings.ToLower(k)] = value
	}
	return Info{Key: v.Key, ETag: v.ETag, Size: v.Size, Modified: v.LastModified, Metadata: metadata}
}
func validIntegrity(v Integrity, max int64) bool {
	if len(v.SHA256) != 64 {
		return false
	}
	b, e := hex.DecodeString(v.SHA256)
	return v.Size >= 0 && v.Size <= max && e == nil && len(b) == 32 && v.SHA256 == strings.ToLower(v.SHA256)
}
func validETag(s string) bool {
	return s != "" && len(s) <= 256 && plain(s) && !strings.ContainsAny(s, "\"\r\n")
}

// Condition must select exactly one of Create or Match. Shared names never
// have an unconditional write path. A 412 is not a successful duplicate.
type Condition struct {
	Create bool
	Match  string
}

func options(c Condition, metadata map[string]string) (minio.PutObjectOptions, error) {
	if c.Create == (c.Match != "") || c.Match != "" && !validETag(c.Match) {
		return minio.PutObjectOptions{}, failure(Invalid)
	}
	o, e := artifactOptions(metadata)
	if e != nil {
		return o, e
	}
	o.DisableMultipart = true
	o.SendContentMd5 = true
	if c.Create {
		o.SetMatchETagExcept("*")
	} else {
		o.SetMatchETag(c.Match)
	}
	return o, nil
}
func artifactOptions(metadata map[string]string) (minio.PutObjectOptions, error) {
	if len(metadata) > 128 {
		return minio.PutObjectOptions{}, failure(Limit)
	}
	copy := make(map[string]string, len(metadata))
	size := 0
	for k, v := range metadata {
		size += len(k) + len(v)
		if size > 8192 {
			return minio.PutObjectOptions{}, failure(Limit)
		}
		if !strings.HasPrefix(k, "cnpg-") || !plain(k) || !plain(v) || strings.ContainsAny(k, ": \t") {
			return minio.PutObjectOptions{}, failure(Invalid)
		}
		copy[k] = v
	}
	contentType := "application/octet-stream"
	// Permanent WAL retirement slots contain JSON, never compressed WAL bytes.
	if copy["cnpg-format"] == "wal-retired-v1" {
		contentType = "application/json"
	}
	return minio.PutObjectOptions{UserMetadata: copy, ContentType: contentType, DisableContentSha256: true}, nil
}

// PutFile is one known-length conditional PUT from an immutable caller-owned
// seekable spool. It never closes the file, retries, or performs cleanup.
// WAL slots are independent of artifact transfer slots.
func (s *Store) PutFile(ctx context.Context, key string, f *os.File, expected Integrity, c Condition, metadata map[string]string) (Info, error) {
	k, e := s.key(key)
	if e != nil {
		return Info{}, e
	}
	o, e := options(c, metadata)
	if e != nil {
		return Info{}, e
	}
	if !validIntegrity(expected, MaxSingleSize) {
		return Info{}, failure(Invalid)
	}
	ctx, cancel := context.WithTimeout(ctx, min(s.walTimeout, s.operationTimeout))
	defer cancel()
	release, e := acquireData(ctx, s.wal, processWAL)
	if e != nil {
		return Info{}, e
	}
	defer release()
	md, e := checkFile(ctx, f, expected)
	if e != nil {
		return Info{}, e
	}
	// Core single PUT avoids SDK's unbounded-reader and automatic MPU paths.
	v, e := s.core.PutObject(ctx, s.bucket, k, io.NewSectionReader(f, 0, expected.Size), expected.Size, md, expected.SHA256, o)
	if e != nil {
		return Info{}, classify(e, true)
	}
	if !validETag(v.ETag) || v.VersionID != "" {
		return Info{}, &Error{Kind: Corrupt, Ambiguous: true}
	}
	return Info{Key: key, ETag: v.ETag, Size: expected.Size}, nil
}

func checkFile(ctx context.Context, f *os.File, expected Integrity) (string, error) {
	if f == nil {
		return "", failure(Invalid)
	}
	st, e := f.Stat()
	if e != nil {
		return "", failure(LocalIO)
	}
	if !st.Mode().IsRegular() || st.Size() != expected.Size {
		return "", failure(Invalid)
	}
	md := md5.New()
	sha := sha256.New()
	n, e := copyContext(ctx, io.MultiWriter(md, sha), io.NewSectionReader(f, 0, expected.Size))
	if e != nil {
		return "", classifyLocal(ctx)
	}
	if n != expected.Size || hex.EncodeToString(sha.Sum(nil)) != expected.SHA256 {
		return "", failure(Corrupt)
	}
	return base64.StdEncoding.EncodeToString(md.Sum(nil)), nil
}
func classifyLocal(ctx context.Context) error {
	if ctx.Err() != nil {
		return failure(Canceled)
	}
	return failure(LocalIO)
}
func copyContext(ctx context.Context, w io.Writer, r io.Reader) (int64, error) {
	buf := make([]byte, 128<<10)
	return io.CopyBuffer(w, contextReader{ctx, r}, buf)
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.r.Read(p)
}

// Head is metadata only, never content verification. A bare HEAD404 reports
// HeadMissing, not NotFound: callers use GET to establish authenticated absence.
func (s *Store) Head(ctx context.Context, key string) (Info, error) {
	k, e := s.key(key)
	if e != nil {
		return Info{}, e
	}
	ctx, cancel := s.operation(ctx)
	defer cancel()
	v, e := s.core.StatObject(ctx, s.bucket, k, minio.StatObjectOptions{})
	if e != nil {
		return Info{}, classify(e, false)
	}
	if !validETag(v.ETag) || v.Size < 0 || v.VersionID != "" {
		return Info{}, failure(Corrupt)
	}
	v.Key = key
	return info(v), nil
}

// Read consumes the GET, including its body errors, before reporting absence
// or success. Intended for bounded JSON metadata whose digest is not yet known.
func (s *Store) Read(ctx context.Context, key string, max int64) ([]byte, Info, error) {
	k, e := s.key(key)
	if e != nil {
		return nil, Info{}, e
	}
	if max < 1 || max > MaxMetadataSize {
		return nil, Info{}, failure(Invalid)
	}
	ctx, cancel := s.operation(ctx)
	defer cancel()
	var b []byte
	var result Info
	ctx = context.WithValue(ctx, requestTimeoutKey{}, s.metadataTimeout)
	e = retryRead(ctx, func() error {
		r, v, _, err := s.core.GetObject(ctx, s.bucket, k, minio.GetObjectOptions{})
		if err != nil {
			return classify(err, false)
		}
		defer r.Close()
		if v.Size < 0 || v.Size > max || !validETag(v.ETag) || v.VersionID != "" {
			return failure(Corrupt)
		}
		b, err = io.ReadAll(io.LimitReader(r, max+1))
		if err != nil {
			if ctx.Err() != nil {
				return failure(Canceled)
			}
			return failure(Transient)
		}
		if int64(len(b)) != v.Size {
			return failure(Corrupt)
		}
		if err = r.Close(); err != nil {
			return failure(Transient)
		}
		v.Key = key
		result = info(v)
		return nil
	})
	if e != nil {
		return nil, Info{}, e
	}
	return b, result, nil
}

// Download writes only a caller-owned private file. Failure leaves untrusted
// partial bytes; the caller must not publish them. Success verifies exact stored
// length/SHA256 and fsyncs. Raw/gzip verification belongs to the WAL/native layer.
func (s *Store) Download(ctx context.Context, key string, dst *os.File, expected Integrity) (Info, error) {
	return s.download(ctx, key, dst, expected, s.artifacts)
}

// DownloadWAL reserves independent WAL capacity even during saturated artifact
// transfers. The caller supplies its shorter recovery-helper read deadline.
func (s *Store) DownloadWAL(ctx context.Context, key string, dst *os.File, expected Integrity) (Info, error) {
	if expected.Size > MaxSingleSize {
		return Info{}, failure(Invalid)
	}
	return s.download(ctx, key, dst, expected, s.wal)
}
func (s *Store) download(ctx context.Context, key string, dst *os.File, expected Integrity, slots chan struct{}) (Info, error) {
	k, e := s.key(key)
	if e != nil {
		return Info{}, e
	}
	if dst == nil || !validIntegrity(expected, MaxArtifactSize) {
		return Info{}, failure(Invalid)
	}
	ctx, cancel := s.operation(ctx)
	defer cancel()
	global := processArtifacts
	if slots == s.wal {
		global = processWAL
	}
	release, e := acquireData(ctx, slots, global)
	if e != nil {
		return Info{}, e
	}
	defer release()
	var result Info
	e = retryRead(ctx, func() error {
		if dst.Truncate(0) != nil {
			return failure(LocalIO)
		}
		if _, err := dst.Seek(0, 0); err != nil {
			return failure(LocalIO)
		}
		r, v, _, err := s.core.GetObject(ctx, s.bucket, k, minio.GetObjectOptions{})
		if err != nil {
			return classify(err, false)
		}
		defer r.Close()
		if v.Size != expected.Size || !validETag(v.ETag) || v.VersionID != "" {
			return failure(Corrupt)
		}
		h := sha256.New()
		w := &fileWriter{f: dst}
		n, err := copyContext(ctx, io.MultiWriter(w, h), io.LimitReader(r, expected.Size+1))
		if w.err != nil {
			return failure(LocalIO)
		}
		if err != nil {
			if ctx.Err() != nil {
				return failure(Canceled)
			}
			return failure(Transient)
		}
		if n != expected.Size || hex.EncodeToString(h.Sum(nil)) != expected.SHA256 {
			return failure(Corrupt)
		}
		if r.Close() != nil {
			return failure(Transient)
		}
		if dst.Sync() != nil {
			return failure(LocalIO)
		}
		v.Key = key
		result = info(v)
		return nil
	})
	return result, e
}

type fileWriter struct {
	f   *os.File
	err error
}

func (w *fileWriter) Write(p []byte) (int, error) { n, e := w.f.Write(p); w.err = e; return n, e }

// Delete must be called serially by the live admitted destructive owner. An
// ambiguous result permanently prevents automatic owner release. No retry.
func (s *Store) Delete(ctx context.Context, key string) error {
	k, e := s.key(key)
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, s.metadataTimeout)
	defer cancel()
	return classify(s.core.RemoveObject(ctx, s.bucket, k, minio.RemoveObjectOptions{}), true)
}
