package s3store

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/minio/minio-go/v7"
)

// Upload identifies a unique attempt-owned artifact and its MPU. On failure,
// including ambiguous initiation (empty ID), retain it for admitted inventory/
// cleanup. It is not permission to reuse the key for different bytes.
type Upload struct{ Key, ID string }

// UploadFile uses two bounded section-reader workers and fixed 64 MiB parts.
// It drains both workers before returning; it NEVER aborts or deletes, even on
// cancellation. Callers reconcile ambiguous completion by verified GET later.
func (s *Store) UploadFile(ctx context.Context, key string, f *os.File, expected Integrity, metadata map[string]string) (Upload, Info, error) {
	u := Upload{Key: key}
	k, e := s.key(key)
	if e != nil {
		return u, Info{}, e
	}
	if !validIntegrity(expected, MaxArtifactSize) || expected.Size == 0 {
		return u, Info{}, failure(Invalid)
	}
	o, e := artifactOptions(metadata)
	if e != nil {
		return u, Info{}, e
	}
	ctx, cancel := s.operation(ctx)
	defer cancel()
	release, e := acquireData(ctx, s.artifacts, processArtifacts)
	if e != nil {
		return u, Info{}, e
	}
	defer release()
	if _, e = checkFile(ctx, f, expected); e != nil {
		return u, Info{}, e
	}
	u.ID, e = s.core.NewMultipartUpload(ctx, s.bucket, k, o)
	if e != nil {
		return u, Info{}, classify(e, true)
	}
	if u.ID == "" || len(u.ID) > 2048 || !plain(u.ID) {
		return u, Info{}, &Error{Kind: Corrupt, Ambiguous: true}
	}
	parts := make([]minio.CompletePart, (expected.Size+PartSize-1)/PartSize)
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	jobs := make(chan int)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	for range s.workers {
		wg.Go(func() {
			for i := range jobs {
				offset := int64(i) * PartSize
				size := min(PartSize, expected.Size-offset)
				h := md5.New()
				_, err := copyContext(workerCtx, h, io.NewSectionReader(f, offset, size))
				if err != nil {
					err = classifyLocal(workerCtx)
				} else {
					var part minio.ObjectPart
					part, err = s.core.PutObjectPart(workerCtx, s.bucket, k, u.ID, i+1, io.NewSectionReader(f, offset, size), size, minio.PutObjectPartOptions{Md5Base64: base64.StdEncoding.EncodeToString(h.Sum(nil)), DisableContentSha256: true})
					err = classify(err, true)
					if err == nil {
						if !validETag(part.ETag) {
							err = &Error{Kind: Corrupt, Ambiguous: true}
						} else {
							parts[i] = minio.CompletePart{PartNumber: i + 1, ETag: part.ETag}
						}
					}
				}
				if err != nil {
					mu.Lock()
					if first == nil {
						first = err
					} else {
						var a, b *Error
						if errors.As(first, &a) && errors.As(err, &b) && b.Ambiguous {
							a.Ambiguous = true
						}
					}
					mu.Unlock()
					stop()
					return
				}
			}
		})
	}
send:
	for i := range parts {
		select {
		case jobs <- i:
		case <-workerCtx.Done():
			break send
		}
	}
	close(jobs)
	wg.Wait()
	if first != nil {
		return u, Info{}, first
	}
	if ctx.Err() != nil {
		return u, Info{}, failure(Canceled)
	}
	v, e := s.core.CompleteMultipartUpload(ctx, s.bucket, k, u.ID, parts, o)
	if e != nil {
		return u, Info{}, classify(e, true)
	}
	if !validETag(v.ETag) || v.VersionID != "" || v.Bucket != s.bucket || v.Key != k {
		return u, Info{}, &Error{Kind: Corrupt, Ambiguous: true}
	}
	return u, Info{Key: key, ETag: v.ETag, Size: expected.Size}, nil
}

// Abort requires exclusive destructive admission, no outstanding part workers,
// and serial request ownership. NoSuchUpload is not proof an earlier abort
// drained; every uncertain result is ambiguous and must block owner release.
func (s *Store) Abort(ctx context.Context, u Upload) error {
	k, e := s.key(u.Key)
	if e != nil {
		return e
	}
	if u.ID == "" || len(u.ID) > 2048 || !plain(u.ID) {
		return failure(Invalid)
	}
	ctx, cancel := context.WithTimeout(ctx, s.metadataTimeout)
	defer cancel()
	return classify(s.core.AbortMultipartUpload(ctx, s.bucket, k, u.ID), true)
}
