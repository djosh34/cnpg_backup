package s3store

import (
	"context"
	"strings"
	"sync/atomic"

	"github.com/minio/minio-go/v7"
)

const MaxInventory = 1_000_000

// List streams a complete, bounded, ordered inventory. The caller must spool
// results and use them ONLY after nil return; partial callbacks are not a
// catalog. There is no unbounded in-memory accumulation or hidden goroutine.
func (s *Store) List(ctx context.Context, prefix string, max int, visit func(Info) error) error {
	p, e := s.listPrefix(prefix)
	if e != nil {
		return e
	}
	if max < 1 || max > MaxInventory || visit == nil {
		return failure(Invalid)
	}
	ctx, cancel := s.operation(ctx)
	defer cancel()
	ctx = context.WithValue(ctx, pageBudgetKey{}, new(atomic.Int64))
	last := ""
	count := 0
	for v := range s.core.ListObjectsIter(ctx, s.bucket, minio.ListObjectsOptions{Prefix: p, Recursive: true, MaxKeys: 1000}) {
		if v.Err != nil {
			return classify(v.Err, false)
		}
		count++
		if count > max {
			return failure(Limit)
		}
		if !strings.HasPrefix(v.Key, p) || v.Key <= last || v.Size < 0 || !validETag(v.ETag) {
			return failure(Corrupt)
		}
		last = v.Key
		v.Key = s.relative(v.Key)
		if _, e = s.key(v.Key); e != nil {
			return failure(Corrupt)
		}
		if e = visit(info(v)); e != nil {
			return e
		}
	}
	if ctx.Err() != nil {
		return failure(Canceled)
	}
	return nil
}
func (s *Store) relative(k string) string {
	if s.prefix != "" {
		return strings.TrimPrefix(k, s.prefix+"/")
	}
	return k
}

// ListUploads deliberately uses Core pages: SDK high-level enumeration also
// lists every part and hides cleanup-oriented behavior. IDs are opaque, bounded.
func (s *Store) ListUploads(ctx context.Context, prefix string, max int, visit func(Upload) error) error {
	p, e := s.listPrefix(prefix)
	if e != nil {
		return e
	}
	if max < 1 || max > MaxInventory || visit == nil {
		return failure(Invalid)
	}
	ctx, cancel := s.operation(ctx)
	defer cancel()
	key, id := "", ""
	count := 0
	for page := 0; page < 10001; page++ {
		v, e := s.core.ListMultipartUploads(ctx, s.bucket, p, key, id, "", 1000)
		if e != nil {
			return classify(e, false)
		}
		if len(v.Uploads) > 1000 || len(v.CommonPrefixes) != 0 {
			return failure(Corrupt)
		}
		for _, u := range v.Uploads {
			count++
			if count > max {
				return failure(Limit)
			}
			if !strings.HasPrefix(u.Key, p) || u.UploadID == "" || len(u.UploadID) > 2048 || !plain(u.UploadID) {
				return failure(Corrupt)
			}
			relative := s.relative(u.Key)
			if _, e = s.key(relative); e != nil {
				return failure(Corrupt)
			}
			if e = visit(Upload{Key: relative, ID: u.UploadID}); e != nil {
				return e
			}
		}
		if !v.IsTruncated {
			return nil
		}
		if v.NextKeyMarker == "" || v.NextKeyMarker < key || v.NextKeyMarker == key && v.NextUploadIDMarker == id || v.NextUploadIDMarker == "" {
			return failure(Corrupt)
		}
		key, id = v.NextKeyMarker, v.NextUploadIDMarker
	}
	return failure(Limit)
}

// ListParts is a bounded complete diagnostic inventory for a known upload; it
// does not resume failed attempts or infer completion/abort from absence.
type Part struct {
	Number int
	Size   int64
	ETag   string
}

func (s *Store) ListParts(ctx context.Context, u Upload, visit func(Part) error) error {
	k, e := s.key(u.Key)
	if e != nil {
		return e
	}
	if u.ID == "" || len(u.ID) > 2048 || !plain(u.ID) || visit == nil {
		return failure(Invalid)
	}
	ctx, cancel := s.operation(ctx)
	defer cancel()
	marker := 0
	count := 0
	for page := 0; page < 11; page++ {
		v, e := s.core.ListObjectParts(ctx, s.bucket, k, u.ID, marker, 1000)
		if e != nil {
			return classify(e, false)
		}
		last := marker
		for _, p := range v.ObjectParts {
			p.ETag = strings.Trim(p.ETag, "\"")
			count++
			if count > 8192 {
				return failure(Limit)
			}
			if p.PartNumber <= last || p.Size < 0 || p.Size > PartSize || !validETag(p.ETag) {
				return failure(Corrupt)
			}
			last = p.PartNumber
			if e = visit(Part{Number: p.PartNumber, Size: p.Size, ETag: p.ETag}); e != nil {
				return e
			}
		}
		if !v.IsTruncated {
			return nil
		}
		if v.NextPartNumberMarker <= marker || v.NextPartNumberMarker != last {
			return failure(Corrupt)
		}
		marker = v.NextPartNumberMarker
	}
	return failure(Limit)
}
