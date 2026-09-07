package s3store

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/minio/minio-go/v7"
)

// CheckBucketSafety is required for writer/GC readiness (not read-only restore).
// It needs the three bucket configuration read permissions. Any lifecycle rule
// is conservatively unsupported, including autonomous multipart expiration.
// Administrative mutation/replication and consistency remain operator contracts.
func (s *Store) CheckBucketSafety(ctx context.Context) error {
	ctx, cancel := s.operation(ctx)
	defer cancel()
	v, e := s.core.GetBucketVersioning(ctx, s.bucket)
	if e != nil {
		return classify(e, false)
	}
	if v.Status != "" {
		return failure(Unsupported)
	}
	lc, e := s.core.GetBucketLifecycle(ctx, s.bucket)
	if e != nil {
		r := minio.ToErrorResponse(e)
		if r.Code != "NoSuchLifecycleConfiguration" || r.StatusCode != 404 {
			return classify(e, false)
		}
	} else if lc == nil || len(lc.Rules) != 0 {
		return failure(Unsupported)
	}
	lock, _, _, _, e := s.core.GetObjectLockConfig(ctx, s.bucket)
	if e != nil {
		r := minio.ToErrorResponse(e)
		if r.Code != "ObjectLockConfigurationNotFoundError" || r.StatusCode != 404 {
			return classify(e, false)
		}
	} else if lock != "" {
		return failure(Unsupported)
	}
	return nil
}

// CheckConditions proves positive/negative create and ETag-CAS behavior on a
// fresh disposable namespace. It never deletes probe objects. Returned keys
// (also on failure) belong in subsequent gate-owned cleanup, not a defer abort.
// probeRoot is the caller-derived repository probes path; workspace must be
// private disk-backed scratch. Finite probes do not prove
// universal linearizability; the endpoint must also document the contract.
func (s *Store) CheckConditions(ctx context.Context, probeRoot, workspace string) ([]string, error) {
	if _, e := s.key(probeRoot); e != nil {
		return nil, e
	}
	ctx, cancel := s.operation(ctx)
	defer cancel()
	token := make([]byte, 16)
	if _, e := rand.Read(token); e != nil {
		return nil, failure(LocalIO)
	}
	token[6] = token[6]&0x0f | 0x40
	token[8] = token[8]&0x3f | 0x80
	base := fmt.Sprintf("%s/%x-%x-%x-%x-%x/", probeRoot, token[:4], token[4:6], token[6:8], token[8:10], token[10:])
	keys := []string{base + "conditional", base + "missing"}
	f, e := os.CreateTemp(workspace, "s3-probe-")
	if e != nil {
		return keys, failure(LocalIO)
	}
	defer os.Remove(f.Name())
	defer f.Close()
	a := []byte("cnpg storage probe original " + hex.EncodeToString(token))
	b := append([]byte("replacement "), a...)
	put := func(key string, data []byte, c Condition) (Info, error) {
		if f.Truncate(0) != nil {
			return Info{}, failure(LocalIO)
		}
		if _, e = f.WriteAt(data, 0); e != nil {
			return Info{}, failure(LocalIO)
		}
		h := sha256.Sum256(data)
		return s.PutFile(ctx, key, f, Integrity{int64(len(data)), hex.EncodeToString(h[:])}, c, nil)
	}
	original, e := put(keys[0], a, Condition{Create: true})
	if e != nil {
		return keys, e
	}
	unsupported := func(err error) error {
		if err == nil {
			return failure(Unsupported)
		}
		if Is(err, Precondition) {
			return nil
		}
		return err
	}
	_, e = put(keys[0], b, Condition{Create: true})
	if e = unsupported(e); e != nil {
		return keys, e
	}
	got, head, e := s.Read(ctx, keys[0], 1024)
	if e != nil {
		return keys, e
	}
	if !bytes.Equal(got, a) || head.ETag != original.ETag {
		return keys, failure(Unsupported)
	}
	_, e = put(keys[1], b, Condition{Match: original.ETag})
	if e = unsupported(e); e != nil {
		return keys, e
	}
	newer, e := put(keys[0], b, Condition{Match: original.ETag})
	if e != nil {
		return keys, e
	}
	if newer.ETag == original.ETag {
		return keys, failure(Unsupported)
	}
	_, e = put(keys[0], a, Condition{Match: original.ETag})
	if e = unsupported(e); e != nil {
		return keys, e
	}
	got, head, e = s.Read(ctx, keys[0], 1024)
	if e != nil {
		return keys, e
	}
	if !bytes.Equal(got, b) || head.ETag != newer.ETag {
		return keys, failure(Unsupported)
	}
	stat, e := s.Head(ctx, keys[0])
	if e != nil {
		return keys, e
	}
	if stat.ETag != newer.ETag || stat.Size != int64(len(b)) {
		return keys, failure(Unsupported)
	}
	count := 0
	e = s.List(ctx, base, 10, func(v Info) error {
		count++
		if v.Key != keys[0] || v.ETag != newer.ETag {
			return failure(Unsupported)
		}
		return nil
	})
	if e != nil {
		return keys, e
	}
	if count != 1 {
		return keys, failure(Unsupported)
	}
	return keys, nil
}
