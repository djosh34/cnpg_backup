package retention

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
)

type zeroBytes struct{}

func (zeroBytes) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func TestRunMaximumManifestAndConclusiveWorkspaceFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "64MiB-input", true: "capacity-refusal"}[fail], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r, s, files, now, _ := runtimeFixture(t)
				ctx := context.Background()
				hash := sha256.New()
				if _, e := io.CopyN(hash, zeroBytes{}, repository.MaxManifestBytes); e != nil {
					t.Fatal(e)
				}
				digest := hex.EncodeToString(hash.Sum(nil))
				for key, v := range s.objects {
					if strings.HasSuffix(key, "/commit.json") {
						var c repository.Commit
						json.Unmarshal(v.body, &c)
						c.ManifestBytes = repository.MaxManifestBytes
						c.ManifestSHA256 = digest
						b, _ := json.Marshal(c)
						s.seed(key, b)
					}
				}
				s.downloadHook = func(k string, f *os.File, want s3store.Integrity) (bool, error) {
					if !strings.HasSuffix(k, "/manifest.pg.json") {
						return false, nil
					}
					if fail {
						return true, repository.ErrCapacity
					}
					if want.Size != repository.MaxManifestBytes {
						t.Fatal("wrong accepted manifest bound", want.Size)
					}
					// Sparse file, streamed hashing by real repository verification: no huge
					// in-memory fake body and no root filesystem exhaustion experiment.
					if e := f.Truncate(want.Size); e != nil {
						return true, e
					}
					entries, e := os.ReadDir(files.Workspace)
					if e != nil {
						t.Fatal(e)
					}
					aggregate := int64(0)
					for _, entry := range entries {
						info, e := entry.Info()
						if e != nil {
							t.Fatal(e)
						}
						aggregate += info.Size() + 8192
					}
					if aggregate > repository.GCWorkspaceBytes {
						t.Fatal("aggregate workspace overflow", aggregate)
					}
					return true, nil
				}
				result, e := Run(ctx, r, files, now, runOptions())
				if fail {
					if !errors.Is(e, repository.ErrCapacity) || s.effects != 0 {
						t.Fatal("capacity failure authorized effects", e, s.effects)
					}
				} else if e != nil || !result.Executed {
					t.Fatal("supported manifest refused", result, e)
				}
				entries, e := os.ReadDir(files.Workspace)
				if e != nil || len(entries) != 0 {
					t.Fatal("owner leaked bounded spools", entries, e)
				}
				if _, e = r.AdmitRestore(ctx, repository.UUID(), repository.UUID()); e != nil {
					t.Fatal("conclusive capacity/read outcome stranded owner", e)
				}
			})
		})
	}
}
