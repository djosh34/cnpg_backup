package retention

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
)

func TestRunDestructiveFaultsVersusRestoreAdmission(t *testing.T) {
	for _, fault := range []string{"reject", "lost", "delayed"} {
		for step := 1; step <= 3; step++ {
			t.Run(fmt.Sprintf("%s-%d", fault, step), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					r, s, files, now := emptyFailedAttempt(t)
					ctx := context.Background()
					attempts := 0
					var pending func() error
					ambiguous := &s3store.Error{Kind: s3store.Transient, Ambiguous: true}
					s.beforeEffect = func(k string) error {
						attempts++
						if attempts != step {
							return nil
						}
						// Request is owned but not yet complete; another cluster has no read
						// entitlement, even though all inventory prerequisites succeeded.
						if _, e := r.AdmitRestore(ctx, repository.UUID(), repository.UUID()); !errors.Is(e, repository.ErrBlocked) {
							t.Fatal("admitted before drain", e)
						}
						if fault == "reject" {
							return &s3store.Error{Kind: s3store.Auth}
						}
						if fault == "delayed" {
							if step == 3 {
								u := s.uploads[0]
								pending = func() error { return s.Abort(ctx, u) }
							} else {
								pending = func() error { return s.Delete(ctx, k) }
							}
							return ambiguous
						}
						return nil
					}
					s.afterEffect = func(string) error {
						if attempts == step && fault == "lost" {
							return ambiguous
						}
						return nil
					}
					_, e := Run(ctx, r, files, now, runOptions())
					if e == nil {
						t.Fatal("fault not reached")
					}
					fresh, e := repository.OpenSource(ctx, s, r.Identity().RepositoryID, t.TempDir())
					if e != nil {
						t.Fatal(e)
					}
					s.beforeEffect = nil
					s.afterEffect = nil
					if pending != nil {
						if e = pending(); e != nil {
							t.Fatal(e)
						}
					}
					target, operation := repository.UUID(), repository.UUID()
					reader, e := fresh.AdmitRestore(ctx, target, operation)
					if fault != "reject" {
						if !errors.Is(e, repository.ErrBlocked) {
							t.Fatal("ambiguous owner adopted after restart/late completion", e)
						}
						if _, e = Run(ctx, fresh, files, now.Add(365*24*time.Hour), runOptions()); !errors.Is(e, repository.ErrBlocked) {
							t.Fatal("clock adopted owner", e)
						}
					} else {
						if e != nil {
							t.Fatal("conclusive rejection blocked restore", e)
						}
						reader.Close(ctx)
						fresh.ReleaseLifetimeAfterTermination(ctx, target, operation)
						result, e := Run(ctx, fresh, files, now, runOptions())
						if e != nil || !result.Executed {
							t.Fatal("restart failed to clean remainder", result, e)
						}
					}
					if s.unsafe {
						t.Fatal("destructive effect without exclusive owner")
					}
				})
			})
		}
	}
}

func TestRunCanceledInventoryAndInterruptedAcknowledgedBatch(t *testing.T) {
	for _, phase := range []string{"inventory", "acknowledged"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r, s, files, now := emptyFailedAttempt(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if phase == "inventory" {
					s.beforeList = func(p string) error {
						if strings.HasSuffix(p, "/wal/") {
							cancel()
							return ctx.Err()
						}
						return nil
					}
				} else {
					s.afterEffect = func(string) error { cancel(); return nil }
				}
				_, e := Run(ctx, r, files, now, runOptions())
				if !errors.Is(e, context.Canceled) {
					t.Fatal("cancellation not observed", e)
				}
				want := 0
				if phase == "acknowledged" {
					want = 1
				}
				if s.effects != want {
					t.Fatal("dispatched after cancellation", s.effects, want)
				}
				fresh, e := repository.OpenSource(context.Background(), s, r.Identity().RepositoryID, t.TempDir())
				if e != nil {
					t.Fatal(e)
				}
				s.beforeList = nil
				s.afterEffect = nil
				result, e := Run(context.Background(), fresh, files, now, runOptions())
				if e != nil || !result.Executed || s.effects != 3 || s.unsafe {
					t.Fatal("conclusive canceled work failed to drain/restart", result, e, s.effects)
				}
			})
		})
	}
}

func TestRunBatchTimeCountYieldAndNoVictims(t *testing.T) {
	for _, slow := range []bool{false, true} {
		t.Run(fmt.Sprint(slow), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r, s, files, now := emptyFailedAttempt(t)
				ctx := context.Background()
				key := s.uploads[0].Key
				for i := 1; i < 140; i++ {
					s.uploads = append(s.uploads, s3store.Upload{Key: key, ID: fmt.Sprint(i)})
				}
				if slow {
					s.afterEffect = func(string) error { time.Sleep(16 * time.Second); return nil }
				}
				started := time.Now()
				result, e := Run(ctx, r, files, now, runOptions())
				if slow {
					if !errors.Is(e, repository.ErrCapacity) || s.effects != 2 || time.Since(started) < 33*time.Second {
						t.Fatal("reset per request or failed to drain/yield", result, e, s.effects, time.Since(started))
					}
				} else if e != nil || result.Planned != 128 || s.effects != 128 || time.Since(started) < time.Second {
					t.Fatal("batch count/yield", result, e, s.effects)
				}
				s.afterEffect = nil
				for pass := 0; pass < 3; pass++ {
					result, e = Run(ctx, r, files, now, runOptions())
					if e != nil {
						t.Fatal(e)
					}
				}
				if result.Planned != 0 || s.effects != 142 || s.unsafe {
					t.Fatal("bounded restart/no-victims", result, s.effects, s.unsafe)
				}
			})
		})
	}
}

func TestRunFailedProtectionAcknowledgmentBlocksGCNotNewRestore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, s, files, now, _ := runtimeFixture(t)
		ctx := context.Background()
		// Remotely committed lifetime admission, but every original CAS/barrier
		// response is lost. No reader or catalog is returned without acknowledgment.
		s.afterPut = func(k string) error {
			if strings.HasSuffix(k, "/gate.json") {
				return &s3store.Error{Kind: s3store.Transient, Ambiguous: true}
			}
			return nil
		}
		h, e := r.AdmitRestore(ctx, repository.UUID(), repository.UUID())
		if e == nil || h != nil || s.lists != 0 {
			t.Fatal("unacknowledged reader obtained access", h, e, s.lists)
		}
		s.afterPut = nil
		if _, e = Run(ctx, r, files, now, runOptions()); !errors.Is(e, repository.ErrBlocked) || s.lists != 0 || s.effects != 0 {
			t.Fatal("uncertain holder failed to protect", e)
		}
		if _, e = r.AdmitRestore(ctx, repository.UUID(), repository.UUID()); e != nil {
			t.Fatal("crashed holder blocked new protected restore", e)
		}
		var g repository.Gate
		json.Unmarshal(s.objects[s.root+"gate.json"].body, &g)
		if g.Owner != nil || len(g.Holders) < 3 {
			t.Fatal("erased prior lifetime", g)
		}
	})
}
