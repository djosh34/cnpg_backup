package retention

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
)

// Check actual durable request effects, not Run's reported decision. The oracle
// uses the independently supplied original graph and segment obligations.
func storedRecoveryOracle(s *inventoryStore, in Inventory, now time.Time) error {
	remaining := Decision{Keep: map[string]string{}}
	for _, b := range in.Backups {
		prefix := s.root + "backups/" + b.ID + "/"
		if _, retired := s.objects[prefix+"retired.json"]; retired {
			continue
		}
		remaining.Keep[b.ID] = "observed live"
		var c repository.Commit
		ob, ok := s.objects[prefix+"commit.json"]
		if !ok || json.Unmarshal(ob.body, &c) != nil {
			return errors.New("live commit lost")
		}
		attempt := prefix + "attempts/" + c.AttemptID + "/"
		if m, ok := s.objects[attempt+"manifest.pg.json"]; !ok || sum(m.body) != c.ManifestSHA256 {
			return errors.New("retained manifest lost")
		}
		for _, a := range c.Artifacts {
			key := attempt + fmt.Sprintf("data/%d.tar", a.Index)
			if a.Compression == "gzip" {
				key += ".gz"
			}
			if payload, ok := s.objects[key]; !ok || sum(payload.body) != a.StoredSHA256 {
				return errors.New("retained input lost")
			}
		}
	}
	// Never infer physical effects from a planner floor or the persisted victim
	// list: a wrong victim translation must be visible to this independent check.
	in.Segments = append([]Segment(nil), in.Segments...)
	for i, seg := range in.Segments {
		name := fmt.Sprintf("%08X%08X%08X", seg.Timeline, seg.Start>>32, (seg.Start&0xffffffff)/in.SegmentBytes)
		ob, ok := s.objects[s.root+fmt.Sprintf("wal/%08X/", seg.Timeline)+name]
		in.Segments[i].Live = ok && ob.info.Metadata["cnpg-format"] == "wal-v1"
	}
	return recoveryOracle(in, remaining, now.Add(-time.Hour), now, 1)
}

func TestRunGeneratedForkEffectsPreserveIndependentRecovery(t *testing.T) {
	for seed := int64(0); seed < 8; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r, s, files, now, ids := runtimeFixture(t)
				ctx := context.Background()
				step := uint64(r.Identity().WALSegmentBytes)
				rng := rand.New(rand.NewSource(seed))
				fork := uint64(3+rng.Intn(4))*step + 17
				in := Inventory{SegmentBytes: step, Paths: [][]Timeline{{{ID: 1}}, {{ID: 1}, {ID: 2, Fork: fork}}}}
				for i, id := range ids {
					in.Backups = append(in.Backups, Backup{ID: id, Timeline: 1, Completed: now.Add(time.Duration(i-4)*time.Hour + time.Minute), Start: uint64(i+1) * step, Redo: uint64(i+1) * step, End: uint64(i+1)*step + 0x100})
				}
				seedWAL := func(timeline uint32, name string, body []byte, raw int64) {
					key := s.root + fmt.Sprintf("wal/%08X/", timeline) + name
					s.seed(key, body)
					v := s.objects[key]
					v.info.Size = raw
					v.info.Metadata = map[string]string{"cnpg-format": "wal-v1", "cnpg-system-id": r.Identity().SystemIdentifier, "cnpg-raw-bytes": fmt.Sprint(raw), "cnpg-raw-sha256": sum(body), "cnpg-stored-sha256": sum(body), "cnpg-compression": "none"}
					s.objects[key] = v
				}
				for timeline := uint32(1); timeline <= 2; timeline++ {
					last := fork / step
					if timeline == 2 {
						last += 4
					}
					first := uint64(2)
					if timeline == 1 {
						first = 1
					}
					for i := first; i <= last; i++ {
						seedWAL(timeline, fmt.Sprintf("%08X00000000%08X", timeline, i), []byte("metadata-only segment fixture"), int64(step))
						in.Segments = append(in.Segments, Segment{timeline, i * step, true})
					}
				}
				history := []byte(fmt.Sprintf("1\t0/%X\tpromotion\n", fork))
				seedWAL(2, "00000002.history", history, int64(len(history)))
				if seed%2 == 1 {
					// A recent differential of the oldest full keeps that root even
					// though a newer independent full satisfies the numeric floor.
					in.Backups = append(in.Backups, seedDifferential(t, s, ids[1], ids[0], now))
				}
				check := func() {
					t.Helper()
					if e := storedRecoveryOracle(s, in, now); e != nil {
						t.Fatalf("seed=%d effects=%d: %v", seed, s.effects, e)
					}
				}
				check()
				// A late WAL-list error after all callbacks must prevent even unrelated
				// backup retirement. No unbounded scan or partial-list entitlement.
				fault := errors.New("last archive page failed")
				s.afterList = func(p string) error {
					if strings.HasSuffix(p, "/wal/") {
						return fault
					}
					return nil
				}
				if _, e := Run(ctx, r, files, now, runOptions()); !errors.Is(e, fault) || s.effects != 0 {
					t.Fatal("late inventory authorized victims", e, s.effects)
				}
				s.afterList = nil
				// Reject retirement or a WAL tombstone after earlier acknowledged effects.
				// Every original effect independently preserves kept replay and parent input.
				attempts := 0
				interrupt := 1 + rng.Intn(int(fork/step)-1)
				if seed%2 == 1 {
					interrupt = 1
				}
				if seed == 6 {
					interrupt = 2
				} // retirement ACK, then lost WAL ACK
				s.beforeEffect = func(string) error {
					attempts++
					if attempts == interrupt && seed < 6 {
						return &s3store.Error{Kind: s3store.Auth}
					}
					return nil
				}
				s.afterEffect = func(string) error {
					check()
					if seed >= 6 && attempts == interrupt {
						return &s3store.Error{Kind: s3store.Transient, Ambiguous: true}
					}
					return nil
				}
				if _, e := Run(ctx, r, files, now, runOptions()); e == nil {
					t.Fatal("scheduled interruption did not fire")
				}
				check()
				s.beforeEffect = nil
				if seed >= 6 {
					fresh, e := repository.OpenSource(ctx, s, r.Identity().RepositoryID, t.TempDir())
					if e != nil {
						t.Fatal(e)
					}
					if _, e = fresh.AdmitRestore(ctx, repository.UUID(), repository.UUID()); !errors.Is(e, repository.ErrBlocked) {
						t.Fatal("lost WAL acknowledgment admitted restore", e)
					}
					if _, e = Run(ctx, fresh, files, now.Add(365*24*time.Hour), runOptions()); !errors.Is(e, repository.ErrBlocked) {
						t.Fatal("adopted uncertain retirement owner", e)
					}
					return
				}
				for pass := 0; pass < 3; pass++ {
					fresh, e := repository.OpenSource(ctx, s, r.Identity().RepositoryID, t.TempDir())
					if e != nil {
						t.Fatal(e)
					}
					result, e := Run(ctx, fresh, files, now, runOptions())
					if e != nil {
						t.Fatal(result, e)
					}
					check()
				}
				_, retired := s.objects[s.root+"backups/"+ids[0]+"/retired.json"]
				if retired != (seed%2 == 0) {
					t.Fatal("expiration failed to respect retained differential parent", retired)
				}
				// Negative control on actual durable bytes, not a damaged Plan return.
				required := s.root + fmt.Sprintf("wal/00000002/0000000200000000%08X", fork/step)
				saved := s.objects[required]
				delete(s.objects, required)
				if storedRecoveryOracle(s, in, now) == nil {
					t.Fatal("durable-effect oracle blessed lost required fork segment")
				}
				s.objects[required] = saved
				if s.unsafe {
					t.Fatal("effects overlapped protected admission")
				}
			})
		})
	}
}

func seedDifferential(t *testing.T, s *inventoryStore, source, parent string, now time.Time) Backup {
	t.Helper()
	var c repository.Commit
	if e := json.Unmarshal(s.objects[s.root+"backups/"+source+"/commit.json"].body, &c); e != nil {
		t.Fatal(e)
	}
	oldPrefix := s.root + "backups/" + source + "/attempts/" + c.AttemptID + "/"
	uid, attempt := repository.UUID(), repository.UUID()
	prefix := s.root + "backups/" + uid + "/"
	attemptPrefix := prefix + "attempts/" + attempt + "/"
	put := func(k string, v any) []byte {
		b, e := json.Marshal(v)
		if e != nil {
			t.Fatal(e)
		}
		s.seed(k, b)
		return b
	}
	var request repository.Request
	json.Unmarshal(s.objects[s.root+"backups/"+source+"/request.json"].body, &request)
	request.BackupUID = uid
	request.RequestedKind = "differential"
	request.RootBackupUID = &parent
	qb := put(prefix+"request.json", request)
	put(attemptPrefix+"claim.json", repository.Claim{Schema: 1, RepositoryID: c.RepositoryID, BackupUID: uid, AttemptID: attempt, ProcessID: repository.UUID(), RequestSHA256: sum(qb)})
	s.seed(attemptPrefix+"manifest.pg.json", s.objects[oldPrefix+"manifest.pg.json"].body)
	for _, a := range c.Artifacts {
		name := fmt.Sprintf("data/%d.tar", a.Index)
		s.seed(attemptPrefix+name, s.objects[oldPrefix+name].body)
	}
	c.BackupUID = uid
	c.AttemptID = attempt
	c.Kind = "differential"
	c.RootBackupUID = parent
	c.ParentBackupUID = &parent
	var full repository.Commit
	json.Unmarshal(s.objects[s.root+"backups/"+parent+"/commit.json"].body, &full)
	c.RootManifestSHA256 = &full.ManifestSHA256
	c.CaptureInstanceUID = full.CaptureInstanceUID
	c.RequestSHA256 = sum(qb)
	c.StartedAt = now.Add(-40 * time.Minute).Format(time.RFC3339Nano)
	c.StoppedAt = now.Add(-30 * time.Minute).Format(time.RFC3339Nano)
	put(prefix+"commit.json", c)
	return Backup{ID: uid, Parent: parent, Timeline: 1, Completed: now.Add(-30 * time.Minute), Start: 2 << 24, Redo: 2 << 24, End: 2<<24 | 0x100}
}
