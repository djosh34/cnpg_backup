package repository

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"testing"
)

// Actual publication orchestration at the native postflight I/O seam. The
// independent stored-byte oracle runs after each event and persisted restart.
func TestCapturePostflightPublicationSimulation(t *testing.T) {
	run := func(seed int64) string {
		rng := rand.New(rand.NewSource(seed))
		var trace []string
		for event := 0; event < 12; event++ {
			r, s := setup(t)
			h, e := r.AdmitBackup(ctx, writerID, backupID)
			if e != nil {
				t.Fatal(e)
			}
			a, _, e := h.Begin(ctx, request(backupID))
			if e != nil {
				t.Fatal(e)
			}
			c, m, files := capture(t, a, "native-verified-fixture")
			outcome := rng.Intn(3)
			called := false
			res, e := a.PublishChecked(ctx, c, m, files, func(context.Context) error {
				called = true
				for _, ar := range c.Artifacts {
					if _, ok := s.objects[r.artifact(c, ar)]; !ok {
						t.Fatal("postflight before upload")
					}
				}
				if _, ok := s.objects[r.backup(backupID)+"commit.json"]; ok {
					t.Fatal("postflight after commit")
				}
				switch outcome {
				case 0:
					return errors.New("source promoted or restarted")
				case 1:
					s.failAt = s.n + 1
					s.fault = "lost"
				}
				return nil
			})
			if !called {
				t.Fatal("native final continuity check omitted")
			}
			if outcome == 0 {
				if e == nil || res != nil {
					t.Fatal("failed postflight published")
				}
				if _, ok := s.objects[r.backup(backupID)+"commit.json"]; ok {
					t.Fatal("incomplete backup selectable")
				}
			} else if e != nil || res == nil {
				t.Fatal("durable committed winner lost", e)
			}
			requireOracle(t, s)
			rr, e := Open(ctx, s, identity(), r.workspace)
			if e != nil {
				t.Fatal(e)
			}
			retry, e := rr.AdmitBackup(ctx, writerID, backupID)
			if e != nil {
				t.Fatal(e)
			}
			next, winner, e := retry.Begin(ctx, request(backupID))
			if e != nil {
				t.Fatal(e)
			}
			if outcome == 0 {
				if next == nil || winner != nil || next.ID() == a.ID() {
					t.Fatal("failed attempt reused")
				}
			} else if next != nil || winner == nil || string(winner.Bytes) != string(res.Bytes) || winner.PublishedAt != res.PublishedAt {
				t.Fatal("retry fabricated native result")
			}
			requireOracle(t, s)
			trace = append(trace, s.trace...)
		}
		return strings.Join(trace, "\n")
	}
	for _, seed := range []int64{5, 19} {
		a, b := run(seed), run(seed)
		if a != b {
			t.Fatal("non-reproducible capture publication trace")
		}
		t.Logf("seed=%d operations=12 trace_sha256=%s", seed, digest([]byte(a)))
	}
}
