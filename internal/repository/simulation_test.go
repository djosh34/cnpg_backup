package repository

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

func TestDeterministicAdmissionDeletionSimulation(t *testing.T) {
	run := func(seed int64) string {
		rng := rand.New(rand.NewSource(seed))
		var trace []string
		for event := 0; event < 8; event++ {
			r, s := setup(t)
			h, res := publish(t, r, backupID, "seeded backup")
			reader, e := r.AdmitRestore(ctx, targetID, UUID())
			if e != nil {
				t.Fatal(e)
			}
			if _, e = r.AcquireGC(ctx); e != ErrBlocked {
				t.Fatal(e)
			}
			requireOracle(t, s)
			// Source controller disappears; an independent target still admits alongside
			// the old paused holders using ONLY persisted storage and source config.
			rr, e := OpenSource(ctx, s, repoID, r.workspace)
			if e != nil {
				t.Fatal(e)
			}
			other, e := rr.AdmitRestore(ctx, targetID, UUID())
			if e != nil {
				t.Fatal(e)
			}
			cat, e := other.Catalog(ctx, CatalogLimits{10, 32 << 20})
			if e != nil {
				t.Fatal(e)
			}
			cat.Close()
			if _, e = rr.AcquireGC(ctx); e != ErrBlocked {
				t.Fatal(e)
			}
			requireOracle(t, s)
			other.Close(ctx)
			rr.ReleaseLifetimeAfterTermination(ctx, targetID, other.holder.OperationID)
			reader.Close(ctx)
			r.ReleaseLifetimeAfterTermination(ctx, targetID, reader.holder.OperationID)
			h.Close(ctx)
			owner, e := rr.AcquireGC(ctx)
			if e != nil {
				t.Fatal(e)
			}
			s.failAt = s.n + 2 + rng.Intn(2)
			s.fault = []string{"lost", "delay"}[rng.Intn(2)]
			if e = owner.Execute(ctx, gcplan(owner, retire(res), payload(res, 0))); e == nil {
				t.Fatal("fault failed to fire")
			}
			if e = owner.Close(ctx); e != ErrUncertain {
				t.Fatal(e)
			}
			requireOracle(t, s)
			s.deliver()
			requireOracle(t, s)
			restarted, e := Open(ctx, s, identity(), r.workspace)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = restarted.AdmitRestore(ctx, targetID, UUID()); e != ErrBlocked {
				t.Fatal("uncertain destructive owner adopted", e)
			}
			trace = append(trace, fmt.Sprintf("event %d: backup/readers/source-loss/GC/fault/restart", event))
			trace = append(trace, s.trace...)
		}
		return strings.Join(trace, "\n")
	}
	for _, seed := range []int64{11, 17} {
		a, b := run(seed), run(seed)
		if a != b {
			t.Fatalf("non-reproducible trace seed=%d\nfirst:\n%s\nsecond:\n%s", seed, a, b)
		}
		t.Logf("seed=%d operations=8 trace_sha256=%s\n%s", seed, digest([]byte(a)), a)
	}
}
func TestIndependentOracleRejectsUnsafeOwnerRepair(t *testing.T) {
	r, s := setup(t)
	h, res := publish(t, r, backupID, "backup")
	h.Close(ctx)
	owner, e := r.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	s.failAt = s.n + 3
	s.fault = "delay"
	if e = owner.Execute(ctx, gcplan(owner, retire(res), payload(res, 0))); e == nil {
		t.Fatal("fault not injected")
	}
	requireOracle(t, s)
	// Deliberately BROKEN test-only negative control: emulate an unsafe TTL/admin
	// repair. This is a contract violation, not standard S3 or product behavior.
	key := r.root + "gate.json"
	o := s.objects[key]
	var g Gate
	json.Unmarshal(o.b, &g)
	g.Owner = nil
	g.Nonce = UUID()
	o.b, _ = json.Marshal(g)
	o.info.Size = int64(len(o.b))
	s.objects[key] = o
	if _, e = r.AdmitRestore(ctx, targetID, UUID()); e != nil {
		t.Fatal(e)
	}
	s.deliver()
	if len(s.oracleErrors) == 0 {
		t.Fatal("oracle did not catch delayed deletion under admitted reader")
	}
}
func TestPlanSameSegmentCoverage(t *testing.T) {
	fp := digest([]byte("bootstrap"))
	op, _ := RestoreOperationID(targetID, fp)
	p := Plan{Schema: 1, Source: identity(), DestinationRepositoryID: destID, TargetClusterUID: targetID, BootstrapSHA256: fp, OperationID: op, LifetimeHoldID: op, ReaderHoldID: capturedID, Target: Target{Kind: "latest", Timeline: 1}, Path: []Timeline{{ID: 1}}, Chain: []Commit{schemaFixture()}, RequiredArchive: []WALRange{{1, "0/1000100", "0/1000200"}}}
	bundle, remote, e := p.Coverage("000000010000000000000001")
	if e != nil || !bundle || !remote {
		t.Fatal("same final bundled segment hides required post-backup bytes", bundle, remote, e)
	}
	p.RequiredArchive = []WALRange{}
	bundle, remote, e = p.Coverage("000000010000000000000001")
	if e != nil || !bundle || remote {
		t.Fatal("bundle-only absence cannot fall back", bundle, remote, e)
	}
	bundle, remote, e = p.Coverage("000000010000000000000002")
	if e != nil || bundle || remote {
		t.Fatal("future tail incorrectly required", e)
	}
}
