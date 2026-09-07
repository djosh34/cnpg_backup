package repository

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

func ptr[T any](v T) *T { return &v }
func gcplan(g *GCOwner, v ...Victim) GCPlan {
	return GCPlan{1, repoID, g.OperationID(), g.r.ProcessID(), "2026-09-07T00:00:00Z", Policy{86400, 2}, v}
}
func retire(res *Result) Victim {
	return Victim{Kind: "retire-backup", BackupUID: ptr(res.Commit.BackupUID), SHA256: digest(res.Bytes)}
}
func payload(res *Result, index int) Victim {
	c := res.Commit
	v := Victim{Kind: "delete-manifest", BackupUID: ptr(c.BackupUID), AttemptID: ptr(c.AttemptID), SHA256: c.ManifestSHA256}
	if index >= 0 {
		v.Kind = "delete-artifact"
		v.Index = ptr(index)
		v.Compression = ptr(c.Artifacts[index].Compression)
		v.SHA256 = c.Artifacts[index].StoredSHA256
	}
	return v
}
func TestRetirementAndDestructiveAmbiguity(t *testing.T) {
	for _, fault := range []string{"", "lost", "delay", "reject"} {
		t.Run(fault, func(t *testing.T) {
			r, s := setup(t)
			h, res := publish(t, r, backupID, "backup")
			if e := h.Close(ctx); e != nil {
				t.Fatal(e)
			}
			g, e := r.AcquireGC(ctx)
			if e != nil {
				t.Fatal(e)
			}
			if fault != "" {
				s.failAt = s.n + 3
				s.fault = fault
			} // plan PUT, retirement PUT, DELETE
			e = g.Execute(ctx, gcplan(g, retire(res), payload(res, 0)))
			if fault == "" && e != nil {
				t.Fatal(e)
			}
			if fault != "" && e == nil {
				t.Fatal("fault not observed")
			}
			if fault == "lost" || fault == "delay" {
				if e = g.Close(ctx); e != ErrUncertain {
					t.Fatal("uncertain owner released", e)
				}
				rr, e := Open(ctx, s, identity(), r.workspace)
				if e != nil {
					t.Fatal(e)
				}
				if _, e = rr.AdmitRestore(ctx, targetID, UUID()); e != ErrBlocked {
					t.Fatal("new incarnation adopted uncertain deleter", e)
				}
				s.deliver()
				if _, e = rr.AdmitRestore(ctx, targetID, UUID()); e != ErrBlocked {
					t.Fatal("late delivery allowed takeover", e)
				}
			} else {
				if e = g.Close(ctx); e != nil {
					t.Fatal(e)
				}
				rh, e := r.AdmitRestore(ctx, targetID, UUID())
				if e != nil {
					t.Fatal(e)
				}
				cat, e := rh.Catalog(ctx, CatalogLimits{10, 32 << 20})
				if e != nil {
					t.Fatal(e)
				}
				seen := 0
				cat.Visit(ctx, func(en Entry) error {
					seen++
					if !en.Retired || en.PublishedAt != res.PublishedAt {
						t.Error("lost permanent success history")
					}
					return nil
				})
				cat.Close()
				if seen != 1 {
					t.Fatal(seen)
				}
			}
			requireOracle(t, s)
		})
	}
}
func TestGCRejectsLivePayloadAndBounds(t *testing.T) {
	r, s := setup(t)
	h, res := publish(t, r, backupID, "data")
	h.Close(ctx)
	g, e := r.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	n := s.n
	if e = g.Execute(ctx, gcplan(g, payload(res, 0))); e != ErrBlocked {
		t.Fatal(e)
	}
	if s.n != n {
		t.Fatal("dispatched before plan validation")
	}
	if e = g.Close(ctx); e != nil {
		t.Fatal(e)
	}
	g, e = r.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	vs := make([]Victim, 129)
	for i := range vs {
		vs[i] = retire(res)
	}
	if e = g.Execute(ctx, gcplan(g, vs...)); e != ErrInvalid {
		t.Fatal(e)
	}
	g.Close(ctx)
	g, e = r.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	g.started = time.Now().Add(-31 * time.Second)
	if e = g.Execute(ctx, gcplan(g, retire(res))); e != ErrCapacity {
		t.Fatal(e)
	}
	if _, ok := s.objects[r.backup(backupID)+"retired.json"]; ok {
		t.Fatal("expired batch dispatched retirement")
	}
	g.Close(ctx)
	requireOracle(t, s)
}
func differential(t *testing.T, r *Repository, full *Result, uid string) (*Hold, *Result) {
	t.Helper()
	req := request(uid)
	req.RequestedKind = "differential"
	req.RootBackupUID = ptr(full.Commit.BackupUID)
	h, e := r.AdmitBackup(ctx, writerID, uid)
	if e != nil {
		t.Fatal(e)
	}
	a, _, e := h.Begin(ctx, req)
	if e != nil {
		t.Fatal(e)
	}
	c, m, fs := capture(t, a, "differential")
	c.Kind = "differential"
	c.ParentBackupUID = ptr(full.Commit.BackupUID)
	c.RootBackupUID = full.Commit.BackupUID
	c.RootManifestSHA256 = ptr(full.Commit.ManifestSHA256)
	c.StartedAt = "2026-09-07T02:00:00Z"
	c.StoppedAt = "2026-09-07T02:01:00Z"
	c.StartLSN = "0/2000028"
	c.RedoLSN = c.StartLSN
	c.BundledWALStartLSN = c.StartLSN
	c.StopLSN = "0/2000100"
	c.BundledWALEndLSN = c.StopLSN
	c.WALRanges = []WALRange{{1, c.StartLSN, c.StopLSN}}
	res, e := a.Publish(ctx, c, m, fs)
	if e != nil {
		t.Fatal(e)
	}
	return h, res
}
func TestFullParentValidationAndRetirementOrder(t *testing.T) {
	r, s := setup(t)
	hf, full := publish(t, r, backupID, "full")
	hd, diff := differential(t, r, full, "77777777-7777-4777-8777-777777777777")
	hf.Close(ctx)
	hd.Close(ctx)
	for _, change := range []func(*Commit){func(c *Commit) { c.CaptureInstanceUID = targetID }, func(c *Commit) { c.PostmasterStartedAt = "2026-09-07T00:30:00Z" }, func(c *Commit) { c.Timeline = 2 }, func(c *Commit) { c.ChecksumVersion = 0 }, func(c *Commit) { c.RootManifestSHA256 = ptr(strings.Repeat("0", 64)) }, func(c *Commit) { c.ParentBackupUID = ptr(c.BackupUID) }} {
		c := diff.Commit
		change(&c)
		if e := validateParent(c, full.Commit); e == nil {
			t.Fatal("accepted incompatible parent")
		}
	}
	g, e := r.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if e = g.Execute(ctx, gcplan(g, retire(full), retire(diff))); e != ErrBlocked {
		t.Fatal(e)
	}
	g.Close(ctx)
	g, e = r.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if e = g.Execute(ctx, gcplan(g, retire(diff), retire(full), payload(full, 0))); e != nil {
		t.Fatal(e)
	}
	g.Close(ctx)
	requireOracle(t, s)
}
func TestMalformedGraphAndPermanentMetadataFailClosed(t *testing.T) {
	for _, damage := range []string{"missing-parent", "cyclic-parent", "retired-without-commit", "bad-request", "bad-claim"} {
		t.Run(damage, func(t *testing.T) {
			r, s := setup(t)
			_, full := publish(t, r, backupID, "full")
			h, diff := differential(t, r, full, "77777777-7777-4777-8777-777777777777")
			switch damage {
			case "missing-parent":
				delete(s.objects, r.backup(full.Commit.BackupUID)+"commit.json")
			case "cyclic-parent":
				key := r.backup(diff.Commit.BackupUID) + "commit.json"
				o := s.objects[key]
				c := diff.Commit
				c.ParentBackupUID = ptr(c.BackupUID)
				c.RootBackupUID = c.BackupUID
				o.b, _ = json.Marshal(c)
				o.info.Size = int64(len(o.b))
				s.objects[key] = o
			case "retired-without-commit":
				key := r.backup(UUID()) + "retired.json"
				s.objects[key] = object{[]byte("{}"), s3store.Info{Key: key, Size: 2}}
			case "bad-request":
				key := r.backup(full.Commit.BackupUID) + "request.json"
				o := s.objects[key]
				o.b = []byte("{}")
				o.info.Size = 2
				s.objects[key] = o
			case "bad-claim":
				key := r.attempt(full.Commit.BackupUID, full.Commit.AttemptID) + "claim.json"
				o := s.objects[key]
				o.b = []byte("{}")
				o.info.Size = 2
				s.objects[key] = o
			}
			if cat, e := h.Catalog(ctx, CatalogLimits{10, 32 << 20}); e == nil {
				cat.Close()
				t.Fatal("malformed catalog escaped")
			}
		})
	}
}
func TestWALPermanentTombstoneAndStalePublisher(t *testing.T) {
	r, s := setup(t)
	name := "000000010000000000000001"
	key, e := r.WALKey(name)
	if e != nil {
		t.Fatal(e)
	}
	b := []byte("compressed WAL fixture")
	hash := digest(b)
	i, e := s.mutate(key, b, s3store.Condition{Create: true}, false)
	if e != nil {
		t.Fatal(e)
	}
	o := s.objects[key]
	o.info.Metadata = map[string]string{"cnpg-format": "wal-v1", "cnpg-system-id": r.id.SystemIdentifier, "cnpg-raw-bytes": "16777216", "cnpg-raw-sha256": hash}
	s.objects[key] = o
	g, e := r.AcquireGC(ctx)
	if e != nil {
		t.Fatal(e)
	}
	v := Victim{Kind: "retire-wal", WALName: &name, ExpectedETag: &i.ETag, SHA256: hash, RawBytes: 16 << 20}
	if e = g.Execute(ctx, gcplan(g, v)); e != nil {
		t.Fatal(e)
	}
	if e = g.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = s.mutate(key, b, s3store.Condition{Create: true}, false); !s3store.Is(e, s3store.Precondition) {
		t.Fatal("resurrected retired WAL", e)
	}
	if _, ok := s.objects[key]; !ok {
		t.Fatal("WAL slot deleted")
	}
	requireOracle(t, s)
}
func TestRestorePlanSelectionPersistenceAndRestart(t *testing.T) {
	r, s := setup(t)
	_, full := publish(t, r, backupID, "full")
	_, diff := differential(t, r, full, "77777777-7777-4777-8777-777777777777")
	fp := digest([]byte("bootstrap"))
	op, _ := RestoreOperationID(targetID, fp)
	rh, e := r.AdmitRestore(ctx, targetID, op)
	if e != nil {
		t.Fatal(e)
	}
	cat, e := rh.Catalog(ctx, CatalogLimits{10, 32 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer cat.Close()
	p, e := rh.Select(ctx, cat, Plan{DestinationRepositoryID: destID, BootstrapSHA256: fp, Target: Target{Kind: "time", Value: "2026-09-07T01:30:00Z", Timeline: 1}, Path: []Timeline{{ID: 1}}, RequiredArchive: []WALRange{{1, "0/1000100", "0/1800000"}}})
	if e != nil {
		t.Fatal(e)
	}
	if len(p.Chain) != 1 || p.Chain[0].BackupUID != full.Commit.BackupUID {
		t.Fatal("newest base after target selected")
	}
	dir := t.TempDir()
	if e = SavePlan(dir, p); e != nil {
		t.Fatal(e)
	}
	loaded, e := ReadPlan(dir)
	if e != nil {
		t.Fatal(e)
	}
	rr, e := Open(ctx, s, identity(), r.workspace)
	if e != nil {
		t.Fatal(e)
	}
	fresh, e := rr.AdmitRestore(ctx, targetID, op)
	if e != nil {
		t.Fatal(e)
	}
	loaded, e = fresh.ValidatePlan(ctx, loaded)
	if e != nil || loaded.ReaderHoldID == p.ReaderHoldID {
		t.Fatal("reader incarnation adoption", e)
	}
	p2, e := rh.Select(ctx, cat, Plan{DestinationRepositoryID: destID, BootstrapSHA256: fp, Target: Target{Kind: "latest", Timeline: 1}, Path: []Timeline{{ID: 1}}, RequiredArchive: []WALRange{{1, "0/2000100", "0/3000000"}}})
	if e != nil {
		t.Fatal(e)
	}
	if len(p2.Chain) != 2 || p2.Chain[1].BackupUID != diff.Commit.BackupUID {
		t.Fatal("not full+chosen differential")
	}
	for _, mutate := range []func(*Plan){func(p *Plan) { p.DestinationRepositoryID = repoID }, func(p *Plan) { p.Target.Timeline = 2 }, func(p *Plan) { p.RequiredArchive[0].StartLSN = "0/2000200" }, func(p *Plan) { p.Chain[1].RootBackupUID = p.Chain[1].BackupUID }} {
		b, _ := json.Marshal(p2)
		var copy Plan
		json.Unmarshal(b, &copy)
		mutate(&copy)
		if copy.Validate() == nil {
			t.Fatal("invalid persisted plan accepted")
		}
	}
	if e = rr.ReleaseLifetimeAfterTermination(ctx, targetID, op); e != nil {
		t.Fatal(e)
	}
	if _, e = fresh.ValidatePlan(ctx, loaded); e != ErrBlocked {
		t.Fatal("terminal source plan usable", e)
	}
}
func TestPlanForkAndTargetRules(t *testing.T) {
	r, _ := setup(t)
	_, full := publish(t, r, backupID, "data")
	fp := digest([]byte("bootstrap"))
	op, _ := RestoreOperationID(targetID, fp)
	p := Plan{Schema: 1, Source: identity(), DestinationRepositoryID: destID, TargetClusterUID: targetID, BootstrapSHA256: fp, OperationID: op, LifetimeHoldID: op, ReaderHoldID: UUID(), Target: Target{Kind: "latest", Timeline: 2}, Path: []Timeline{{ID: 1}, {ID: 2, ForkLSN: ptr("0/2000000")}}, Chain: []Commit{full.Commit}, RequiredArchive: []WALRange{{1, "0/1000100", "0/2000000"}, {2, "0/2000000", "0/3000000"}}}
	if e := p.Validate(); e != nil {
		t.Fatal(e)
	}
	p.Path[1].ForkLSN = ptr("0/1000100")
	if p.Validate() == nil {
		t.Fatal("base at fork accepted")
	}
	for _, target := range []Target{{Kind: "name", Value: "point", Timeline: 1}, {Kind: "immediate", Timeline: 1}, {Kind: "xid", Value: "2", Timeline: 1, BackupUID: ptr(backupID)}, {Kind: "time", Value: "2026-09-07 01:00:00", Timeline: 1}, {Kind: "latest", Timeline: 0}, {Kind: "lsn", Value: "0/000001", Timeline: 1}} {
		if target.validate() == nil {
			t.Fatal("invalid target", target)
		}
	}
}
func TestStrictSchemasAndGzipBounds(t *testing.T) {
	r, _ := setup(t)
	_, res := publish(t, r, backupID, "data")
	valid := res.Bytes
	cases := [][]byte{append(bytes.Clone(valid), []byte(" {}")...), bytes.Replace(valid, []byte(`"schema":1`), []byte(`"schema":2`), 1), bytes.Replace(valid, []byte(`"schema":1`), []byte(`"schema":1,"schema":1`), 1), bytes.Replace(valid, []byte(`"schema":1`), []byte(`"Schema":1`), 1), bytes.Replace(valid, []byte(`"schema":1,`), nil, 1), bytes.Replace(valid, []byte(`"tablespaces":[]`), []byte(`"tablespaces":null`), 1), bytes.Replace(valid, []byte(`native label fixture`), []byte(`\ud800`), 1), bytes.Replace(valid, []byte(`"manifest_bytes":32`), []byte(`"manifest_bytes":67108865`), 1)}
	for i, b := range cases {
		if bytes.Equal(b, valid) {
			continue
		}
		if _, e := DecodeCommit(b, r.id); e == nil {
			t.Fatalf("strict case %d accepted", i)
		}
	}
	raw := bytes.Repeat([]byte("a"), 1<<20)
	var compressed bytes.Buffer
	gz, _ := gzip.NewWriterLevel(&compressed, 1)
	gz.Write(raw)
	gz.Close()
	b := compressed.Bytes()
	f := file(t, b)
	if e := verifyFile(ctx, f, int64(len(b)), digest(b), "gzip", int64(len(raw)), digest(raw)); e != nil {
		t.Fatal(e)
	}
	if e := verifyFile(ctx, f, int64(len(b)), digest(b), "gzip", 1024, digest(raw[:1024])); e == nil {
		t.Fatal("expansion bound not enforced")
	}
	double := append(bytes.Clone(b), b...)
	if e := verifyFile(ctx, file(t, double), int64(len(double)), digest(double), "gzip", int64(len(raw)), digest(raw)); e == nil {
		t.Fatal("multiple gzip members accepted")
	}
}
func TestInitializationDoesNotAdoptOrRepairUsedRepository(t *testing.T) {
	r, s := setup(t)
	_, _ = publish(t, r, backupID, "data")
	delete(s.objects, r.root+"gate.json")
	if e := initialize(ctx, s, identity(), r.workspace); e == nil {
		t.Fatal("recreated missing gate over payload")
	}
	delete(s.objects, r.root+"repository.json")
	if e := initialize(ctx, s, identity(), r.workspace); e == nil {
		t.Fatal("adopted unidentified payload")
	}
}
func TestLocalCloseWaitsForInFlightRead(t *testing.T) {
	r, _ := setup(t)
	h, _ := publish(t, r, backupID, "data")
	h.mu.Lock()
	done := make(chan error, 1)
	go func() { done <- h.Close(ctx) }()
	select {
	case <-done:
		t.Fatal("close skipped in-flight work")
	case <-time.After(10 * time.Millisecond):
	}
	h.mu.Unlock()
	if e := <-done; e != nil {
		t.Fatal(e)
	}
}
func TestCanceledGCOwnerNeverReleasesOnClock(t *testing.T) {
	r, s := setup(t)
	h, res := publish(t, r, backupID, "data")
	h.Close(ctx)
	g, _ := r.AcquireGC(ctx)
	s.failAt = s.n + 2
	s.fault = "delay"
	if e := g.Execute(ctx, gcplan(g, retire(res))); e == nil {
		t.Fatal("missing fault")
	}
	c, cancel := context.WithCancel(ctx)
	cancel()
	if e := g.Close(c); !errors.Is(e, ErrUncertain) {
		t.Fatal(e)
	}
	g.started = time.Now().Add(-24 * time.Hour)
	s.deliver()
	if _, e := r.AdmitRestore(ctx, targetID, UUID()); e != ErrBlocked {
		t.Fatal("clock/absence substituted drain", e)
	}
}
func TestNativeLimitsAndPathGrammar(t *testing.T) {
	r, _ := setup(t)
	_, res := publish(t, r, backupID, "data")
	for _, modify := range []func(*Commit){func(c *Commit) { c.ManifestBytes = MaxManifestBytes + 1 }, func(c *Commit) { c.Artifacts[0].StoredBytes = MaxArtifactBytes + 1 }, func(c *Commit) { c.Tablespaces = make([]Tablespace, 65) }, func(c *Commit) { c.Artifacts = make([]Artifact, 67) }} {
		b, _ := json.Marshal(res.Commit)
		var c Commit
		json.Unmarshal(b, &c)
		modify(&c)
		if c.validate(r.id) == nil {
			t.Fatal("native cap ignored")
		}
	}
	for _, name := range []string{"../000000010000000000000001", "000000010000000000000100", "000000000000000000000001", "00000001.history.gz", "000000010000000000000001.01000000.backup", "00000001000000000000000a"} {
		if _, e := r.WALKey(name); e == nil {
			t.Fatal("bad WAL path", name)
		}
	}
	for _, name := range []string{"000000010000000000000001", "00000002.history", "000000010000000000000001.00000028.backup"} {
		if _, e := r.WALKey(name); e != nil {
			t.Fatal(name, e)
		}
	}
	for _, key := range []string{r.root + "backups/" + backupID + "/attempts/" + UUID() + "/data/66.tar", r.root + "backups/" + backupID + "/v2.json"} {
		if _, _, ok := r.catalogKey(key); ok {
			t.Fatal("unsupported key", key)
		}
	}
}
func TestPlanFileBound(t *testing.T) {
	dir := t.TempDir()
	f, e := os.Create(dir + "/recovery.json")
	if e != nil {
		t.Fatal(e)
	}
	f.Truncate(commitLimit + 1)
	f.Close()
	if _, e = ReadPlan(dir); e != ErrCapacity {
		t.Fatal(fmt.Sprint(e))
	}
}
