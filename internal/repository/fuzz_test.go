package repository

import (
	"encoding/json"
	"strings"
	"testing"
)

func schemaFixture() Commit {
	h := strings.Repeat("a", 64)
	return Commit{Schema: 1, RepositoryID: repoID, BackupUID: backupID, AttemptID: destID, RequestSHA256: h, Kind: "full", RootBackupUID: backupID, SystemIdentifier: identity().SystemIdentifier, PostgresMajor: 18, ToolVersion: "18.6", Timeline: 1, ChecksumVersion: 1, CaptureInstanceUID: capturedID, PostmasterStartedAt: "2026-09-07T00:00:00Z", StartedAt: "2026-09-07T01:00:00Z", StoppedAt: "2026-09-07T01:01:00Z", StartLSN: "0/1000028", StopLSN: "0/1000100", RedoLSN: "0/1000028", BundledWALStartLSN: "0/1000028", BundledWALEndLSN: "0/1000100", WALRanges: []WALRange{{1, "0/1000028", "0/1000100"}}, BackupLabel: "START WAL LOCATION: 0/1000028\n", Tablespaces: []Tablespace{}, ManifestBytes: 1024, ManifestSHA256: h, Artifacts: []Artifact{{0, "base", nil, "none", 1024, 1024, h, h}, {1, "wal", nil, "none", 1024, 1024, h, h}}}
}
func FuzzCommitMetadata(f *testing.F) {
	b, _ := json.Marshal(schemaFixture())
	f.Add(b)
	f.Add([]byte(`{"schema":2}`))
	f.Add([]byte(`{"schema":1,"schema":1}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > int(commitLimit)+1 {
			return
		}
		c, e := DecodeCommit(b, identity())
		if e != nil {
			return
		}
		encoded, e := json.Marshal(c)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = DecodeCommit(encoded, identity()); e != nil {
			t.Fatal("accepted metadata not stable", e)
		}
		if len(c.Artifacts) > 66 || len(c.Tablespaces) > 64 || c.ManifestBytes > 64<<20 {
			t.Fatal("accepted out-of-envelope metadata")
		}
	})
}
func FuzzKeysAndLSNs(f *testing.F) {
	for _, s := range []string{"000000010000000000000001", "00000002.history", "0/1000028", "../bad", "000000010000000000000100", "0/FFFFFFFF"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 2048 {
			return
		}
		r := Repository{id: identity(), root: "v1/" + repoID + "/"}
		if key, e := r.WALKey(s); e == nil {
			if !strings.HasPrefix(key, r.root+"wal/") || strings.Contains(key, "..") || strings.Count(key, "/") != 4 {
				t.Fatal("key escaped", key)
			}
		}
		if n, e := ParseLSN(s); e == nil {
			if n == 0 && s != "0/0" {
				t.Fatal("noncanonical LSN")
			}
		}
	})
}
func FuzzParentGraph(f *testing.F) {
	b, _ := json.Marshal(schemaFixture())
	f.Add(b, b)
	child := schemaFixture()
	child.BackupUID = targetID
	child.Kind = "differential"
	child.ParentBackupUID = ptr(backupID)
	child.RootBackupUID = backupID
	child.RootManifestSHA256 = ptr(child.ManifestSHA256)
	child.StartLSN = "0/2000028"
	child.RedoLSN = child.StartLSN
	child.BundledWALStartLSN = child.StartLSN
	child.StopLSN = "0/2000100"
	child.BundledWALEndLSN = child.StopLSN
	child.WALRanges = []WALRange{{1, child.StartLSN, child.StopLSN}}
	child.StartedAt = "2026-09-07T02:00:00Z"
	child.StoppedAt = "2026-09-07T02:01:00Z"
	cb, _ := json.Marshal(child)
	f.Add(cb, b)
	f.Fuzz(func(t *testing.T, child, parent []byte) {
		if len(child) > int(commitLimit) || len(parent) > int(commitLimit) {
			return
		}
		c, ce := DecodeCommit(child, identity())
		p, pe := DecodeCommit(parent, identity())
		if ce != nil || pe != nil {
			return
		}
		if validateParent(c, p) == nil {
			if c.Kind != "differential" || p.Kind != "full" || c.BackupUID == p.BackupUID || c.RootBackupUID != p.BackupUID || c.CaptureInstanceUID != p.CaptureInstanceUID {
				t.Fatal("invalid full-parent edge accepted")
			}
		}
	})
}
func FuzzPlanMetadata(f *testing.F) {
	fp := strings.Repeat("a", 64)
	op, _ := RestoreOperationID(targetID, fp)
	p := Plan{Schema: 1, Source: identity(), DestinationRepositoryID: destID, TargetClusterUID: targetID, BootstrapSHA256: fp, OperationID: op, LifetimeHoldID: op, ReaderHoldID: capturedID, Target: Target{Kind: "latest", Timeline: 1}, Path: []Timeline{{ID: 1}}, Chain: []Commit{schemaFixture()}, RequiredArchive: []WALRange{{1, "0/1000100", "0/2000000"}}}
	b, _ := json.Marshal(p)
	f.Add(b)
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > int(commitLimit) {
			return
		}
		var p Plan
		if strict(b, commitLimit, &p) != nil || p.Validate() != nil {
			return
		}
		if p.Source.RepositoryID == p.DestinationRepositoryID || len(p.Chain) > 2 {
			t.Fatal("invalid plan accepted")
		}
		bundle, remote, e := p.Coverage("000000010000000000000001")
		_ = bundle
		_ = remote
		if e != nil {
			t.Fatal("valid plan coverage failed", e)
		}
	})
}
