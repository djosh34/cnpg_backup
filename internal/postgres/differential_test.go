package postgres

import (
	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"testing"
)

func TestDifferentialEligibility(t *testing.T) {
	c := &Capture{Snapshot: &configuration.CaptureSnapshot{Repository: &configuration.Snapshot{Spec: configuration.Spec{Native: configuration.Native{MaxReferenceAge: "192h"}}}}, before: serverState{Postmaster: "2026-09-01T00:00:00Z", Clock: "2026-09-03T00:00:00Z"}, control: captureControl{System: "123", Timeline: 1, Checksum: 1}}
	root := repository.Commit{Kind: "full", SystemIdentifier: "123", PostgresMajor: 18, ToolVersion: "18.6", Timeline: 1, ChecksumVersion: 1, CaptureInstanceUID: "pod", PostmasterStartedAt: c.before.Postmaster, StartedAt: "2026-09-02T00:00:00Z", StoppedAt: "2026-09-02T01:00:00Z"}
	if e := c.EligibleFull(root, "pod"); e != nil {
		t.Fatal(e)
	}
	for _, tc := range []struct {
		name   string
		change func(*repository.Commit)
	}{
		{"not-full", func(r *repository.Commit) { r.Kind = "differential" }},
		{"system", func(r *repository.Commit) { r.SystemIdentifier = "456" }},
		{"promotion", func(r *repository.Commit) { r.Timeline++ }},
		{"checksum", func(r *repository.Commit) { r.ChecksumVersion = 0 }},
		{"pod", func(r *repository.Commit) { r.CaptureInstanceUID = "other" }},
		{"restart", func(r *repository.Commit) { r.PostmasterStartedAt = "2026-09-01T01:00:00Z" }},
		{"age", func(r *repository.Commit) { r.StartedAt = "2026-08-01T00:00:00Z" }},
		{"clock", func(r *repository.Commit) { r.StoppedAt = "2026-09-04T00:00:00Z" }},
		{"version", func(r *repository.Commit) { r.ToolVersion = "18.5" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := root
			tc.change(&r)
			if c.EligibleFull(r, "pod") == nil {
				t.Fatal("ineligible reference accepted")
			}
		})
	}
}
