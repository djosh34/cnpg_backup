package cnpgi

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/postgres"
	"github.com/djosh34/cnpg_backup/internal/s3store"
)

func TestRecoveryPlanLargeBundleRoundTrip(t *testing.T) {
	plan := recoveryPlanFixture(t)
	plan.Materialized = true
	segmentBytes := plan.Plan.Source.WALSegmentBytes
	for i := range 3000 {
		name := postgres.WALFilename(1, uint64(i)*uint64(segmentBytes), segmentBytes)
		plan.Bundled[name] = s3store.Integrity{Size: segmentBytes, SHA256: testHash([]byte(name))}
	}
	dir := t.TempDir()
	if err := saveRecovery(dir, plan); err != nil {
		t.Fatal(err)
	}
	got, err := readRecovery(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, plan) {
		t.Fatal("saved plan changed on read")
	}
}

func TestRecoveryPlanReadSizeLimit(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "recovery.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(recoveryPlanLimit + 1); err != nil {
		t.Fatal(err)
	}
	if _, err := readRecovery(dir); err == nil {
		t.Fatal("read oversized plan")
	}
}
