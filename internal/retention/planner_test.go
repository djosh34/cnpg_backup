package retention

import (
	"testing"
	"time"
)

func TestOldFullSupportsRecentDifferentialAndLastRoot(t *testing.T) {
	now := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	input := Inventory{SegmentBytes: 1 << 24, Paths: [][]Timeline{{{ID: 1}}}, Backups: []Backup{
		{ID: "old", Timeline: 1, Completed: now.Add(-30 * 24 * time.Hour), Start: 1 << 24, Redo: 1 << 24, End: 2 << 24},
		{ID: "parent", Timeline: 1, Completed: now.Add(-10 * 24 * time.Hour), Start: 2 << 24, Redo: 2 << 24, End: 3 << 24},
		{ID: "recent", Parent: "parent", Timeline: 1, Completed: now.Add(-24 * time.Hour), Start: 3 << 24, Redo: 3 << 24, End: 4 << 24},
	}}
	p, err := Plan(input, now.Add(-7*24*time.Hour), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.Keep["parent"] == "" || p.Keep["recent"] == "" || len(p.Expire) != 1 || p.Expire[0] != "old" || p.WALFloor != 2<<24 {
		t.Fatalf("unsafe plan: %+v", p)
	}
	input.Backups = input.Backups[:1]
	p, err = Plan(input, now.Add(-7*24*time.Hour), 1, nil)
	if err != nil || len(p.Expire) != 0 || p.Keep["old"] == "" {
		t.Fatalf("last root lost: %+v %v", p, err)
	}
}
