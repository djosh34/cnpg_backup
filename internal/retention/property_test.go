package retention

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// Independent oracle enumerates recoverable target instants and reconstructs
// each surviving F/D graph. It never calls Plan or recomputes its keep union.
func recoveryOracle(in Inventory, p Decision, cutoff, now time.Time, minimum int) error {
	remaining := map[string]Backup{}
	roots, total := 0, 0
	for _, b := range in.Backups {
		if b.Parent == "" {
			total++
		}
		if p.Keep[b.ID] != "" {
			remaining[b.ID] = b
			if b.Parent == "" {
				roots++
			}
		}
	}
	if roots < min(minimum, total) {
		return fmt.Errorf("independent roots lost")
	}
	for _, b := range remaining {
		if b.Parent != "" {
			f, ok := remaining[b.Parent]
			if !ok || f.Parent != "" {
				return fmt.Errorf("parent lost")
			}
		}
		if b.Timeline == p.Current {
			for _, s := range in.Segments {
				if s.Timeline == b.Timeline && s.Start+in.SegmentBytes > b.Redo && s.Start < p.WALFloor {
					return fmt.Errorf("retained redo WAL lost")
				}
			}
		}
	}
	// Sample every hour and every completion boundary in the promised window.
	targets := []time.Time{}
	for at := cutoff; !at.After(now); at = at.Add(time.Hour) {
		targets = append(targets, at)
	}
	for _, b := range in.Backups {
		if !b.Completed.Before(cutoff) {
			targets = append(targets, b.Completed)
		}
	}
	for _, at := range targets {
		before, after := false, false
		for _, b := range in.Backups {
			if !b.Completed.After(at) {
				before = true
			}
		}
		for _, b := range remaining {
			if !b.Completed.After(at) {
				after = true
			}
		}
		if before && !after {
			return fmt.Errorf("target %s lost", at)
		}
	}
	return nil
}
func TestSeededBackupGraphIndependentOracle(t *testing.T) {
	now := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	for seed := int64(0); seed < 200; seed++ {
		rng := rand.New(rand.NewSource(seed))
		in := Inventory{SegmentBytes: 1 << 20, Paths: [][]Timeline{{{ID: 1}}}}
		parent := ""
		for i := 0; i < 60; i++ {
			id := fmt.Sprint(i)
			root := parent
			if i == 0 || rng.Intn(4) == 0 {
				root = ""
				parent = id
			}
			in.Backups = append(in.Backups, Backup{ID: id, Parent: root, Timeline: 1, Completed: now.Add(time.Duration(i-60) * time.Hour), Redo: uint64(i+1) << 20, Start: uint64(i+1) << 20, End: uint64(i+2) << 20})
		}
		for i := 0; i < 64; i++ {
			in.Segments = append(in.Segments, Segment{1, uint64(i) << 20, true})
		}
		cutoff := now.Add(-time.Duration(1+rng.Intn(48)) * time.Hour)
		minimum := 1 + rng.Intn(5)
		p, e := Plan(in, cutoff, minimum, nil)
		if e != nil {
			t.Fatalf("seed %d: %v", seed, e)
		}
		if e = recoveryOracle(in, p, cutoff, now, minimum); e != nil {
			t.Fatalf("seed %d: %v", seed, e)
		}
		rng.Shuffle(len(in.Backups), func(i, j int) { in.Backups[i], in.Backups[j] = in.Backups[j], in.Backups[i] })
		again, e := Plan(in, cutoff, minimum, nil)
		if e != nil || fmt.Sprint(p.Expire) != fmt.Sprint(again.Expire) || p.WALFloor != again.WALFloor {
			t.Fatalf("seed %d unstable ordering", seed)
		}
		// Negative control: losing a surviving full MUST be noticed by the oracle.
		damaged := p
		damaged.Keep = map[string]string{}
		if recoveryOracle(in, damaged, cutoff, now, minimum) == nil {
			t.Fatal("oracle blessed lost last root")
		}
	}
}
func TestTimelineForkFloorCoverageAndHints(t *testing.T) {
	now := time.Now().UTC()
	step := uint64(1 << 20)
	in := Inventory{SegmentBytes: step, Paths: [][]Timeline{{{ID: 1}}, {{ID: 1}, {ID: 2, Fork: 4*step + 17}}}, Backups: []Backup{{ID: "ancestor", Timeline: 1, Completed: now.Add(-2 * time.Hour), Redo: step, Start: step, End: 2 * step}}, Segments: []Segment{}}
	for i := 2; i < 5; i++ {
		in.Segments = append(in.Segments, Segment{1, uint64(i) * step, true})
	}
	for i := 4; i < 8; i++ {
		in.Segments = append(in.Segments, Segment{2, uint64(i) * step, true})
	}
	p, e := Plan(in, now.Add(-time.Hour), 1, &Hint{2, 7 * step})
	if e != nil || p.WALFloor != 4*step {
		t.Fatalf("fork floor weakened: %+v %v", p, e)
	}
	p, e = Plan(in, now.Add(-time.Hour), 1, &Hint{2, step})
	if e != nil || p.WALFloor != step {
		t.Fatal("stronger hint ignored", p, e)
	}
	in.Segments[3].Live = false
	if _, e = Plan(in, now.Add(-time.Hour), 1, nil); e == nil {
		t.Fatal("retired fork segment accepted as coverage")
	}
	in.Segments[3].Live = true
	in.Paths = append(in.Paths, []Timeline{{ID: 1}, {ID: 3, Fork: 3 * step}})
	if _, e = Plan(in, now.Add(-time.Hour), 1, nil); e == nil {
		t.Fatal("ambiguous writer branch authorized deletion")
	}
}
