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
		for _, s := range in.Segments {
			if s.Live && s.Timeline == b.Timeline && s.Start+in.SegmentBytes > min(b.Redo, b.Start) && !survivesWAL(s, p) {
				return fmt.Errorf("retained redo WAL lost")
			}
		}
		for _, path := range in.Paths {
			if oracleOnPath(b, path) && !oracleReplay(in, b, path, p) {
				return fmt.Errorf("retained %s cannot replay on T%d", b.ID, path[len(path)-1].ID)
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
	for _, path := range in.Paths {
		beforeRoots, afterRoots := 0, 0
		for _, b := range in.Backups {
			if b.Parent == "" && oracleOnPath(b, path) {
				beforeRoots++
				if _, ok := remaining[b.ID]; ok {
					afterRoots++
				}
			}
		}
		if afterRoots < min(minimum, beforeRoots) {
			return fmt.Errorf("lineage full floor lost")
		}
		for _, at := range targets {
			before, after := false, false
			for _, b := range in.Backups {
				if !b.Completed.After(at) && oracleOnPath(b, path) {
					before = true
				}
			}
			for _, b := range remaining {
				if !b.Completed.After(at) && oracleOnPath(b, path) && oracleReplay(in, b, path, p) {
					after = true
				}
			}
			if before && !after {
				return fmt.Errorf("target %s on T%d lost", at, path[len(path)-1].ID)
			}
		}
	}
	return nil
}

func survivesWAL(s Segment, p Decision) bool {
	return s.Live && !(s.Timeline == p.Current && s.Start < p.WALFloor)
}

func oracleOnPath(b Backup, path []Timeline) bool {
	for i, node := range path {
		if node.ID == b.Timeline {
			return i == len(path)-1 || b.End < path[i+1].Fork
		}
	}
	return false
}

// Walk backwards from each lineage's archived frontier to the selected bundle.
// Check actual surviving segment coverage of every interval, including the
// partial segment containing each fork. No production reachability/floor helper.
func oracleReplay(in Inventory, b Backup, path []Timeline, p Decision) bool {
	leaf := path[len(path)-1].ID
	end := uint64(0)
	for _, s := range in.Segments {
		if s.Timeline == leaf {
			end = max(end, s.Start+in.SegmentBytes)
		}
	}
	for i := len(path) - 1; i >= 0; i-- {
		node := path[i]
		start := node.Fork
		if node.ID == b.Timeline {
			start = b.End
		}
		for pos := start; pos < end; {
			covered := false
			for _, s := range in.Segments {
				if s.Timeline == node.ID && s.Start <= pos && pos-s.Start < in.SegmentBytes && survivesWAL(s, p) {
					pos = s.Start + in.SegmentBytes
					covered = true
					break
				}
			}
			if !covered {
				return false
			}
		}
		if node.ID == b.Timeline {
			return true
		}
		end = node.Fork
	}
	return false
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

func TestIndependentOracleRejectsLostForkWAL(t *testing.T) {
	now := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	step := uint64(1 << 20)
	in := Inventory{SegmentBytes: step, Paths: [][]Timeline{{{ID: 1}}, {{ID: 1}, {ID: 2, Fork: 4*step + 17}}}, Backups: []Backup{{ID: "ancestor", Timeline: 1, Completed: now.Add(-2 * time.Hour), Redo: step, Start: step, End: 2 * step}}}
	for i := 2; i < 5; i++ {
		in.Segments = append(in.Segments, Segment{1, uint64(i) * step, true})
	}
	for i := 4; i < 8; i++ {
		in.Segments = append(in.Segments, Segment{2, uint64(i) * step, true})
	}
	p, e := Plan(in, now.Add(-time.Hour), 1, nil)
	if e != nil {
		t.Fatal(e)
	}
	if e = recoveryOracle(in, p, now.Add(-time.Hour), now, 1); e != nil {
		t.Fatal(e)
	}
	p.WALFloor = 8 * step
	if recoveryOracle(in, p, now.Add(-time.Hour), now, 1) == nil {
		t.Fatal("oracle accepted loss of required T2 fork WAL for surviving ancestor")
	}
}

func TestGeneratedForkGraphsIndependentRecoveryOracle(t *testing.T) {
	now := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	step := uint64(1 << 20)
	for seed := int64(0); seed < 80; seed++ {
		rng := rand.New(rand.NewSource(seed))
		in := Inventory{SegmentBytes: step}
		path := []Timeline{{ID: 1}}
		levels := 2 + rng.Intn(3)
		for level := 0; level < levels; level++ {
			if level > 0 {
				path = append(path, Timeline{ID: uint32(level + 1), Fork: uint64(level*16)*step + uint64(1+rng.Intn(100))})
			}
			in.Paths = append(in.Paths, append([]Timeline(nil), path...))
			parent := ""
			for i := 0; i < 6; i++ {
				id := fmt.Sprintf("%d-%d", level, i)
				root := parent
				if i == 0 || rng.Intn(3) == 0 {
					root = ""
					parent = id
				}
				start := uint64(level*16+i*2+1) * step
				in.Backups = append(in.Backups, Backup{ID: id, Parent: root, Timeline: uint32(level + 1), Completed: now.Add(time.Duration(level*12+i-levels*12) * time.Hour), Start: start, Redo: start, End: start + step})
			}
			for i := level * 16; i <= (level+1)*16; i++ {
				in.Segments = append(in.Segments, Segment{uint32(level + 1), uint64(i) * step, true})
			}
		}
		cutoff := now.Add(-time.Duration(1+rng.Intn(levels*12)) * time.Hour)
		minimum := 1 + rng.Intn(3)
		p, e := Plan(in, cutoff, minimum, nil)
		if e != nil {
			t.Fatalf("seed=%d %v", seed, e)
		}
		if e = recoveryOracle(in, p, cutoff, now, minimum); e != nil {
			t.Fatalf("seed=%d %v", seed, e)
		}
		damaged := p
		damaged.WALFloor = uint64(levels*16+1) * step
		if recoveryOracle(in, damaged, cutoff, now, minimum) == nil {
			t.Fatalf("seed=%d oracle accepted missing leaf WAL", seed)
		}
		// Descendant bases cannot replace recovery on the original lineage,
		// even if the global full count and timestamp-only target checks pass.
		damaged = p
		damaged.Keep = map[string]string{}
		for id, why := range p.Keep {
			damaged.Keep[id] = why
		}
		for _, b := range in.Backups {
			if b.Timeline == 1 {
				delete(damaged.Keep, b.ID)
			}
		}
		if recoveryOracle(in, damaged, cutoff, now, minimum) == nil {
			t.Fatalf("seed=%d oracle accepted lost lineage roots", seed)
		}
		// Generated competing leaves are unknown writer coverage, not entitlement.
		in.Paths = append(in.Paths, []Timeline{{ID: 1}, {ID: uint32(levels + 1), Fork: 15 * step}})
		if _, e = Plan(in, cutoff, minimum, nil); e == nil {
			t.Fatalf("seed=%d competing fork accepted", seed)
		}
	}
}
