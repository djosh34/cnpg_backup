// Package retention separates pure recovery obligations from gate-owned I/O.
package retention

import (
	"errors"
	"sort"
	"time"
)

var ErrCoverage = errors.New("retention blocked: incomplete or inconsistent recovery coverage")

// Compact, validated catalog facts; native manifests/payloads stay on disk in
// repository. The caller must supply a complete inventory under exclusive GC.
type Backup struct {
	ID, Parent       string
	Timeline         uint32
	Completed        time.Time
	Start, Redo, End uint64
}
type Timeline struct {
	ID   uint32
	Fork uint64
}
type Segment struct {
	Timeline uint32
	Start    uint64
	Live     bool
}
type Inventory struct {
	Backups      []Backup
	Paths        [][]Timeline
	Segments     []Segment
	SegmentBytes uint64
}
type Hint struct {
	Timeline uint32
	Start    uint64
}
type Decision struct {
	Keep      map[string]string
	Expire    []string
	Current   uint32
	WALFloor  uint64
	Shortened bool
}

// Plan never uses object age or lexical cross-timeline ordering. Paths include
// every known timeline, including abandoned branches. With competing leaves we
// cannot establish the current writer timeline and refuse destructive work.
func Plan(in Inventory, cutoff time.Time, minimum int, hint *Hint) (Decision, error) {
	p := Decision{Keep: map[string]string{}}
	fail := func() (Decision, error) { return Decision{}, ErrCoverage }
	step := in.SegmentBytes
	if cutoff.IsZero() || minimum < 1 || minimum > 100 || step < 1<<20 || step > 1<<30 || step&(step-1) != 0 || len(in.Backups) == 0 || len(in.Paths) == 0 || len(in.Paths) > 1024 {
		return fail()
	}
	paths := map[uint32][]Timeline{}
	ancestors := map[uint32]bool{}
	for _, path := range in.Paths {
		if len(path) == 0 || len(path) > 1024 || path[0].Fork != 0 {
			return fail()
		}
		for i, t := range path {
			if t.ID == 0 || i > 0 && (t.ID <= path[i-1].ID || t.Fork < path[i-1].Fork) {
				return fail()
			}
			if i < len(path)-1 {
				ancestors[t.ID] = true
			}
		}
		id := path[len(path)-1].ID
		if paths[id] != nil {
			return fail()
		}
		paths[id] = path
	}
	for id, path := range paths {
		for i, t := range path {
			prefix := paths[t.ID]
			if len(prefix) != i+1 {
				return fail()
			}
			for j := range prefix {
				if prefix[j] != path[j] {
					return fail()
				}
			}
		}
		if !ancestors[id] {
			if p.Current != 0 {
				return fail()
			}
			p.Current = id
		}
	}
	byID := map[string]Backup{}
	for _, b := range in.Backups {
		if b.ID == "" || b.Completed.IsZero() || b.Start >= b.End || b.Redo > b.Start || paths[b.Timeline] == nil {
			return fail()
		}
		if _, ok := byID[b.ID]; ok {
			return fail()
		}
		byID[b.ID] = b
	}
	for _, b := range in.Backups {
		if b.Parent != "" {
			f, ok := byID[b.Parent]
			if !ok || f.Parent != "" || f.Timeline != b.Timeline || f.Completed.After(b.Completed) || f.End > b.Start {
				return fail()
			}
		}
	}
	reachable := func(b Backup, path []Timeline) bool {
		for i, t := range path {
			if t.ID == b.Timeline {
				return i == len(path)-1 || b.End < path[i+1].Fork
			}
		}
		return false
	}
	newer := func(a, b Backup) bool {
		if a.Completed.Equal(b.Completed) {
			return a.ID > b.ID
		}
		return a.Completed.After(b.Completed)
	}
	for _, path := range in.Paths {
		candidates := []Backup{}
		for _, b := range in.Backups {
			if reachable(b, path) {
				candidates = append(candidates, b)
			}
		}
		if len(candidates) == 0 {
			continue
		}
		sort.Slice(candidates, func(i, j int) bool { return newer(candidates[i], candidates[j]) })
		roots, anchor := 0, false
		for _, b := range candidates {
			if !b.Completed.Before(cutoff) {
				p.Keep[b.ID] = "inside recovery window"
			}
			if !anchor && !b.Completed.After(cutoff) {
				p.Keep[b.ID] = "window anchor"
				anchor = true
			}
			if b.Parent == "" && roots < minimum {
				p.Keep[b.ID] = "minimum full roots"
				roots++
			}
		}
		p.Keep[candidates[0].ID] = "latest usable backup"
		if !anchor {
			p.Shortened = true
			p.Keep[candidates[len(candidates)-1].ID] = "earliest usable; shortened window"
		}
	}
	for id := range p.Keep {
		if parent := byID[id].Parent; parent != "" {
			p.Keep[parent] = "full parent of retained differential"
		}
	}
	for _, b := range in.Backups {
		if p.Keep[b.ID] == "" {
			p.Expire = append(p.Expire, b.ID)
		}
	}
	sort.Slice(p.Expire, func(i, j int) bool {
		a, b := byID[p.Expire[i]], byID[p.Expire[j]]
		if (a.Parent != "") != (b.Parent != "") {
			return a.Parent != ""
		}
		return a.ID < b.ID
	})
	live := map[uint32]map[uint64]bool{}
	frontier := map[uint32]uint64{}
	for _, s := range in.Segments {
		if paths[s.Timeline] == nil || s.Start%step != 0 || s.Start > ^uint64(0)-step {
			return fail()
		}
		if live[s.Timeline] == nil {
			live[s.Timeline] = map[uint64]bool{}
		}
		if _, ok := live[s.Timeline][s.Start]; ok {
			return fail()
		}
		live[s.Timeline][s.Start] = s.Live
		frontier[s.Timeline] = max(frontier[s.Timeline], s.Start+step)
	}
	p.WALFloor = ^uint64(0)
	for id := range p.Keep {
		b := byID[id]
		for _, path := range in.Paths {
			if !reachable(b, path) {
				continue
			}
			pos := b.End
			started := false
			for i, t := range path {
				if t.ID == b.Timeline {
					started = true
				}
				if !started {
					continue
				}
				end := max(pos, frontier[t.ID])
				if i+1 < len(path) {
					end = path[i+1].Fork
				}
				// Range length is bounded by the inventory, never by hostile LSN span.
				if end < pos || (end-pos)/step > uint64(len(in.Segments)) {
					return fail()
				}
				if end > pos {
					for start := pos / step * step; start < end; start += step {
						if !live[t.ID][start] {
							return fail()
						}
					}
				}
				if t.ID == p.Current {
					floor := min(b.Start, b.Redo)
					if b.Timeline != p.Current {
						floor = t.Fork
					}
					p.WALFloor = min(p.WALFloor, floor/step*step)
				}
				pos = end
			}
		}
	}
	if p.WALFloor == ^uint64(0) {
		return fail()
	}
	if hint != nil && hint.Timeline == p.Current {
		p.WALFloor = min(p.WALFloor, hint.Start/step*step)
	}
	return p, nil
}
