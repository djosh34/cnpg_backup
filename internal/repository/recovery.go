package repository

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

// ReadSource binds an external WAL I/O operation to this live reader. Returning
// from fn means all its requests/local publications have actually completed.
// No caller may retain this permission or dispatch asynchronous work from fn.
func (h *Hold) ReadSource(ctx context.Context, fn func(context.Context) error) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.holder.Kind != "restore-reader" || fn == nil {
		return ErrIdentity
	}
	if e := h.check(ctx); e != nil {
		return e
	}
	return fn(ctx)
}

// ArchiveNames returns only a complete bounded inventory. Retired slots are
// deliberately included: their required presence must subsequently fail rather
// than silently shortening the known frontier. Actual bytes are verified on read.
func (h *Hold) ArchiveNames(ctx context.Context) ([]string, error) {
	var names []string
	e := h.ReadSource(ctx, func(ctx context.Context) error {
		prefix := h.r.root + "wal/"
		return h.r.store.List(ctx, prefix, MaxCatalogRecords, func(i s3store.Info) error {
			p := strings.Split(strings.TrimPrefix(i.Key, prefix), "/")
			if !strings.HasPrefix(i.Key, prefix) || len(p) != 2 || len(p[0]) != 8 || len(names) >= MaxCatalogRecords {
				return ErrCorrupt
			}
			t, e := walTimeline(p[1], h.r.id.WALSegmentBytes)
			if e != nil || p[0] != fmt.Sprintf("%08X", t) {
				return ErrCorrupt
			}
			names = append(names, p[1])
			return nil
		})
	})
	if e != nil {
		return nil, e
	}
	return names, nil
}

// ParseHistory parses authenticated PostgreSQL history (ancestor, switch LSN,
// reason), producing numeric ancestry with no lexical timeline assumptions.
func ParseHistory(timeline uint32, b []byte) ([]Timeline, error) {
	if timeline <= 1 || len(b) == 0 || len(b) > 1<<20 {
		return nil, ErrInvalid
	}
	path := []Timeline{}
	var previous uint32
	var last uint64
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 || len(path) >= 1023 {
			return nil, ErrCorrupt
		}
		n, e := strconv.ParseUint(fields[0], 10, 32)
		end, e2 := ParseLSN(fields[1])
		if e != nil || e2 != nil || n == 0 || uint32(n) <= previous || uint32(n) >= timeline || (len(path) > 0 && end < last) {
			return nil, ErrCorrupt
		}
		var fork *string
		if len(path) > 0 {
			v := fmt.Sprintf("%X/%X", last>>32, last&math.MaxUint32)
			fork = &v
		}
		path = append(path, Timeline{ID: uint32(n), ForkLSN: fork})
		previous, last = uint32(n), end
	}
	if len(path) == 0 {
		return nil, ErrCorrupt
	}
	fork := fmt.Sprintf("%X/%X", last>>32, last&math.MaxUint32)
	return append(path, Timeline{ID: timeline, ForkLSN: &fork}), nil
}

// Resolve selects a full or differential by the existing strict eligibility
// rules, then freezes continuous post-bundle coverage through the admission
// inventory. readHistory must authenticate archive bytes under this holder.
// Missing histories, competing leaves and any interior remote gap fail closed.
func (h *Hold) Resolve(ctx context.Context, catalog *Catalog, template Plan, names []string, checkInput func(Commit) error, readHistory func(context.Context, string) ([]byte, error)) (Plan, error) {
	if catalog == nil || catalog.hold != h || readHistory == nil || checkInput == nil || len(names) > MaxCatalogRecords {
		return Plan{}, ErrInvalid
	}
	timelines := map[uint32]bool{}
	segments := map[uint32]map[uint64]bool{}
	historyNames := map[uint32]bool{}
	for _, name := range names {
		t, e := walTimeline(name, h.r.id.WALSegmentBytes)
		if e != nil {
			return Plan{}, e
		}
		// Promotion preserves an incomplete old-timeline file under a distinct
		// auxiliary name. It proves neither complete WAL nor timeline ancestry.
		if strings.HasSuffix(name, ".partial") {
			continue
		}
		timelines[t] = true
		if strings.HasSuffix(name, ".history") {
			historyNames[t] = true
		}
		if len(name) == 24 {
			log, _ := strconv.ParseUint(name[8:16], 16, 32)
			seg, _ := strconv.ParseUint(name[16:24], 16, 32)
			start := log<<32 | seg*uint64(h.r.id.WALSegmentBytes)
			if segments[t] == nil {
				segments[t] = map[uint64]bool{}
			}
			segments[t][start] = true
		}
	}
	if e := catalog.Visit(ctx, func(en Entry) error {
		if !en.Retired {
			timelines[en.Commit.Timeline] = true
		}
		return nil
	}); e != nil {
		return Plan{}, e
	}
	if template.Target.Timeline != 0 {
		timelines[template.Target.Timeline] = true
	}
	ids := make([]uint32, 0, len(timelines))
	for t := range timelines {
		ids = append(ids, t)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) == 0 || len(ids) > 1024 {
		return Plan{}, ErrCapacity
	}
	paths := map[uint32][]Timeline{1: {{ID: 1}}}
	ancestor := map[uint32]bool{}
	for _, t := range ids {
		if t == 1 {
			continue
		}
		if !historyNames[t] {
			return Plan{}, ErrCorrupt
		}
		b, e := readHistory(ctx, fmt.Sprintf("%08X.history", t))
		if e != nil {
			return Plan{}, e
		}
		p, e := ParseHistory(t, b)
		if e != nil {
			return Plan{}, e
		}
		paths[t] = p
		for i, a := range p[:len(p)-1] {
			if a.ID > 1 && !historyNames[a.ID] {
				return Plan{}, ErrCorrupt
			}
			ancestor[a.ID] = true
			if known, ok := paths[a.ID]; ok && !reflect.DeepEqual(known, p[:i+1]) {
				return Plan{}, ErrCorrupt
			}
		}
	}
	if template.Target.Timeline == 0 {
		for _, t := range ids {
			if !ancestor[t] {
				if template.Target.Timeline != 0 {
					return Plan{}, ErrBlocked
				}
				template.Target.Timeline = t
			}
		}
	}
	template.Path = paths[template.Target.Timeline]
	if len(template.Path) == 0 || template.Target.validate() != nil {
		return Plan{}, ErrInvalid
	}
	var selected *Commit
	if e := catalog.Visit(ctx, func(en Entry) error {
		if !en.Retired && eligible(en.Commit, template.Target, template.Path) && (selected == nil || newer(en.Commit, *selected)) {
			c := en.Commit
			selected = &c
		}
		return nil
	}); e != nil {
		return Plan{}, e
	}
	if selected == nil {
		return Plan{}, ErrBlocked
	}
	// Native caller rejects unsupported type/byte budgets BEFORE Select's
	// actual payload verification can allocate an over-budget spool.
	if e := checkInput(*selected); e != nil {
		return Plan{}, e
	}
	template.RequiredArchive = []WALRange{}
	if template.Target.Kind != "immediate" {
		pos, _ := ParseLSN(selected.BundledWALEndLSN)
		for i, t := range template.Path {
			if t.ID < selected.Timeline {
				continue
			}
			frontier := pos
			if i+1 < len(template.Path) {
				frontier, _ = ParseLSN(*template.Path[i+1].ForkLSN)
			} else {
				for start := range segments[t.ID] {
					if start > math.MaxUint64-uint64(h.r.id.WALSegmentBytes) {
						return Plan{}, ErrCapacity
					}
					frontier = max(frontier, start+uint64(h.r.id.WALSegmentBytes))
				}
			}
			if frontier > pos {
				step := uint64(h.r.id.WALSegmentBytes)
				count := uint64(0)
				for start := pos / step * step; start < frontier; {
					count++
					if count > MaxCatalogRecords || !segments[t.ID][start] {
						return Plan{}, ErrBlocked
					}
					if start > math.MaxUint64-step {
						return Plan{}, ErrCapacity
					}
					start += step
				}
				template.RequiredArchive = append(template.RequiredArchive, WALRange{Timeline: t.ID, StartLSN: fmt.Sprintf("%X/%X", pos>>32, pos&math.MaxUint32), EndLSN: fmt.Sprintf("%X/%X", frontier>>32, frontier&math.MaxUint32)})
			}
			pos = frontier
		}
	}
	// Select verifies the exact winning input and binds all holder identities.
	return h.Select(ctx, catalog, template)
}
