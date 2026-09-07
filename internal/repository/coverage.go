package repository

import (
	"strconv"
	"strings"
)

// Coverage classifies PLAN requirements, not storage errors or local file
// availability. The WAL caller must always try the archive first, verify bytes,
// and may use bundle=true for ordinary-miss fallback only after verifying the
// actual guarded local file AND remote=false. Corruption/outage is never EOF.
func (p Plan) Coverage(name string) (bundle, remote bool, err error) {
	if err = p.Validate(); err != nil {
		return false, false, err
	}
	timeline, e := walTimeline(name, p.Source.WALSegmentBytes)
	if e != nil {
		return false, false, e
	}
	if strings.HasSuffix(name, ".history") {
		for _, t := range p.Path {
			if t.ID == timeline && timeline > 1 {
				return false, true, nil
			}
		}
		return false, false, nil
	}
	if len(name) != 24 {
		return false, false, nil
	}
	log, _ := strconv.ParseUint(name[8:16], 16, 32)
	segment, _ := strconv.ParseUint(name[16:24], 16, 32)
	start := log<<32 | segment*uint64(p.Source.WALSegmentBytes)
	intersects := func(w WALRange) bool {
		if w.Timeline != timeline {
			return false
		}
		a, _ := ParseLSN(w.StartLSN)
		b, _ := ParseLSN(w.EndLSN)
		return start < b && (a < start || a-start < uint64(p.Source.WALSegmentBytes))
	}
	c := p.Chain[len(p.Chain)-1]
	for _, w := range c.WALRanges {
		bundle = bundle || intersects(w)
	}
	for _, w := range p.RequiredArchive {
		remote = remote || intersects(w)
	}
	return bundle, remote, nil
}
