package repository

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

var walRE = regexp.MustCompile(`^([0-9A-F]{24}|[0-9A-F]{8}\.history|[0-9A-F]{24}\.[0-9A-F]{8}\.backup)$`)

func walTimeline(name string, size int64) (uint32, error) {
	if !walRE.MatchString(name) {
		return 0, ErrInvalid
	}
	t, e := strconv.ParseUint(name[:8], 16, 32)
	if e != nil || t == 0 {
		return 0, ErrInvalid
	}
	if !strings.HasSuffix(name, ".history") {
		seg, _ := strconv.ParseUint(name[16:24], 16, 32)
		if seg >= uint64(1<<32/size) {
			return 0, ErrInvalid
		}
		if strings.HasSuffix(name, ".backup") {
			off, _ := strconv.ParseUint(name[25:33], 16, 32)
			if off >= uint64(size) {
				return 0, ErrInvalid
			}
		}
	}
	return uint32(t), nil
}

// RestoreOperationID is stable across Pods/Jobs but changes with immutable
// bootstrap semantics. fingerprint is the SHA256 of validated nonsecret input.
func RestoreOperationID(target, fingerprint string) (string, error) {
	if !validID(target) || !hashRE.MatchString(fingerprint) {
		return "", ErrInvalid
	}
	b := sha256.Sum256([]byte("cnpg-backup/restore/v1\x00" + target + "\x00" + fingerprint))
	b[6] = (b[6] & 15) | 80
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

type Target struct {
	Kind      string  `json:"kind"`
	Value     string  `json:"value"`
	Exclusive bool    `json:"exclusive"`
	BackupUID *string `json:"backup_uid"`
	Timeline  uint32  `json:"timeline"`
}

// Path is a single source-history ancestry, oldest first. ForkLSN is null only
// at the initial timeline. The later WAL module must derive it from verified
// history bytes, not LIST lexicographic order or target-cluster timelines.
type Timeline struct {
	ID      uint32  `json:"id"`
	ForkLSN *string `json:"fork_lsn"`
}
type Plan struct {
	Schema                  int        `json:"schema"`
	Source                  Identity   `json:"source"`
	DestinationRepositoryID string     `json:"destination_repository_id"`
	TargetClusterUID        string     `json:"target_cluster_uid"`
	BootstrapSHA256         string     `json:"bootstrap_sha256"`
	OperationID             string     `json:"operation_id"`
	LifetimeHoldID          string     `json:"lifetime_hold_id"`
	ReaderHoldID            string     `json:"reader_hold_id"`
	Target                  Target     `json:"target"`
	Path                    []Timeline `json:"path"`
	Chain                   []Commit   `json:"chain"`
	// RequiredArchive is exact post-bundle coverage frozen at admission. Segment
	// intersection (not filename membership) governs remote-required WAL.
	RequiredArchive []WALRange `json:"required_archive"`
}

func timeTarget(s string) (time.Time, error) {
	t, e := time.Parse(time.RFC3339Nano, s)
	if e != nil {
		return time.Time{}, ErrInvalid
	}
	return t, nil
}
func (t Target) validate() error {
	if t.Timeline == 0 || (t.BackupUID != nil && !validID(*t.BackupUID)) {
		return ErrInvalid
	}
	switch t.Kind {
	case "latest":
		if t.Value != "" || t.Exclusive {
			return ErrInvalid
		}
	case "time":
		if _, e := timeTarget(t.Value); e != nil {
			return e
		}
	case "lsn":
		if _, e := ParseLSN(t.Value); e != nil {
			return e
		}
	case "name":
		if !validText(t.Value, 63) || t.Exclusive || t.BackupUID == nil {
			return ErrInvalid
		}
	case "xid":
		n, ok := decimal(t.Value)
		if !ok || n < 3 || n > 1<<32-1 || t.BackupUID == nil {
			return ErrInvalid
		}
	case "immediate":
		if t.Value != "" || t.Exclusive || t.BackupUID == nil {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
func (p Plan) Validate() error {
	if p.Schema != 1 || p.Source.Validate() != nil || !validID(p.DestinationRepositoryID) || p.DestinationRepositoryID == p.Source.RepositoryID || !validID(p.TargetClusterUID) || p.TargetClusterUID == p.Source.WriterClusterUID || !validID(p.ReaderHoldID) {
		return ErrIdentity
	}
	op, e := RestoreOperationID(p.TargetClusterUID, p.BootstrapSHA256)
	if e != nil || op != p.OperationID || p.LifetimeHoldID != op {
		return ErrIdentity
	}
	if e = p.Target.validate(); e != nil {
		return e
	}
	if len(p.Chain) < 1 || len(p.Chain) > 2 || len(p.Path) < 1 || len(p.Path) > 1024 || p.RequiredArchive == nil || len(p.RequiredArchive) > 1024 {
		return ErrCapacity
	}
	for _, c := range p.Chain {
		if e = c.validate(p.Source); e != nil {
			return e
		}
	}
	if p.Chain[0].Kind != "full" {
		return ErrCorrupt
	}
	if len(p.Chain) == 2 {
		if e = validateParent(p.Chain[1], p.Chain[0]); e != nil {
			return e
		}
	}
	ids := map[uint32]int{}
	last := uint64(0)
	for i, t := range p.Path {
		if t.ID == 0 {
			return ErrInvalid
		}
		if _, ok := ids[t.ID]; ok {
			return ErrInvalid
		}
		ids[t.ID] = i
		if i == 0 {
			if t.ForkLSN != nil {
				return ErrInvalid
			}
		} else {
			if t.ID <= p.Path[i-1].ID || t.ForkLSN == nil {
				return ErrInvalid
			}
			n, e := ParseLSN(*t.ForkLSN)
			if e != nil || n < last {
				return ErrInvalid
			}
			last = n
		}
	}
	if p.Path[len(p.Path)-1].ID != p.Target.Timeline {
		return ErrInvalid
	}
	selected := p.Chain[len(p.Chain)-1]
	if !eligible(selected, p.Target, p.Path) {
		return ErrInvalid
	}
	// Coverage must begin at selected bundle End-LSN and remain contiguous to
	// a frontier on the resolved timeline, crossing only the exact history forks.
	pos, _ := ParseLSN(selected.BundledWALEndLSN)
	pi := ids[selected.Timeline]
	for _, w := range p.RequiredArchive {
		a, ae := ParseLSN(w.StartLSN)
		b, be := ParseLSN(w.EndLSN)
		if ae != nil || be != nil || a != pos || a >= b || w.Timeline != p.Path[pi].ID {
			return ErrInvalid
		}
		pos = b
		if pi+1 < len(p.Path) {
			fork, _ := ParseLSN(*p.Path[pi+1].ForkLSN)
			if b > fork {
				return ErrInvalid
			}
			if b == fork {
				pi++
			}
		}
	}
	if len(p.RequiredArchive) > 0 && pi != len(p.Path)-1 {
		return ErrInvalid
	}
	if p.Target.Kind != "immediate" && pi != len(p.Path)-1 {
		return ErrInvalid
	}
	return nil
}
func eligible(c Commit, t Target, path []Timeline) bool {
	if t.BackupUID != nil && *t.BackupUID != c.BackupUID {
		return false
	}
	idx := -1
	for i, v := range path {
		if v.ID == c.Timeline {
			idx = i
			break
		}
	}
	if idx < 0 {
		return false
	}
	end, _ := ParseLSN(c.StopLSN)
	if idx+1 < len(path) {
		if path[idx+1].ForkLSN == nil {
			return false
		}
		fork, e := ParseLSN(*path[idx+1].ForkLSN)
		if e != nil || end >= fork {
			return false
		}
	}
	switch t.Kind {
	case "time":
		stop, ok := stamp(c.StoppedAt)
		target, e := timeTarget(t.Value)
		return ok && e == nil && stop.Before(target)
	case "lsn":
		target, e := ParseLSN(t.Value)
		if e != nil || end >= target {
			return false
		}
		for i := idx + 1; i < len(path); i++ {
			if path[i].ForkLSN == nil {
				return false
			}
			fork, e := ParseLSN(*path[i].ForkLSN)
			if e != nil || target < fork {
				return false
			}
		}
	}
	return true
}

// Select uses a complete protected catalog. Target.Timeline and path/coverage
// must come from the WAL caller's verified history/continuous archive inventory.
// This primitive does not claim PostgreSQL has reached any requested target.
func (h *Hold) Select(ctx context.Context, catalog *Catalog, template Plan) (Plan, error) {
	if catalog == nil || catalog.hold != h || h.holder.Kind != "restore-reader" {
		return Plan{}, ErrIdentity
	}
	if template.Target.validate() != nil {
		return Plan{}, ErrInvalid
	}
	// Validate path shape before eligible can traverse it.
	if len(template.Path) == 0 || len(template.Path) > 1024 {
		return Plan{}, ErrInvalid
	}
	for i, t := range template.Path {
		if t.ID == 0 || (i == 0 && t.ForkLSN != nil) || (i > 0 && t.ForkLSN == nil) {
			return Plan{}, ErrInvalid
		}
	}
	var chosen *Commit
	e := catalog.Visit(ctx, func(en Entry) error {
		if en.Retired || !eligible(en.Commit, template.Target, template.Path) {
			return nil
		}
		if chosen == nil || newer(en.Commit, *chosen) {
			v := en.Commit
			chosen = &v
		}
		return nil
	})
	if e != nil {
		return Plan{}, e
	}
	if chosen == nil {
		return Plan{}, ErrBlocked
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if e = h.check(ctx); e != nil {
		return Plan{}, e
	}
	r := h.r
	c, b, _, e := r.readCommit(ctx, chosen.BackupUID)
	if e != nil {
		return Plan{}, e
	}
	if e = r.verifyWinner(ctx, c, b); e != nil {
		return Plan{}, e
	}
	template.Schema = 1
	template.Source = r.id
	template.TargetClusterUID = h.holder.TargetClusterUID
	template.OperationID = h.holder.OperationID
	template.LifetimeHoldID = h.holder.OperationID
	template.ReaderHoldID = h.holder.ID
	template.Chain = []Commit{c}
	if c.Kind == "differential" {
		p, _, _, e := r.readCommit(ctx, *c.ParentBackupUID)
		if e != nil {
			return Plan{}, e
		}
		template.Chain = []Commit{p, c}
	}
	if e = template.Validate(); e != nil {
		return Plan{}, e
	}
	return template, nil
}
func newer(a, b Commit) bool {
	ta, _ := stamp(a.StoppedAt)
	tb, _ := stamp(b.StoppedAt)
	if !ta.Equal(tb) {
		return ta.After(tb)
	}
	return a.BackupUID > b.BackupUID
}

// ValidatePlan reacquires no old process holder. The caller must first obtain a
// fresh AdmitRestore handle; it can reuse exact chain/target metadata with its
// own ReaderHoldID, never silently choose a new backup after partial work.
func (h *Hold) ValidatePlan(ctx context.Context, p Plan) (Plan, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e := h.check(ctx); e != nil {
		return Plan{}, e
	}
	if h.holder.Kind != "restore-reader" || p.Validate() != nil || p.Source != h.r.id || p.TargetClusterUID != h.holder.TargetClusterUID || p.OperationID != h.holder.OperationID {
		return Plan{}, ErrIdentity
	}
	for _, c := range p.Chain {
		got, b, _, e := h.r.readCommit(ctx, c.BackupUID)
		if e != nil {
			return Plan{}, e
		}
		a, _ := json.Marshal(c)
		bb, _ := json.Marshal(got)
		if digest(a) != digest(bb) {
			return Plan{}, ErrIdentity
		}
		if e = h.r.verifyWinner(ctx, got, b); e != nil {
			return Plan{}, e
		}
	}
	p.ReaderHoldID = h.holder.ID
	return p, nil
}

// SavePlan writes a nonsecret plan in a caller-owned private state directory
// OUTSIDE PGDATA, after local target ownership is established by recovery-guard.
// It never creates/clears guard markers or authorizes target takeover.
func SavePlan(directory string, p Plan) error {
	if p.Validate() != nil || !filepath.IsAbs(directory) {
		return ErrInvalid
	}
	b, e := json.Marshal(p)
	if e != nil {
		return e
	}
	if int64(len(b)) > commitLimit {
		return ErrCapacity
	}
	root, e := os.OpenRoot(directory)
	if e != nil {
		return e
	}
	defer root.Close()
	name := "recovery-" + UUID() + ".json"
	f, e := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer root.Remove(name)
	if _, e = f.Write(b); e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e != nil {
		return e
	}
	if ce != nil {
		return ce
	}
	if e = root.Rename(name, "recovery.json"); e != nil {
		return e
	}
	d, e := root.Open(".")
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func ReadPlan(directory string) (Plan, error) {
	var p Plan
	root, e := os.OpenRoot(directory)
	if e != nil {
		return p, e
	}
	defer root.Close()
	f, e := root.Open("recovery.json")
	if e != nil {
		return p, e
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil || !st.Mode().IsRegular() || st.Size() > commitLimit {
		return p, ErrCapacity
	}
	b, e := io.ReadAll(io.LimitReader(f, commitLimit+1))
	if e != nil {
		return p, e
	}
	if !utf8.Valid(b) {
		return p, ErrInvalid
	}
	if e = strict(b, commitLimit, &p); e != nil {
		return p, e
	}
	return p, p.Validate()
}
