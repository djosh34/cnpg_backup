package repository

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"sync"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

type Holder struct {
	ID               string  `json:"id"`
	Kind             string  `json:"kind"`
	TargetClusterUID string  `json:"target_cluster_uid"`
	OperationID      string  `json:"operation_id"`
	ProcessID        *string `json:"process_id"`
}
type Owner struct {
	OperationID string `json:"operation_id"`
	ProcessID   string `json:"process_id"`
	Kind        string `json:"kind"`
}
type Gate struct {
	Schema       int      `json:"schema"`
	RepositoryID string   `json:"repository_id"`
	Generation   string   `json:"generation"`
	Nonce        string   `json:"nonce"`
	Owner        *Owner   `json:"owner"`
	Holders      []Holder `json:"holders"`
}

func (g Gate) validate(id Identity) error {
	if g.Schema != 1 || g.RepositoryID != id.RepositoryID || !validID(g.Nonce) || g.Holders == nil {
		return ErrCorrupt
	}
	if _, ok := decimal(g.Generation); !ok {
		return ErrCorrupt
	}
	if len(g.Holders) > 1024 {
		return ErrCapacity
	}
	if g.Owner != nil && (!validID(g.Owner.OperationID) || !validID(g.Owner.ProcessID) || g.Owner.Kind != "gc" || len(g.Holders) != 0) {
		return ErrCorrupt
	}
	last := ""
	for _, h := range g.Holders {
		if h.validate() != nil || h.ID <= last {
			return ErrCorrupt
		}
		last = h.ID
	}
	return nil
}
func (h Holder) validate() error {
	if !validID(h.ID) || !validID(h.TargetClusterUID) || !validID(h.OperationID) {
		return ErrInvalid
	}
	switch h.Kind {
	case "restore-lifetime":
		if h.ProcessID != nil || h.ID != h.OperationID {
			return ErrInvalid
		}
	case "backup", "restore-reader":
		if h.ProcessID == nil || !validID(*h.ProcessID) {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
func sameHolder(a, b Holder) bool {
	if a.ID != b.ID || a.Kind != b.Kind || a.TargetClusterUID != b.TargetClusterUID || a.OperationID != b.OperationID {
		return false
	}
	if a.ProcessID == nil || b.ProcessID == nil {
		return a.ProcessID == nil && b.ProcessID == nil
	}
	return *a.ProcessID == *b.ProcessID
}
func (r *Repository) readGate(ctx context.Context) (Gate, s3store.Info, error) {
	var g Gate
	b, i, e := r.store.Read(ctx, r.root+"gate.json", gateLimit)
	if e != nil {
		return g, i, e
	}
	if e = strict(b, gateLimit, &g); e != nil {
		return g, i, e
	}
	return g, i, g.validate(r.id)
}

// changeGate's explicit no-op generation/nonce barrier fences delayed requests
// after ambiguity. Absence in GET alone is NEVER evidence a CAS did not apply.
func (r *Repository) changeGate(ctx context.Context, change func(*Gate) (bool, error)) error {
	barrier := false
	for round := 0; round < 10; round++ {
		g, i, e := r.readGate(ctx)
		if e != nil {
			return e
		}
		if !barrier {
			done, e := change(&g)
			if e != nil {
				return e
			}
			if done {
				return nil
			}
		}
		n, _ := decimal(g.Generation)
		if n == math.MaxUint64 {
			return ErrCapacity
		}
		g.Generation = strconv.FormatUint(n+1, 10)
		g.Nonce = UUID()
		sort.Slice(g.Holders, func(i, j int) bool { return g.Holders[i].ID < g.Holders[j].ID })
		if e = g.validate(r.id); e != nil {
			return e
		}
		b, _ := json.Marshal(g)
		if int64(len(b)) > gateLimit {
			return ErrCapacity
		}
		_, e = r.put(ctx, r.root+"gate.json", b, s3store.Condition{Match: i.ETag})
		if e == nil {
			if barrier {
				barrier = false
				continue
			}
			return nil
		}
		if ambiguous(e) {
			barrier = true
			continue
		}
		if s3store.Is(e, s3store.Precondition) || s3store.Is(e, s3store.Conflict) {
			continue
		}
		return e
	}
	return ErrContention
}
func (r *Repository) addHolder(ctx context.Context, h Holder) error {
	if h.validate() != nil {
		return ErrInvalid
	}
	return r.changeGate(ctx, func(g *Gate) (bool, error) {
		for _, v := range g.Holders {
			if v.ID == h.ID {
				if !sameHolder(v, h) {
					return false, ErrIdentity
				}
				return true, nil
			}
		}
		if g.Owner != nil {
			return false, ErrBlocked
		}
		if len(g.Holders) == 1024 {
			return false, ErrCapacity
		}
		g.Holders = append(g.Holders, h)
		return false, nil
	})
}
func (r *Repository) removeHolder(ctx context.Context, h Holder) error {
	return r.changeGate(ctx, func(g *Gate) (bool, error) {
		for i, v := range g.Holders {
			if v.ID == h.ID {
				if !sameHolder(v, h) {
					return false, ErrIdentity
				}
				g.Holders = append(g.Holders[:i], g.Holders[i+1:]...)
				return false, nil
			}
		}
		return true, nil
	})
}

// Hold serializes local work with its irreversible close boundary. A crashed
// handle is never reconstructed. Old holds block GC, not new source readers.
type Hold struct {
	mu                sync.Mutex
	r                 *Repository
	holder            Holder
	closed, uncertain bool
}

func (r *Repository) AdmitBackup(ctx context.Context, writer, operation string) (*Hold, error) {
	if writer != r.id.WriterClusterUID || !validID(operation) {
		return nil, ErrIdentity
	}
	return r.admit(ctx, "backup", writer, operation)
}
func (r *Repository) admit(ctx context.Context, kind, target, operation string) (*Hold, error) {
	p := r.process
	h := Holder{ID: UUID(), Kind: kind, TargetClusterUID: target, OperationID: operation, ProcessID: &p}
	if e := r.addHolder(ctx, h); e != nil {
		return nil, e
	}
	return &Hold{r: r, holder: h}, nil
}

// EstablishLifetime is idempotent and gives NO data-read capability. Controller
// terminal-state validation remains the CNPG caller's responsibility.
func (r *Repository) EstablishLifetime(ctx context.Context, target, operation string) error {
	return r.addHolder(ctx, Holder{ID: operation, Kind: "restore-lifetime", TargetClusterUID: target, OperationID: operation})
}
func (r *Repository) AdmitRestore(ctx context.Context, target, operation string) (*Hold, error) {
	if e := r.EstablishLifetime(ctx, target, operation); e != nil {
		return nil, e
	}
	return r.admit(ctx, "restore-reader", target, operation)
}

// ReleaseLifetimeAfterTermination may only be called after durable terminal
// operation state and matching Job/all-Pod termination proof. It cannot remove
// process-reader holders, even those of the same operation.
func (r *Repository) ReleaseLifetimeAfterTermination(ctx context.Context, target, operation string) error {
	h := Holder{ID: operation, Kind: "restore-lifetime", TargetClusterUID: target, OperationID: operation}
	if h.validate() != nil {
		return ErrInvalid
	}
	return r.removeHolder(ctx, h)
}
func (h *Hold) ID() string { return h.holder.ID }
func (h *Hold) check(ctx context.Context) error {
	if h.closed {
		return ErrClosed
	}
	if h.uncertain {
		return ErrUncertain
	}
	g, _, e := h.r.readGate(ctx)
	if e != nil {
		return e
	}
	found := false
	life := h.holder.Kind != "restore-reader"
	for _, v := range g.Holders {
		if sameHolder(v, h.holder) {
			found = true
		}
		if v.Kind == "restore-lifetime" && v.ID == h.holder.OperationID && v.TargetClusterUID == h.holder.TargetClusterUID {
			life = true
		}
	}
	if !found || !life || g.Owner != nil {
		return ErrBlocked
	}
	return nil
}

// Close waits for every locally admitted call. An uncertain mutation latches
// the holder forever, even if subsequent GET establishes a durable result.
func (h *Hold) Close(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	if h.uncertain {
		return ErrUncertain
	}
	return h.r.removeHolder(ctx, h.holder)
}
