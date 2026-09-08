package repository

import (
	"context"
	"github.com/djosh34/cnpg_backup/internal/s3store"
)

// BeginDifferential reuses the frozen request root on every retry, even when a
// newer full exists. Eligibility is only a selection filter for a NEW request;
// the native caller rechecks it before capture. Existing winners replay without
// requiring a live source. The ordinary Begin/publication protocol is unchanged.
func (h *Hold) BeginDifferential(ctx context.Context, req Request, eligible func(Commit) error) (*Attempt, *Result, Commit, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e := h.check(ctx); e != nil {
		return nil, nil, Commit{}, e
	}
	if req.RequestedKind != "differential" || eligible == nil || !validID(req.BackupUID) || h.holder.Kind != "backup" || req.BackupUID != h.holder.OperationID {
		return nil, nil, Commit{}, ErrInvalid
	}
	r := h.r
	b, _, e := r.store.Read(ctx, r.backup(req.BackupUID)+"request.json", smallLimit)
	if e == nil {
		var frozen Request
		if strict(b, smallLimit, &frozen) != nil || frozen.validate(r.id) != nil || frozen.RequestedKind != "differential" {
			return nil, nil, Commit{}, ErrIdentity
		}
		req.RootBackupUID = frozen.RootBackupUID
	} else if s3store.Is(e, s3store.NotFound) {
		f, e := r.spoolCatalog(ctx, CatalogLimits{MaxRecords: MaxCatalogRecords, MaxSpoolBytes: 256 << 20})
		if e != nil {
			return nil, nil, Commit{}, e
		}
		defer removeFile(f)
		var selected *Commit
		e = scanEntries(ctx, f, func(en Entry) error {
			if !en.Retired && en.Commit.Kind == "full" && eligible(en.Commit) == nil && (selected == nil || newer(en.Commit, *selected)) {
				v := en.Commit
				selected = &v
			}
			return ctx.Err()
		})
		if e != nil {
			return nil, nil, Commit{}, e
		}
		if selected == nil {
			return nil, nil, Commit{}, ErrBlocked
		}
		req.RootBackupUID = &selected.BackupUID
	} else {
		return nil, nil, Commit{}, e
	}
	attempt, winner, e := h.begin(ctx, req)
	if e != nil {
		return nil, nil, Commit{}, e
	}
	if winner != nil {
		return nil, winner, Commit{}, nil
	}
	root, _, _, e := r.readCommit(ctx, *req.RootBackupUID)
	return attempt, nil, root, e
}
