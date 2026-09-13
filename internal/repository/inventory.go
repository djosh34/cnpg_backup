package repository

import (
	"context"
	"strings"
)

func (r *Repository) validateListedMetadata(ctx context.Context, key string) error {
	p := strings.Split(strings.TrimPrefix(key, r.root+"backups/"), "/")
	uid := p[0]
	if len(p) == 2 {
		b, _, e := r.store.Read(ctx, key, smallLimit)
		if e != nil {
			return e
		}
		switch p[1] {
		case "request.json":
			var q Request
			if strict(b, smallLimit, &q) != nil || q.validate(r.id) != nil || q.BackupUID != uid {
				return ErrCorrupt
			}
		case "retired.json":
			var ret Retirement
			if strict(b, smallLimit, &ret) != nil {
				return ErrCorrupt
			}
			_, cb, _, e := r.readCommit(ctx, uid)
			if e != nil {
				return e
			}
			return ret.validate(r.id, uid, digest(cb))
		default:
			return ErrCorrupt
		}
		return nil
	}
	cl, e := r.readClaim(ctx, uid, p[2])
	if e != nil {
		return e
	}
	qb, _, e := r.store.Read(ctx, r.backup(uid)+"request.json", smallLimit)
	if e != nil {
		return e
	}
	var req Request
	if strict(qb, smallLimit, &req) != nil || req.validate(r.id) != nil || req.BackupUID != uid || digest(qb) != cl.RequestSHA256 {
		return ErrCorrupt
	}
	return nil
}
func (r *Repository) readClaim(ctx context.Context, uid, attempt string) (Claim, error) {
	var cl Claim
	if !validID(uid) || !validID(attempt) {
		return cl, ErrInvalid
	}
	b, _, e := r.store.Read(ctx, r.attempt(uid, attempt)+"claim.json", smallLimit)
	if e != nil {
		return cl, e
	}
	if strict(b, smallLimit, &cl) != nil || cl.Schema != 1 || cl.RepositoryID != r.id.RepositoryID || cl.BackupUID != uid || cl.AttemptID != attempt || !validID(cl.ProcessID) || !hashRE.MatchString(cl.RequestSHA256) {
		return cl, ErrCorrupt
	}
	return cl, nil
}
