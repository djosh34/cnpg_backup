package repository

import (
	"context"
	"strings"

	"github.com/djosh34/cnpg_backup/internal/s3store"
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

// OrphanUploads is bounded complete MPU discovery for a claimed attempt. An
// unknown initiation response can be recovered by enumeration, never by age.
// The returned victims still need an immutable GCPlan and Execute under this
// same live owner. No callback receives a partial list.
func (g *GCOwner) OrphanUploads(ctx context.Context, uid, attempt string) ([]Victim, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if e := g.check(ctx); e != nil {
		return nil, e
	}
	cl, e := g.r.readClaim(ctx, uid, attempt)
	if e != nil {
		return nil, e
	}
	var victims []Victim
	e = g.r.store.ListUploads(ctx, g.r.attempt(uid, attempt), 128, func(u s3store.Upload) error {
		if len(victims) >= 128 {
			return ErrCapacity
		}
		if _, _, ok := g.r.catalogKey(u.Key); !ok {
			return ErrCorrupt
		}
		suffix := strings.TrimPrefix(u.Key, g.r.attempt(uid, attempt))
		v := Victim{BackupUID: &uid, AttemptID: &attempt, UploadID: &u.ID, SHA256: cl.RequestSHA256}
		if suffix == "manifest.pg.json" {
			v.Kind = "abort-manifest"
		} else if strings.HasPrefix(suffix, "data/") {
			v.Kind = "abort-artifact"
			name := strings.TrimPrefix(suffix, "data/")
			comp := "none"
			if strings.HasSuffix(name, ".gz") {
				comp = "gzip"
				name = strings.TrimSuffix(name, ".gz")
			}
			n, ok := decimal(strings.TrimSuffix(name, ".tar"))
			if !ok || n >= 66 {
				return ErrCorrupt
			}
			i := int(n)
			v.Index = &i
			v.Compression = &comp
		} else {
			return ErrCorrupt
		}
		if e := g.validateVictim(ctx, v, map[string]bool{}); e != nil {
			return e
		}
		victims = append(victims, v)
		return nil
	})
	if e != nil {
		return nil, e
	}
	return victims, nil
}

// DecodeCommit provides bounded strict version validation for fixtures and
// future native/recovery callers; keys always remain repository-derived.
func DecodeCommit(b []byte, id Identity) (Commit, error) {
	var c Commit
	if id.Validate() != nil {
		return c, ErrIdentity
	}
	if e := strict(b, commitLimit, &c); e != nil {
		return c, e
	}
	return c, c.validate(id)
}
