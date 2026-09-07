package repository

import "context"

// Authoritative inventories verify original manifests and bounded artifact
// metadata, not every database byte in every backup. A HEAD is never represented
// as checksum verification: publication/replay and selected restore inputs use
// verifyPayload. This lets the manager inventory without database-sized spools.
func (r *Repository) payloadMetadata(ctx context.Context, c Commit) error {
	if e := r.downloadVerify(ctx, r.attempt(c.BackupUID, c.AttemptID)+"manifest.pg.json", c.ManifestBytes, c.ManifestSHA256, "none", c.ManifestBytes, c.ManifestSHA256); e != nil {
		return e
	}
	for _, a := range c.Artifacts {
		i, e := r.store.Head(ctx, r.artifact(c, a))
		if e != nil {
			return e
		}
		if i.Size != a.StoredBytes || i.ETag == "" {
			return ErrCorrupt
		}
	}
	return nil
}
func (r *Repository) parentMetadata(ctx context.Context, c Commit) error {
	if c.Kind == "full" {
		return nil
	}
	p, b, _, e := r.readCommit(ctx, *c.ParentBackupUID)
	if e != nil {
		return e
	}
	if e = validateParent(c, p); e != nil {
		return e
	}
	if e = r.live(ctx, p, b); e != nil {
		return e
	}
	if _, e = r.requestFor(ctx, p); e != nil {
		return e
	}
	return r.payloadMetadata(ctx, p)
}
