package repository

import (
	"context"
	"time"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

// BackupHistory describes publication history, NOT current recoverability. Zero
// timestamps mean never successful, and are usable only with a nil scan error.
type BackupHistory struct{ Full, Differential time.Time }

// ReadBackupHistory reads only permanent metadata. Unlike a selection catalog it
// needs no deletion hold: requests, claims and commits survive retirement. It
// never reads payloads, acquires admission, or mutates storage. The ordered LIST
// and strict metadata validation are shared with the repository catalog; only two
// maxima are retained in memory. Partial/failed scans return no history.
func ReadBackupHistory(ctx context.Context, s Storage, repositoryID, writerUID string) (BackupHistory, error) {
	var result BackupHistory
	if s == nil || !validID(repositoryID) || !validID(writerUID) {
		return result, ErrInvalid
	}
	root := "v1/" + repositoryID + "/"
	b, _, err := s.Read(ctx, root+"repository.json", smallLimit)
	if err != nil {
		return result, err
	}
	var id Identity
	if strict(b, smallLimit, &id) != nil || id.Validate() != nil || id.RepositoryID != repositoryID || id.WriterClusterUID != writerUID {
		return result, ErrIdentity
	}
	r := &Repository{store: s, id: id, root: root}
	err = s.List(ctx, root+"backups/", MaxCatalogRecords, func(info s3store.Info) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		uid, kind, ok := r.catalogKey(info.Key)
		if !ok {
			return ErrCorrupt
		}
		if kind != "commit" {
			return r.validateListedMetadata(ctx, info.Key)
		}
		c, _, published, err := r.readCommit(ctx, uid)
		if err != nil {
			return err
		}
		if _, err = r.requestFor(ctx, c); err != nil {
			return err
		}
		if c.Kind == "differential" {
			// Validate the permanent edge, not parent liveness: expiration of
			// both members cannot erase a previously valid differential success.
			parent, _, _, err := r.readCommit(ctx, c.RootBackupUID)
			if err != nil {
				return err
			}
			if err = validateParent(c, parent); err != nil {
				return err
			}
		}
		if published.Modified.IsZero() || published.Modified.Unix() <= 0 {
			return ErrCorrupt
		}
		latest := &result.Full
		if c.Kind == "differential" {
			latest = &result.Differential
		}
		if published.Modified.After(*latest) {
			*latest = published.Modified
		}
		return nil
	})
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return BackupHistory{}, err
	}
	return result, nil
}
