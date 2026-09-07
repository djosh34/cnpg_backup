package repository

import (
	"context"
	"time"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

// OpenWriter binds physical identity AND the immutable Kubernetes Cluster UID.
// It never adopts foreign lineage, even with the same PostgreSQL system ID.
// Unset/false CNPG-I empty guards cannot alter this check. First initialization
// retains Initialize's conditional capability and complete-empty-inventory gate.
func OpenWriter(ctx context.Context, s *s3store.Store, expected Identity, workspace string) (*Repository, error) {
	open := func() (*Repository, error) {
		r, e := OpenSource(ctx, s, expected.RepositoryID, workspace)
		if e != nil {
			return nil, e
		}
		want := expected
		want.CreatedAt = r.id.CreatedAt
		if want != r.id {
			return nil, ErrIdentity
		}
		return r, nil
	}
	r, e := open()
	if e == nil {
		return r, nil
	}
	if !s3store.Is(e, s3store.NotFound) {
		return nil, e
	}
	expected.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	r, e = Initialize(ctx, s, expected, workspace)
	if e == nil {
		return r, nil
	}
	// Concurrent first writers may choose different creation timestamps. Only a
	// complete valid existing identity/gate with the exact stable fields wins.
	if winner, re := open(); re == nil {
		return winner, nil
	}
	return nil, e
}
