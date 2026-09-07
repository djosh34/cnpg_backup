package wal

import (
	"context"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
)

func TestOrdinaryWriterIdentityIsImmutableAcrossRetries(t *testing.T) {
	w, b := setup(t, 1<<20)
	id := w.Repository.Identity()
	id.CreatedAt = ""
	if _, e := repository.OpenWriter(context.Background(), w.Store.(*s3store.Store), id, w.Workspace); e != nil {
		t.Fatal("legitimate initialization retry", e)
	}
	for _, change := range []func(*repository.Identity){
		func(i *repository.Identity) { i.WriterClusterUID = repository.UUID() },
		func(i *repository.Identity) { i.SystemIdentifier = "987654" },
		func(i *repository.Identity) { i.WALSegmentBytes = 64 << 20 },
	} {
		foreign := id
		change(&foreign)
		if _, e := repository.OpenWriter(context.Background(), w.Store.(*s3store.Store), foreign, w.Workspace); e != repository.ErrIdentity {
			t.Fatal("foreign writer adoption", e)
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.put != 0 {
		t.Fatal("identity rejection dispatched writes")
	}
}
