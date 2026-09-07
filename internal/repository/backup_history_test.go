package repository

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

type metadataOnlyHistory struct{ Storage }

func (s metadataOnlyHistory) Read(ctx context.Context, key string, limit int64) ([]byte, s3store.Info, error) {
	if strings.HasSuffix(key, "manifest.pg.json") || strings.HasSuffix(key, ".tar") || strings.HasSuffix(key, ".gz") {
		return nil, s3store.Info{}, errors.New("history attempted payload read")
	}
	return s.Storage.Read(ctx, key, limit)
}
func (s metadataOnlyHistory) Head(context.Context, string) (s3store.Info, error) {
	return s3store.Info{}, errors.New("history attempted payload HEAD")
}

func TestBackupHistoryPermanentPublicationNotPayloadOrGate(t *testing.T) {
	r, s := setup(t)
	empty, err := ReadBackupHistory(ctx, s, repoID, writerID)
	if err != nil || !empty.Full.IsZero() || !empty.Differential.IsZero() {
		t.Fatalf("empty history: %+v %v", empty, err)
	}
	h, res := publish(t, r, backupID, "payload")
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	before := s.n
	history, err := ReadBackupHistory(ctx, metadataOnlyHistory{s}, repoID, writerID)
	if err != nil || !history.Full.Equal(res.PublishedAt) || !history.Differential.IsZero() {
		t.Fatalf("publication history: %+v %v", history, err)
	}
	if s.n != before {
		t.Fatal("metrics mutated storage")
	}
	g, err := r.AcquireGC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = g.Execute(ctx, gcplan(g, retire(res), payload(res, 0))); err != nil {
		t.Fatal(err)
	}
	// GC is STILL admitted; metadata-only metrics must not acquire a hold or
	// require payloads that retention has legitimately removed.
	before = s.n
	history, err = ReadBackupHistory(ctx, metadataOnlyHistory{s}, repoID, writerID)
	if err != nil || !history.Full.Equal(res.PublishedAt) {
		t.Fatalf("retired history: %+v %v", history, err)
	}
	if s.n != before {
		t.Fatal("metrics touched gate")
	}
	if _, err := ReadBackupHistory(ctx, s, repoID, targetID); err == nil {
		t.Fatal("attributed history to another writer")
	}
	requireOracle(t, s)
}

type historyListFault struct {
	Storage
	cancel context.CancelFunc
	fail   bool
}

func (s historyListFault) List(ctx context.Context, prefix string, max int, visit func(s3store.Info) error) error {
	if err := s.Storage.List(ctx, prefix, max, visit); err != nil {
		return err
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.fail {
		return errors.New("final page failed")
	}
	return nil
}
func TestBackupHistoryUnknownNeverLeaksPartialMaxima(t *testing.T) {
	r, s := setup(t)
	_, _ = publish(t, r, backupID, "payload")
	h, err := ReadBackupHistory(ctx, historyListFault{Storage: s, fail: true}, repoID, writerID)
	if err == nil || !h.Full.IsZero() {
		t.Fatal("partial history escaped", h, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	defer cancel()
	h, err = ReadBackupHistory(canceled, historyListFault{Storage: s, cancel: cancel}, repoID, writerID)
	if !errors.Is(err, context.Canceled) || !h.Full.IsZero() {
		t.Fatal("accepted cancellation at EOF", h, err)
	}
	key := r.backup(backupID) + "commit.json"
	saved := s.objects[key]
	damaged := saved
	damaged.info.Modified = time.Time{}
	s.objects[key] = damaged
	if _, err = ReadBackupHistory(ctx, s, repoID, writerID); err == nil {
		t.Fatal("fabricated missing LastModified")
	}
	s.objects[key] = saved
	s.objects[r.root+"backups/unknown"] = object{info: s3store.Info{Key: r.root + "backups/unknown", ETag: "x"}}
	if _, err = ReadBackupHistory(ctx, s, repoID, writerID); err == nil {
		t.Fatal("unknown catalog key accepted")
	}
}

func TestBackupHistorySeparatesRequestedTypesAndRejectsMalformedCommit(t *testing.T) {
	r, s := setup(t)
	fullHold, full := publish(t, r, backupID, "full")
	if err := fullHold.Close(ctx); err != nil {
		t.Fatal(err)
	}
	diffHold, diff := differential(t, r, full, targetID)
	if err := diffHold.Close(ctx); err != nil {
		t.Fatal(err)
	}
	h, err := ReadBackupHistory(ctx, metadataOnlyHistory{s}, repoID, writerID)
	if err != nil || !h.Full.Equal(full.PublishedAt) || !h.Differential.Equal(diff.PublishedAt) {
		t.Fatalf("per-type history %+v %v", h, err)
	}
	g, err := r.AcquireGC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = g.Execute(ctx, gcplan(g, retire(diff), retire(full), payload(diff, 0), payload(full, 0))); err != nil {
		t.Fatal(err)
	}
	h, err = ReadBackupHistory(ctx, metadataOnlyHistory{s}, repoID, writerID)
	if err != nil || !h.Full.Equal(full.PublishedAt) || !h.Differential.Equal(diff.PublishedAt) {
		t.Fatal("retired full+differential history lost", h, err)
	}
	key := r.backup(targetID) + "commit.json"
	obj := s.objects[key]
	obj.b = []byte(`{"schema":999,"secret":"must-not-be-treated-as-success"}`)
	s.objects[key] = obj
	h, err = ReadBackupHistory(ctx, s, repoID, writerID)
	if err == nil || !h.Full.IsZero() || !h.Differential.IsZero() {
		t.Fatal("malformed later commit exposed partial full maximum", h, err)
	}
}
