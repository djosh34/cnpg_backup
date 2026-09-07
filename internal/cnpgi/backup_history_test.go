package cnpgi

import (
	"context"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
)

func TestBackupSlowHistoryDoesNotBlockScrapesOrFailureObservation(t *testing.T) {
	a, c, _ := fixture(t, false)
	m := newBackupMetrics()
	o := newBackupObserver(a, m)
	defer o.queue.ShutDown()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		_, _ = a.reconcileBackupHistory(ctx, m, unstruct(c), func(ctx context.Context, _ string, _ string, _ configuration.Spec) (repository.BackupHistory, error) {
			close(started)
			<-ctx.Done()
			return repository.BackupHistory{}, ctx.Err()
		})
	}()
	<-started
	responsive := make(chan struct{})
	go func() {
		defer close(responsive)
		_ = metricText(m)
		b := observation("s3-outage", "failed", "full")
		o.observe(b, false)
		if err := o.process(context.Background(), b.GetUID(), time.Now()); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-responsive:
	case <-time.After(time.Second):
		t.Fatal("S3 blocked metrics/failure path")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("history did not cancel")
	}
	for _, s := range m.snapshot() {
		if s.Known || !s.Success.IsZero() {
			t.Fatal("canceled scan published history")
		}
	}
}
