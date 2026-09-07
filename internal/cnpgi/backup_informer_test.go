package cnpgi

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func waitBackupTest(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("Backup informer did not reach expected state")
}
func TestBackupActualInformerInitialSyncWatchAndCancellation(t *testing.T) {
	a, c, _ := fixture(t, false)
	repo, err := a.Get(context.Background(), repositories, "test", "destination")
	if err != nil {
		t.Fatal(err)
	}
	initial := observation("baseline", "failed", "full")
	active := observation("watch", "started", "full")
	a.Client = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{backups: "BackupList"}, unstruct(c), repo, initial, active)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := newBackupMetrics()
	o := newBackupObserver(a, m)
	done := make(chan struct{})
	go func() { o.run(ctx); close(done) }()
	waitBackupTest(t, func() bool { o.mu.Lock(); defer o.mu.Unlock(); return len(o.observed) == 2 })
	for _, s := range m.snapshot() {
		if s.Failures != 0 {
			t.Fatal("initial sync replay")
		}
	}
	failed := observation("watch", "failed", "full")
	if _, err = a.Client.Resource(backups).Namespace("test").UpdateStatus(ctx, failed, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	l := backupLabels{"22222222-2222-4222-8222-222222222222", "test", "database", "full"}
	waitBackupTest(t, func() bool { return m.snapshot()[l].Failures == 1 })
	for range 3 {
		if _, err = a.Client.Resource(backups).Namespace("test").UpdateStatus(ctx, failed, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if err = a.Client.Resource(backups).Namespace("test").Delete(ctx, failed.GetName(), metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitBackupTest(t, func() bool { o.mu.Lock(); defer o.mu.Unlock(); _, ok := o.observed[failed.GetUID()]; return !ok })
	if m.snapshot()[l].Failures != 1 {
		t.Fatal("watch duplicate counted")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("informer/worker did not cancel")
	}
}
