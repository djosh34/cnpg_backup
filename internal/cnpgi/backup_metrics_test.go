package cnpgi

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

func metricText(m *backupMetrics) string {
	out := httptest.NewRecorder()
	m.ServeHTTP(out, httptest.NewRequest("GET", "/metrics", nil))
	return out.Body.String()
}
func metricLine(name string, l backupLabels, value string) string {
	return name + "{" + l.text() + "} " + value + "\n"
}
func observation(uid, phase, kind string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "postgresql.cnpg.io/v1", "kind": "Backup", "metadata": map[string]any{"name": "backup-" + uid, "namespace": "test", "uid": uid},
		"spec":   map[string]any{"cluster": map[string]any{"name": "database"}, "method": "plugin", "pluginConfiguration": map[string]any{"name": "cnpg-backup.djosh34.github.io", "parameters": map[string]any{"backupType": kind}}},
		"status": map[string]any{"phase": phase, "error": "https://secret-endpoint/path?credential=must-not-leak"},
	}}
}
func TestBackupFailureBaselineDedupRestartDeletionAndRedaction(t *testing.T) {
	a, _, _ := fixture(t, false)
	m := newBackupMetrics()
	o := newBackupObserver(a, m)
	defer o.queue.ShutDown()
	handler := o.handlers()
	ctx := context.Background()
	now := time.Now()
	initial := observation("old", "failed", "full")
	handler.OnAdd(initial, true)
	handler.OnUpdate(initial, initial.DeepCopy()) // informer resync/relist baseline
	if err := o.process(ctx, initial.GetUID(), now); err != nil {
		t.Fatal(err)
	}
	if len(m.snapshot()) != 0 {
		t.Fatal("initial failed replayed")
	}
	started := observation("new", "started", "full")
	failed := observation("new", "failed", "full")
	handler.OnAdd(started, true)
	handler.OnUpdate(started, failed)
	for range 5 {
		handler.OnUpdate(failed, failed.DeepCopy())
		if err := o.process(ctx, failed.GetUID(), now); err != nil {
			t.Fatal(err)
		}
	}
	l := backupLabels{"22222222-2222-4222-8222-222222222222", "test", "database", "full"}
	if m.snapshot()[l].Failures != 1 {
		t.Fatal("retry/resync counter", m.snapshot())
	}
	// Even a pathological phase regression cannot double-count the same UID.
	handler.OnUpdate(failed, started)
	handler.OnUpdate(started, failed)
	_ = o.process(ctx, failed.GetUID(), now)
	if m.snapshot()[l].Failures != 1 {
		t.Fatal("UID counted twice")
	}
	second := observation("second", "failed", "full")
	handler.OnAdd(second, false)
	_ = o.process(ctx, second.GetUID(), now.Add(time.Minute))
	third := observation("third", "failed", "differential")
	handler.OnAdd(third, false)
	_ = o.process(ctx, third.GetUID(), now)
	// Invalid type and foreign plugin never create another type label/counter.
	bad := observation("bad", "failed", "secret-type")
	handler.OnAdd(bad, false)
	_ = o.process(ctx, bad.GetUID(), now)
	foreign := observation("foreign", "failed", "full")
	_ = unstructured.SetNestedField(foreign.Object, "other", "spec", "pluginConfiguration", "name")
	handler.OnAdd(foreign, false)
	_ = o.process(ctx, foreign.GetUID(), now)
	if m.snapshot()[l].Failures != 2 {
		t.Fatal("wrong matching failures", m.snapshot())
	}
	events := 0
	for _, action := range a.Client.(*dynamicfake.FakeDynamicClient).Actions() {
		if action.GetVerb() != "create" || action.GetResource().Resource != "events" {
			continue
		}
		event := action.(clienttesting.CreateAction).GetObject().(*unstructured.Unstructured)
		if event.Object["type"] != "Warning" || event.Object["reason"] != "BackupFailed" || strings.Contains(string(raw(event)), "must-not-leak") {
			t.Fatal("invalid Warning", event)
		}
		events++
	}
	if events != 2 {
		t.Fatalf("want one Warning per type within throttle, got %d", events)
	}
	handler.OnDelete(cache.DeletedFinalStateUnknown{Key: "test/backup-new", Obj: failed})
	o.mu.Lock()
	_, exists := o.observed[failed.GetUID()]
	o.mu.Unlock()
	if exists {
		t.Fatal("deletion did not evict UID")
	}
	// Kubernetes never reuses a UID; this negative control proves eviction.
	handler.OnAdd(failed, false)
	_ = o.process(ctx, failed.GetUID(), now.Add(6*time.Minute))
	if m.snapshot()[l].Failures != 3 {
		t.Fatal("deleted observation retained")
	}
	restarted := newBackupObserver(a, newBackupMetrics())
	defer restarted.queue.ShutDown()
	restarted.observe(failed, true)
	_ = restarted.process(ctx, failed.GetUID(), now)
	if len(restarted.metrics.snapshot()) != 0 {
		t.Fatal("downtime failures fabricated on restart")
	}
	compact, _ := compactBackup(failed)
	if strings.Contains(string(raw(compact)), "must-not-leak") {
		t.Fatal("informer retained error text")
	}
	text := metricText(m)
	if strings.Contains(text, "uid=") || strings.Contains(text, "secret") || strings.Contains(text, "backup-new") {
		t.Fatal("unbounded or secret metric labels", text)
	}
}

func TestBackupDurableHistoryUnknownNeverSuccessfulAndLostResponse(t *testing.T) {
	a, c, _ := fixture(t, false)
	m := newBackupMetrics()
	ctx := context.Background()
	l := backupLabels{"22222222-2222-4222-8222-222222222222", "test", "database", "full"}
	scan := func(h repository.BackupHistory, e error) {
		_, err := a.reconcileBackupHistory(ctx, m, unstruct(c), func(context.Context, string, string, configuration.Spec) (repository.BackupHistory, error) {
			return h, e
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	scan(repository.BackupHistory{}, nil)
	text := metricText(m)
	if !strings.Contains(text, metricLine("cnpg_backup_success_history_known", l, "1")) || strings.Contains(text, "cnpg_backup_last_success_timestamp_seconds{") {
		t.Fatal("empty catalog fabricated success", text)
	}
	stamp := time.Unix(1700000000, 0)
	scan(repository.BackupHistory{Full: stamp}, nil)
	o := newBackupObserver(a, m)
	defer o.queue.ShutDown()
	o.observe(observation("lost-response", "started", "full"), false)
	failed := observation("lost-response", "failed", "full")
	o.observe(failed, false)
	if err := o.process(ctx, failed.GetUID(), time.Now()); err != nil {
		t.Fatal(err)
	}
	text = metricText(m)
	if !strings.Contains(text, metricLine("cnpg_backup_last_success_timestamp_seconds", l, "1700000000")) || !strings.Contains(text, metricLine("cnpg_backup_failures_total", l, "1")) {
		t.Fatal("lost-response disagreement hidden", text)
	}
	// A failed uncommitted invocation cannot advance an unchanged scan.
	scan(repository.BackupHistory{Full: stamp}, nil)
	if !m.snapshot()[l].Success.Equal(stamp) {
		t.Fatal("failure refreshed success")
	}
	scan(repository.BackupHistory{Full: time.Now()}, errors.New("partial S3 catalog"))
	text = metricText(m)
	if !strings.Contains(text, metricLine("cnpg_backup_success_history_known", l, "0")) || strings.Contains(text, "cnpg_backup_last_success_timestamp_seconds{") {
		t.Fatal("partial/unknown history leaked", text)
	}
	m.configure(l, configuration.Freshness{FullMaxAge: "24h"})
	text = metricText(m)
	if !strings.Contains(text, metricLine("cnpg_backup_freshness_max_age_seconds", l, "86400")) {
		t.Fatal("missing configured schedule threshold", text)
	}
	d := l
	d.Type = "differential"
	if strings.Contains(text, "cnpg_backup_freshness_max_age_seconds{"+d.text()) {
		t.Fatal("enabled unscheduled differential alert")
	}
	m.history(l, repository.BackupHistory{Full: stamp}, nil, time.Now().Add(-backupHistoryTTL-time.Second))
	if strings.Contains(metricText(m), "cnpg_backup_last_success_timestamp_seconds{") {
		t.Fatal("stalled sweeper stayed known forever")
	}
	// CNPG owns Backup status: manager only GETs config and CREATEs Events.
	for _, action := range a.Client.(*dynamicfake.FakeDynamicClient).Actions() {
		if action.GetVerb() != "get" && !(action.GetVerb() == "create" && action.GetResource().Resource == "events") {
			t.Fatal("unauthorized status ownership", action)
		}
	}
}

func TestBackupSnapshotUsesAllowlistAndOneSecretGeneration(t *testing.T) {
	a, c, _ := fixture(t, false)
	_, spec, err := a.backupConfiguration(context.Background(), unstruct(c))
	if err != nil {
		t.Fatal(err)
	}
	snap, err := a.backupSnapshot(context.Background(), "test", spec)
	if err != nil {
		t.Fatal(err)
	}
	if string(snap.AccessKey) != "test-access" || string(snap.SecretKey) != "test-secret" {
		t.Fatal("snapshot differs")
	}
	secretReads := 0
	for _, action := range a.Client.(*dynamicfake.FakeDynamicClient).Actions() {
		if action.GetResource().Resource == "secrets" {
			secretReads++
		}
	}
	if secretReads != 1 {
		t.Fatal("mixed Secret generations", secretReads)
	}
	a.SecretNames["test"] = nil
	if _, err = a.backupSnapshot(context.Background(), "test", spec); err == nil {
		t.Fatal("Secret allowlist bypass")
	}
}

func TestBackupPendingFailureRetriesAPIWithoutRecount(t *testing.T) {
	a, _, _ := fixture(t, false)
	m := newBackupMetrics()
	o := newBackupObserver(a, m)
	defer o.queue.ShutDown()
	b := observation("retry", "failed", "full")
	o.observe(b, false)
	// A later mutable spec must not relabel the already observed failure.
	o.observe(observation("retry", "failed", "invalid"), false)
	a.Namespaces = nil
	if err := o.process(context.Background(), types.UID("retry"), time.Now()); err == nil {
		t.Fatal("unavailable API counted")
	}
	a.Namespaces = []string{"test"}
	for range 3 {
		if err := o.process(context.Background(), b.GetUID(), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	for _, s := range m.snapshot() {
		if s.Failures > 1 {
			t.Fatal("API retry double count")
		}
	}
	// Delete pending work before configuration is available; no orphan UID state.
	b = observation("deleted", "failed", "full")
	o.observe(b, false)
	o.deleted(b)
	if err := o.process(context.Background(), b.GetUID(), time.Now()); err != nil {
		t.Fatal(err)
	}
}
