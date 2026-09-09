package cnpgi

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestRecoveryMetricsOwnOnlyTheSelectedSourcePlugin(t *testing.T) {
	a, c, _ := fixture(t, true)
	ctx := context.Background()
	m := newBackupMetrics()
	scrape := func() string {
		w := httptest.NewRecorder()
		m.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
		return w.Body.String()
	}
	// OUR selected source, but no durable operation yet: Unknown is important.
	a.reconcileRecoveryOperation(ctx, c, m, time.Now())
	if !strings.Contains(scrape(), `cnpg_backup_restore_observation_known{namespace="test",cluster="database"} 0`) {
		t.Fatal(scrape())
	}
	// A new unrelated restore can reuse a deleted Cluster's name. Its destination
	// may still archive with us; that does not make its bootstrap source ours.
	c.Spec.ExternalClusters[0].Plugin.Name = "other.example/plugin"
	a.reconcileRecoveryOperation(ctx, c, m, time.Now())
	if strings.Contains(scrape(), "cnpg_backup_restore_observation_known{") {
		t.Fatal("unrelated recovery caused a false Unknown incident", scrape())
	}
	// An unused declaration of our source also does not confer ownership.
	c.Spec.ExternalClusters[0].Plugin.Name = "cnpg-backup.djosh34.github.io"
	c.Spec.Bootstrap.Recovery.Source = "different-source"
	a.reconcileRecoveryOperation(ctx, c, m, time.Now())
	if strings.Contains(scrape(), "cnpg_backup_restore_observation_known{") {
		t.Fatal("unused source declaration was observed", scrape())
	}
	c.Spec.Bootstrap.Recovery.Source = c.Spec.ExternalClusters[0].Name
	a.Client.(*dynamicfake.FakeDynamicClient).PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("operation read unavailable")
	})
	a.reconcileRecoveryOperation(ctx, c, m, time.Now())
	if !strings.Contains(scrape(), `cnpg_backup_restore_observation_known{namespace="test",cluster="database"} 0`) {
		t.Fatal("our unreadable operation was hidden", scrape())
	}
}

// Exercise the existing durable-operation reconciliation and public scrape,
// not a second recovery-state model. No metric may release source protection.
func TestRecoveryReconcileMetrics(t *testing.T) {
	a, c, _ := fixture(t, true)
	source, err := a.Repository(context.Background(), "test", "source")
	if err != nil {
		t.Fatal(err)
	}
	releases := 0
	a.recoveryLifetime = func(_ context.Context, _ Cluster, _ configuration.Spec, release bool) error {
		if release {
			releases++
		}
		return nil
	}
	p := RecoveryPlacement{ClusterUID: string(c.Metadata.UID), Namespace: "test", Cluster: c.Metadata.Name, OperationID: c.OperationUID(), BootstrapSHA256: c.BootstrapFingerprint(), Source: "source", Destination: "destination", SourceConfigSHA256: source.Hash()}
	g := recoveryguard.Config{ClusterUID: string(c.Metadata.UID), OperationUID: c.OperationUID(), Targets: []recoveryguard.Target{{PVCUID: repository.UUID(), Mount: "/var/lib/postgresql/data"}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.recoveryContext = ctx
	if err := a.ensureRecovery(ctx, c, p, g, source); err != nil {
		t.Fatal(err)
	}
	m := newBackupMetrics()
	scrape := func() string {
		w := httptest.NewRecorder()
		m.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
		return w.Body.String()
	}
	labels := `{namespace="test",cluster="` + c.Metadata.Name + `"}`
	check := func(name, value string) {
		t.Helper()
		if !strings.Contains(scrape(), name+labels+" "+value+"\n") {
			t.Fatal(scrape())
		}
	}
	now := time.Now()
	a.reconcileRecoveryOperation(ctx, c, m, now)
	check("cnpg_backup_restore_observation_known", "1")
	check("cnpg_backup_restore_active", "1")
	check("cnpg_backup_restore_uncertain", "0")
	cm, err := a.Get(ctx, coreResource("configmaps"), "test", operationName(c))
	if err != nil {
		t.Fatal(err)
	}
	state, err := operationFrom(cm, c)
	if err != nil {
		t.Fatal(err)
	}
	state.ObserverID = repository.UUID() // manager restart must not adopt an active observer
	if err := a.writeOperation(ctx, c, cm, state); err != nil {
		t.Fatal(err)
	}
	a.reconcileRecoveryOperation(ctx, c, m, now)
	check("cnpg_backup_restore_uncertain", "1")
	check("cnpg_backup_restore_active", "0")
	if releases != 0 {
		t.Fatal("metrics reconciliation released uncertain hold")
	}
	a.reconcileRecoveryOperation(ctx, c, m, now.Add(-6*time.Minute))
	check("cnpg_backup_restore_observation_known", "0")
	if strings.Contains(scrape(), "cnpg_backup_restore_uncertain"+labels) {
		t.Fatal("stale observation fabricated current state")
	}
	if strings.Contains(scrape(), c.OperationUID()) {
		t.Fatal("operation UID leaked into metric labels")
	}
}
