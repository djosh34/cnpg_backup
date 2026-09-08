package cnpgi

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/retention"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestRetentionDefaultsPeriodicWarningAndNonblockingEvents(t *testing.T) {
	a, c, _ := fixture(t, false)
	ctx := context.Background()
	m := newBackupMetrics()
	now := time.Now().UTC()
	calls := 0
	run := func(context.Context, string, string, configuration.Spec, time.Time) (retentionObservation, error) {
		calls++
		return retentionObservation{holders: 2}, repository.ErrBlocked
	}
	get := func() *unstructured.Unstructured {
		o, e := a.Get(ctx, repositories, "test", "destination")
		if e != nil {
			t.Fatal(e)
		}
		return o
	}
	if e := a.reconcileRetention(ctx, m, unstruct(c), now, run); e != nil || calls != 0 {
		t.Fatal("disabled ran GC", calls, e)
	}
	o := get()
	spec := o.Object["spec"].(map[string]any)
	spec["retention"] = map[string]any{"enabled": true, "dryRun": true, "window": "1h", "minimumFulls": int64(1), "interval": "5m"}
	if _, e := a.Client.Resource(repositories).Namespace("test").Update(ctx, o, metav1.UpdateOptions{}); e != nil {
		t.Fatal(e)
	}
	warnings := 0
	client := a.Client.(*dynamicfake.FakeDynamicClient)
	client.PrependReactor("create", "events", func(action ktesting.Action) (bool, runtime.Object, error) {
		warnings++
		event := action.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured)
		if event.Object["reason"] != "RetentionBlocked" || event.Object["type"] != "Warning" {
			t.Fatal(event)
		}
		return true, nil, errors.New("optional event delivery unavailable")
	})
	if e := a.reconcileRetention(ctx, m, unstruct(c), now, run); e != nil || calls != 1 || warnings != 1 {
		t.Fatal(calls, warnings, e)
	}
	var state repositoryStatus
	b, _ := json.Marshal(get().Object["status"])
	json.Unmarshal(b, &state)
	if !meta.IsStatusConditionTrue(state.Conditions, "RetentionBlocked") || state.Retention.LastWarningTime == nil {
		t.Fatal(state)
	}
	// Recreate manager-facing API, but retain durable scheduling/throttle.
	fresh := &API{Client: client, Namespaces: a.Namespaces, SecretNames: a.SecretNames}
	if e := fresh.reconcileRetention(ctx, m, unstruct(c), now.Add(time.Minute), run); e != nil || calls != 1 {
		t.Fatal("interval ignored", calls, e)
	}
	if e := fresh.reconcileRetention(ctx, m, unstruct(c), now.Add(6*time.Minute), run); e != nil || calls != 2 || warnings != 2 {
		t.Fatal(calls, warnings, e)
	}
	if text := metricText(m); !strings.Contains(text, "cnpg_backup_repository_holders{repository_id=") || !strings.Contains(text, "cluster=\"database\"} 2") {
		t.Fatal(text)
	}
	good := func(context.Context, string, string, configuration.Spec, time.Time) (retentionObservation, error) {
		return retentionObservation{result: retention.Result{Planned: 3}}, nil
	}
	if e := fresh.reconcileRetention(ctx, m, unstruct(c), now.Add(12*time.Minute), good); e != nil {
		t.Fatal(e)
	}
	b, _ = json.Marshal(get().Object["status"])
	json.Unmarshal(b, &state)
	if meta.IsStatusConditionTrue(state.Conditions, "RetentionBlocked") || !state.Retention.DryRun || state.Retention.Planned != 3 {
		t.Fatal(state)
	}
	// Configuration health must preserve the independently observed storage state.
	if e := fresh.ReconcileRepositoryStatus(ctx, get(), now.Add(13*time.Minute)); e != nil {
		t.Fatal(e)
	}
	b, _ = json.Marshal(get().Object["status"])
	json.Unmarshal(b, &state)
	if state.Retention.Planned != 3 {
		t.Fatal("configuration erased retention")
	}
}
