package cnpgi

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/retention"
	appsv1 "k8s.io/api/apps/v1"
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
		return retentionObservation{holders: 2, gateObserved: true}, repository.ErrBlocked
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

func TestRenderedRetentionAggregateWorkspace(t *testing.T) {
	image := "example.invalid/image@sha256:" + strings.Repeat("a", 64)
	b, e := exec.Command("python3", "../../config/render.py", "--manager-image", image, "--data-image", image, "--managed-namespace", "test", "--secret-name", "s3").Output()
	if e != nil {
		t.Fatal(e)
	}
	var list struct {
		Items []json.RawMessage `json:"items"`
	}
	if e = json.Unmarshal(b, &list); e != nil {
		t.Fatal(e)
	}
	for _, raw := range list.Items {
		var d appsv1.Deployment
		if json.Unmarshal(raw, &d) != nil || d.Kind != "Deployment" {
			continue
		}
		manager := d.Spec.Template.Spec.Containers[0]
		for _, mount := range manager.VolumeMounts {
			if mount.MountPath != "/cnpg-backup/retention" {
				continue
			}
			for _, volume := range d.Spec.Template.Spec.Volumes {
				if volume.Name == mount.Name && volume.EmptyDir != nil && volume.EmptyDir.Medium == "" && volume.EmptyDir.SizeLimit != nil && volume.EmptyDir.SizeLimit.Value() >= 512<<20 {
					return
				}
			}
		}
	}
	t.Fatal("manager has no dedicated disk mount for aggregate 256MiB catalog + 64MiB manifest + history/control/allocation headroom")
}

func TestRetentionWorkspaceAccountsForAggregateAndCrashRemainder(t *testing.T) {
	if repository.GCWorkspaceBytes < repository.GCCatalogBytes+repository.MaxManifestBytes+(4+3+16)<<20 {
		t.Fatal("aggregate reservation omits live scratch or headroom")
	}
	root := t.TempDir()
	dir, e := newRetentionWorkspace(root)
	if e != nil {
		t.Fatal(e)
	}
	os.RemoveAll(dir)
	// Sparse, cheap capacity negative: count logical bounds, not just current
	// node free space or sparse allocation. Do not fill the runner filesystem.
	f, e := os.Create(filepath.Join(root, "previous-owner"))
	if e != nil {
		t.Fatal(e)
	}
	if e = f.Truncate(retentionMountBytes - repository.GCWorkspaceBytes); e != nil {
		t.Fatal(e)
	}
	f.Close()
	if _, e = newRetentionWorkspace(root); !errors.Is(e, repository.ErrCapacity) {
		t.Fatal("ignored retained aggregate bytes", e)
	}
	if _, e = os.Stat(f.Name()); e != nil {
		t.Fatal("removed another operation's local state", e)
	}
}

func TestRetentionUnobservedGateIsUnknownNotHealthy(t *testing.T) {
	for _, mode := range []string{"disabled", "credentials-failed", "drained-diagnostics-failed"} {
		t.Run(mode, func(t *testing.T) {
			a, c, _ := fixture(t, false)
			ctx := context.Background()
			now := time.Now().UTC()
			m := newBackupMetrics()
			if mode != "disabled" {
				o, e := a.Get(ctx, repositories, "test", "destination")
				if e != nil {
					t.Fatal(e)
				}
				o.Object["spec"].(map[string]any)["retention"] = map[string]any{"enabled": true, "dryRun": false, "window": "1h", "minimumFulls": int64(1), "interval": "5m"}
				if _, e = a.Client.Resource(repositories).Namespace("test").Update(ctx, o, metav1.UpdateOptions{}); e != nil {
					t.Fatal(e)
				}
			}
			run := func(context.Context, string, string, configuration.Spec, time.Time) (retentionObservation, error) {
				if mode == "credentials-failed" {
					return retentionObservation{}, errors.New("credentials unavailable")
				}
				return retentionObservation{result: retention.Result{Executed: true, Planned: 1}}, nil
			}
			if e := a.reconcileRetention(ctx, m, unstruct(c), now, run); e != nil {
				t.Fatal(e)
			}
			o, e := a.Get(ctx, repositories, "test", "destination")
			if e != nil {
				t.Fatal(e)
			}
			var state repositoryStatus
			b, _ := json.Marshal(o.Object["status"])
			json.Unmarshal(b, &state)
			condition := meta.FindStatusCondition(state.Conditions, "RepositoryAdmissionBlocked")
			if condition == nil || condition.Status != metav1.ConditionUnknown {
				t.Fatal("unobserved gate published healthy", condition)
			}
			text := metricText(m)
			for _, name := range []string{"cnpg_backup_repository_admission_blocked{", "cnpg_backup_repository_holders{"} {
				if strings.Contains(text, name) {
					t.Fatal("unobserved zero gauge", text)
				}
			}
			if mode == "drained-diagnostics-failed" && meta.IsStatusConditionTrue(state.Conditions, "RetentionBlocked") {
				t.Fatal("diagnostics vetoed drained work")
			}
		})
	}
}

func TestRetentionObservedOwnerDisableAndObservedEmpty(t *testing.T) {
	a, c, _ := fixture(t, false)
	ctx := context.Background()
	now := time.Now().UTC()
	m := newBackupMetrics()
	owner := repository.UUID()
	for pass := 0; pass < 3; pass++ {
		o, e := a.Get(ctx, repositories, "test", "destination")
		if e != nil {
			t.Fatal(e)
		}
		o.Object["spec"].(map[string]any)["retention"] = map[string]any{"enabled": pass != 1, "dryRun": false, "window": "1h", "minimumFulls": int64(1), "interval": "5m"}
		if _, e = a.Client.Resource(repositories).Namespace("test").Update(ctx, o, metav1.UpdateOptions{}); e != nil {
			t.Fatal(e)
		}
		run := func(context.Context, string, string, configuration.Spec, time.Time) (retentionObservation, error) {
			if pass == 0 {
				return retentionObservation{gateObserved: true, admissionBlocked: true, result: retention.Result{OperationID: owner}}, repository.ErrBlocked
			}
			if pass == 1 {
				t.Fatal("disabled dispatched GC")
			}
			return retentionObservation{gateObserved: true}, nil
		}
		if e = a.reconcileRetention(ctx, m, unstruct(c), now.Add(time.Duration(pass)*time.Minute), run); e != nil {
			t.Fatal(e)
		}
		o, e = a.Get(ctx, repositories, "test", "destination")
		if e != nil {
			t.Fatal(e)
		}
		var state repositoryStatus
		b, _ := json.Marshal(o.Object["status"])
		json.Unmarshal(b, &state)
		want := []metav1.ConditionStatus{metav1.ConditionTrue, metav1.ConditionUnknown, metav1.ConditionFalse}[pass]
		condition := meta.FindStatusCondition(state.Conditions, "RepositoryAdmissionBlocked")
		if condition == nil || condition.Status != want {
			t.Fatal(condition, want)
		}
		if pass == 0 && state.Retention.OperationID != owner {
			t.Fatal("lost observed owner's operation ID", state.Retention)
		}
		text := metricText(m)
		if pass == 1 && (strings.Contains(text, "cnpg_backup_repository_admission_blocked{") || strings.Contains(text, "cnpg_backup_repository_holders{")) {
			t.Fatal("stale healthy/blocked gauges survived unknown observation", text)
		}
		if pass != 1 && !strings.Contains(text, "cnpg_backup_repository_admission_blocked{") {
			t.Fatal("observed gauge omitted", text)
		}
	}
}
