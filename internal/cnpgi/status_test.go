package cnpgi

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestRepositoryStatusGenerationInvalidRotationAndWarningThrottle(t *testing.T) {
	api, _, _ := fixture(t, false)
	ctx := context.Background()
	get := func() *unstructured.Unstructured {
		t.Helper()
		o, e := api.Get(ctx, repositories, "test", "destination")
		if e != nil {
			t.Fatal(e)
		}
		return o
	}
	object := get()
	object.SetGeneration(7)
	if _, e := api.Client.Resource(repositories).Namespace("test").Update(ctx, object, meta.UpdateOptions{}); e != nil {
		t.Fatal(e)
	}
	now := time.Now().UTC().Truncate(time.Second)
	reconcile := func(at time.Time) repositoryStatus {
		t.Helper()
		if e := api.ReconcileRepositoryStatus(ctx, get(), at); e != nil {
			t.Fatal(e)
		}
		var s repositoryStatus
		b, _ := json.Marshal(get().Object["status"])
		if e := json.Unmarshal(b, &s); e != nil {
			t.Fatal(e)
		}
		return s
	}
	valid := reconcile(now)
	if valid.ObservedGeneration != 7 || valid.ConfigurationHash == "" || !apimeta.IsStatusConditionTrue(valid.Conditions, "Ready") {
		t.Fatal(valid)
	}
	client := api.Client.(*dynamicfake.FakeDynamicClient)
	client.PrependReactor("create", "events", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured)
		object.SetName("event-" + recoveryguard.NewUUID())
		return false, nil, nil
	})
	writes := func(verb, resource string) int {
		n := 0
		for _, a := range client.Actions() {
			if a.GetVerb() == verb && a.GetResource().Resource == resource {
				n++
			}
		}
		return n
	}
	before := writes("patch", "repositories")
	reconcile(now.Add(time.Second))
	if writes("patch", "repositories") != before {
		t.Fatal("unchanged status caused churn")
	}
	if err := api.Client.Resource(coreResource("secrets")).Namespace("test").Delete(ctx, "auth", meta.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	invalid := reconcile(now.Add(2 * time.Second))
	if invalid.ConfigurationHash != "" || !apimeta.IsStatusConditionTrue(invalid.Conditions, "Invalid") || writes("create", "events") != 1 {
		t.Fatal(invalid)
	}
	// Repeated reconciles, including a fresh API wrapper (manager restart), must
	// use the persisted throttle, not an unbounded in-memory UID ledger.
	api = &API{Client: client, Namespaces: api.Namespaces, SecretNames: api.SecretNames}
	reconcile(now.Add(time.Minute))
	if writes("create", "events") != 1 {
		t.Fatal("Warning storm")
	}
	reconcile(now.Add(6 * time.Minute))
	if writes("create", "events") != 2 {
		t.Fatal("Warning did not resume after bound")
	}
	if apimeta.FindStatusCondition(invalid.Conditions, "RetentionBlocked").Status != meta.ConditionUnknown {
		t.Fatal("fabricated retention health")
	}
}
