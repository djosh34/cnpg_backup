package cnpgi

import (
	"context"
	"testing"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestActualOperatorArtifactAndRolloutRequired(t *testing.T) {
	good := apps.Deployment{TypeMeta: meta.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}, ObjectMeta: meta.ObjectMeta{Name: "cnpg-controller-manager", Namespace: "cnpg-system", Generation: 3}, Spec: apps.DeploymentSpec{Replicas: ptr(int32(1)), Template: core.PodTemplateSpec{Spec: core.PodSpec{Containers: []core.Container{{Name: "manager", Image: OperatorImage}}}}}, Status: apps.DeploymentStatus{ObservedGeneration: 3, UpdatedReplicas: 1, AvailableReplicas: 1, Replicas: 1}}
	for name, change := range map[string]func(*apps.Deployment){"valid": func(*apps.Deployment) {}, "tag": func(d *apps.Deployment) {
		d.Spec.Template.Spec.Containers[0].Image = "ghcr.io/cloudnative-pg/cloudnative-pg:1.30.0"
	}, "old-version": func(d *apps.Deployment) { d.Spec.Template.Spec.Containers[0].Image = "wrong@sha256:other" }, "stale-status": func(d *apps.Deployment) { d.Status.ObservedGeneration = 2 }, "rollout": func(d *apps.Deployment) { d.Status.UpdatedReplicas = 0 }, "unavailable": func(d *apps.Deployment) { d.Status.AvailableReplicas = 0 }} {
		t.Run(name, func(t *testing.T) {
			d := good.DeepCopy()
			change(d)
			api := &API{OperatorNamespace: "cnpg-system", Client: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), unstruct(d))}
			err := api.VerifyOperator(context.Background())
			if (err == nil) != (name == "valid") {
				t.Fatal(err)
			}
		})
	}
}

func TestNegativeClusterSettingsAndNormalizedLayoutCollisions(t *testing.T) {
	for _, parameters := range []map[string]string{{"wal_level": "minimal"}, {"full_page_writes": "off"}, {"summarize_wal": "off"}} {
		_, c, _ := fixture(t, false)
		c.Spec.PostgreSQL.Parameters = parameters
		if _, _, err := c.Repositories(); err == nil {
			t.Fatal("incompatible settings accepted", parameters)
		}
	}
	for _, name := range []string{"../escape", "bad/name", "fast$space"} {
		_, c, _ := fixture(t, false)
		table := c.Spec.Tablespaces[0]
		table.Name = name
		c.Spec.Tablespaces = append(c.Spec.Tablespaces, table)
		if _, _, err := c.Repositories(); err == nil {
			t.Fatal("unsafe/colliding tablespace accepted", name)
		}
	}
}
