// Copyright 2026 cnpg_backup contributors. All rights reserved.
package cnpgi

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func lifecycleFixture(t *testing.T) (Lifecycle, Cluster, core.Pod) {
	t.Helper()
	api, c, pod := fixture(t, false)
	api.OperatorNamespace = "cnpg-system"
	d := apps.Deployment{TypeMeta: meta.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}, ObjectMeta: meta.ObjectMeta{Name: "cnpg-controller-manager", Namespace: api.OperatorNamespace, Generation: 1}, Spec: apps.DeploymentSpec{Replicas: ptr(int32(1)), Template: core.PodTemplateSpec{Spec: core.PodSpec{Containers: []core.Container{{Name: "manager", Image: OperatorImage}}}}}, Status: apps.DeploymentStatus{ObservedGeneration: 1, UpdatedReplicas: 1, AvailableReplicas: 1, Replicas: 1}}
	if _, err := api.Client.Resource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}).Namespace(api.OperatorNamespace).Create(context.Background(), unstruct(d), meta.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	return Lifecycle{API: api, Image: "test-only-image"}, c, pod
}

func hookPod(t *testing.T, s Lifecycle, c Cluster, pod core.Pod, op lifecycle.OperatorOperationType_Type) (core.Pod, []byte) {
	t.Helper()
	response, err := s.LifecycleHook(context.Background(), &lifecycle.OperatorLifecycleRequest{OperationType: &lifecycle.OperatorOperationType{Type: op}, ClusterDefinition: raw(c), ObjectDefinition: raw(pod)})
	if err != nil {
		t.Fatal(err)
	}
	result := *pod.DeepCopy()
	if len(response.JsonPatch) > 0 {
		if err := json.Unmarshal(apply(t, raw(pod), response.JsonPatch), &result); err != nil {
			t.Fatal(err)
		}
	}
	return result, response.JsonPatch
}

// Exercises the real manager entry point, not Place (the fresh-template builder).
// Admission defaults, token mounts and unrelated webhook fields must survive
// metadata reconciliation byte-for-byte, including fields unknown to our types.
func TestLifecycleLivePodPreservesAdmissionAndSnapshot(t *testing.T) {
	for _, op := range []lifecycle.OperatorOperationType_Type{lifecycle.OperatorOperationType_TYPE_PATCH, lifecycle.OperatorOperationType_TYPE_UPDATE} {
		for _, changed := range []bool{false, true} {
			t.Run(op.String()+map[bool]string{false: "/unchanged", true: "/new-config-and-image"}[changed], func(t *testing.T) {
				s, c, fresh := lifecycleFixture(t)
				pod, _ := hookPod(t, s, c, fresh, lifecycle.OperatorOperationType_TYPE_CREATE)
				pod.UID = "77777777-7777-4777-8777-777777777777"
				pod.ResourceVersion = "10"
				pod.OwnerReferences = []meta.OwnerReference{{APIVersion: c.APIVersion, Kind: c.Kind, Name: c.Metadata.Name, UID: c.Metadata.UID, Controller: ptr(true)}}
				pod.Spec.NodeName = "admitted-node"
				pod.Spec.DNSPolicy = core.DNSClusterFirst
				mount := core.VolumeMount{Name: "kube-api-access-test", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true}
				pod.Spec.Volumes = append(pod.Spec.Volumes, core.Volume{Name: mount.Name, VolumeSource: core.VolumeSource{Projected: &core.ProjectedVolumeSource{DefaultMode: ptr(int32(0420))}}})
				for i := range pod.Spec.InitContainers {
					pod.Spec.InitContainers[i].VolumeMounts = append(pod.Spec.InitContainers[i].VolumeMounts, mount)
					pod.Spec.InitContainers[i].Env = append(pod.Spec.InitContainers[i].Env, core.EnvVar{Name: "ADMISSION_FIELD", Value: "preserve"})
				}
				pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, mount)
				pod.Annotations["unrelated.example/annotation"] = "preserve"
				pod.Labels = map[string]string{"role": "primary"}
				if changed {
					obj, err := s.API.Get(context.Background(), repositories, "test", "destination")
					if err != nil {
						t.Fatal(err)
					}
					obj.Object["spec"].(map[string]any)["compression"] = "none"
					if _, err := s.API.Client.Resource(repositories).Namespace("test").Update(context.Background(), obj, meta.UpdateOptions{}); err != nil {
						t.Fatal(err)
					}
					s.Image = "next-test-only-image"
				}
				if _, err := s.API.Client.Resource(coreResource("pods")).Namespace("test").Create(context.Background(), unstruct(pod), meta.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
				next, patch := hookPod(t, s, c, pod, op)
				if !reflect.DeepEqual(pod.Spec, next.Spec) || !reflect.DeepEqual(pod.Annotations, next.Annotations) || len(patch) != 0 {
					t.Fatal("live hook rewrote admission fields or immutable image/config snapshot")
				}
				// Actual CNPG supplies a fresh desired Pod to EVALUATE, then uses its normal
				// rolling policy. A live no-op must not suppress new-template changes.
				desired, _ := hookPod(t, s, c, fresh, lifecycle.OperatorOperationType_TYPE_EVALUATE)
				if changed && (desired.Annotations[configAnnotation] == pod.Annotations[configAnnotation] || desired.Spec.InitContainers[2].Image == pod.Spec.InitContainers[2].Image) {
					t.Fatal("new configuration/image did not reach EVALUATE template")
				}
				created, _ := hookPod(t, s, c, fresh, lifecycle.OperatorOperationType_TYPE_CREATE)
				if !reflect.DeepEqual(desired, created) {
					t.Fatal("CREATE and EVALUATE disagree")
				}
			})
		}
	}
}

func TestLifecycleFreshValidationStillRejectsSecurityAndCollisions(t *testing.T) {
	for _, op := range []lifecycle.OperatorOperationType_Type{lifecycle.OperatorOperationType_TYPE_CREATE, lifecycle.OperatorOperationType_TYPE_EVALUATE} {
		for name, change := range map[string]func(*core.Pod){
			"private-PID": func(p *core.Pod) { p.Spec.HostPID = true },
			"identity":    func(p *core.Pod) { p.Spec.SecurityContext.RunAsUser = ptr(int64(0)) },
			"sidecar-collision": func(p *core.Pod) {
				p.Spec.InitContainers = append(p.Spec.InitContainers, core.Container{Name: "cnpg-backup"})
			},
			"volume-collision": func(p *core.Pod) { p.Spec.Volumes = append(p.Spec.Volumes, core.Volume{Name: "cnpg-backup-work"}) },
			"mount-collision": func(p *core.Pod) {
				p.Spec.Containers[0].VolumeMounts = append(p.Spec.Containers[0].VolumeMounts, core.VolumeMount{Name: "foreign", MountPath: "/plugins"})
			},
		} {
			t.Run(op.String()+"/"+name, func(t *testing.T) {
				s, c, p := lifecycleFixture(t)
				change(&p)
				_, err := s.LifecycleHook(context.Background(), &lifecycle.OperatorLifecycleRequest{OperationType: &lifecycle.OperatorOperationType{Type: op}, ClusterDefinition: raw(c), ObjectDefinition: raw(p)})
				if err == nil {
					t.Fatal("unsafe fresh placement accepted")
				}
			})
		}
	}
}
