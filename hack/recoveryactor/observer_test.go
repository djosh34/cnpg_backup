package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"testing"

	patch "gopkg.in/evanphx/json-patch.v4"
	admission "k8s.io/api/admission/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const actorImage = "observer@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func observe(t *testing.T, pod core.Pod) core.Pod {
	t.Helper()
	raw, _ := json.Marshal(pod)
	review, _ := json.Marshal(admission.AdmissionReview{Request: &admission.AdmissionRequest{UID: "request", Namespace: "campaign-target", Object: runtime.RawExtension{Raw: raw}}})
	response := httptest.NewRecorder()
	observerHandler(actorImage).ServeHTTP(response, httptest.NewRequest("POST", "/", bytes.NewReader(review)))
	var result admission.AdmissionReview
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Response == nil || !result.Response.Allowed {
		t.Fatalf("observer denied: %s", response.Body.String())
	}
	if len(result.Response.Patch) == 0 {
		return pod
	}
	p, err := patch.DecodePatch(result.Response.Patch)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = p.Apply(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &pod); err != nil {
		t.Fatal(err)
	}
	return pod
}

// The campaign retained status-only snapshots, not an API Pod spec. This is the
// production lifecycle golden shape plus CNPG's controller emptyDir arrangement;
// actual observer HTTP/JSONPatch and actual Python replacement caller follow.
func recoveryPod(t *testing.T) core.Pod {
	t.Helper()
	b, err := os.ReadFile("../../internal/cnpgi/testdata/recovery.json")
	if err != nil {
		t.Fatal(err)
	}
	var p core.Pod
	if err := json.Unmarshal(b, &p); err != nil {
		t.Fatal(err)
	}
	p.Name, p.Namespace, p.UID = "g-032-original", "campaign-target", "723d19f9-8009-4da7-821b-e440a374ce16"
	p.Labels = map[string]string{"cnpg.io/cluster": "g-032", "batch.kubernetes.io/controller-uid": "job"}
	controller := true
	p.OwnerReferences = []meta.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "g-032-job", UID: "job", Controller: &controller}}
	p.Spec.Containers[0].Name = "full-recovery"
	mount := core.VolumeMount{Name: "controller", MountPath: "/controller"}
	p.Spec.Containers[0].VolumeMounts = append(p.Spec.Containers[0].VolumeMounts, mount)
	p.Spec.InitContainers[0].VolumeMounts = []core.VolumeMount{mount}
	p.Spec.Volumes = append(p.Spec.Volumes, core.Volume{Name: "controller", VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}}})
	return p
}

func replacementPod(t *testing.T, pod core.Pod) core.Pod {
	t.Helper()
	raw, _ := json.Marshal(pod)
	command := exec.Command("python3", "-c", "import json,sys; sys.path.insert(0,'hack'); from test_recovery_replacement import construct; print(json.dumps(construct(json.load(sys.stdin))))")
	command.Dir = "../.."
	command.Stdin = bytes.NewReader(raw)
	raw, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &pod); err != nil {
		t.Fatal(err)
	}
	return pod
}

func TestObserverReplacementAdmission(t *testing.T) {
	original := observe(t, recoveryPod(t))
	constructed := replacementPod(t, original)
	replacement := observe(t, constructed)
	// API requires unique mountPath per container (not unique volume name).
	for _, c := range replacement.Spec.Containers {
		paths := map[string]bool{}
		for i, m := range c.VolumeMounts {
			if paths[m.MountPath] {
				t.Errorf("spec.containers[%s].volumeMounts[%d].mountPath: duplicate %q; API requires unique mountPath", c.Name, i, m.MountPath)
			}
			paths[m.MountPath] = true
		}
	}
	if !reflect.DeepEqual(constructed.Spec, replacement.Spec) {
		t.Error("re-admission changed already observed replacement spec")
	}
}

func TestObserverAdmissionIdempotent(t *testing.T) {
	first := observe(t, recoveryPod(t))
	second := observe(t, first)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("observer duplicated mutation on re-admission")
	}
}
