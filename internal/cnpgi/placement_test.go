package cnpgi

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func raw(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
func unstruct(v any) *unstructured.Unstructured {
	var result map[string]any
	if err := json.Unmarshal(raw(v), &result); err != nil {
		panic(err)
	}
	return &unstructured.Unstructured{Object: result}
}
func fixture(t *testing.T, recovery bool) (*API, Cluster, core.Pod) {
	t.Helper()
	c, err := ParseCluster([]byte(`{"apiVersion":"postgresql.cnpg.io/v1","kind":"Cluster","metadata":{"name":"database","namespace":"test","uid":"11111111-1111-4111-8111-111111111111"},"spec":{"imageName":"ghcr.io/cloudnative-pg/postgresql@` + DatabaseDigest + `","plugins":[{"name":"cnpg-backup.djosh34.github.io","parameters":{"repository":"destination"}}],"walStorage":{"size":"1Gi"},"tablespaces":[{"name":"fast_space"}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	if recovery {
		value := map[string]any{}
		json.Unmarshal(raw(c), &value)
		spec := value["spec"].(map[string]any)
		spec["bootstrap"] = map[string]any{"recovery": map[string]any{"source": "origin"}}
		spec["externalClusters"] = []any{map[string]any{"name": "origin", "plugin": map[string]any{"name": recoveryguard.PluginName, "parameters": map[string]any{"repository": "source"}}}}
		c, err = ParseCluster(raw(value))
		if err != nil {
			t.Fatal(err)
		}
	}
	repository := configuration.Defaults()
	repository.RepositoryID = "22222222-2222-4222-8222-222222222222"
	repository.S3.Endpoint = "https://minio.test"
	repository.S3.Bucket = "test-bucket"
	repository.S3.Prefix = "repository"
	repository.S3.AccessKeySecret = configuration.Selector{Name: "auth", Key: "access"}
	repository.S3.SecretKeySecret = configuration.Selector{Name: "auth", Key: "secret"}
	repository.Workspace.StorageClassName = "bounded-disk"
	repository.Workspace.Size = "1Gi"
	dst := &unstructured.Unstructured{Object: map[string]any{"apiVersion": configuration.Group + "/" + configuration.Version, "kind": "Repository", "metadata": map[string]any{"name": "destination", "namespace": "test"}, "spec": unstruct(repository).Object}}
	src := dst.DeepCopy()
	src.SetName("source")
	src.Object["spec"].(map[string]any)["repositoryID"] = "33333333-3333-4333-8333-333333333333"
	secret := &core.Secret{TypeMeta: meta.TypeMeta{APIVersion: "v1", Kind: "Secret"}, ObjectMeta: meta.ObjectMeta{Name: "auth", Namespace: "test"}, Data: map[string][]byte{"access": []byte("test-access"), "secret": []byte("test-secret")}}
	replication := &core.Secret{TypeMeta: meta.TypeMeta{APIVersion: "v1", Kind: "Secret"}, ObjectMeta: meta.ObjectMeta{Name: "database-replication", Namespace: "test"}, Data: map[string][]byte{"tls.crt": []byte("test-only-cert"), "tls.key": []byte("test-only-key")}}
	ca := &core.Secret{TypeMeta: meta.TypeMeta{APIVersion: "v1", Kind: "Secret"}, ObjectMeta: meta.ObjectMeta{Name: "database-ca", Namespace: "test"}, Data: map[string][]byte{"ca.crt": []byte("test-only-ca"), "ca.key": []byte("must-not-project")}}
	objects := []runtime.Object{unstruct(c), dst, src, unstruct(secret), unstruct(replication), unstruct(ca)}
	command := []string{"/controller/manager", "instance", "run"}
	if recovery {
		command = []string{"/controller/manager", "instance", "restore", "--pg-wal", "/var/lib/postgresql/wal/pg_wal", "--log-level=info"}
	}
	pod := core.Pod{TypeMeta: meta.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: meta.ObjectMeta{Name: "database-1", Namespace: "test"}, Spec: core.PodSpec{SecurityContext: &core.PodSecurityContext{RunAsUser: ptr(int64(26)), RunAsGroup: ptr(int64(26)), FSGroup: ptr(int64(26))}, InitContainers: []core.Container{{Name: "bootstrap-controller", Command: []string{"/manager", "bootstrap", "/controller/manager"}}}, Containers: []core.Container{{Name: "postgres", Command: command}}}}
	for index, target := range []struct{ name, mount string }{{"pgdata", "/var/lib/postgresql/data"}, {"pg-wal", "/var/lib/postgresql/wal"}, {"tbs-fast-space", "/var/lib/postgresql/tablespaces/fast_space"}} {
		pod.Spec.Volumes = append(pod.Spec.Volumes, core.Volume{Name: target.name, VolumeSource: core.VolumeSource{PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{ClaimName: target.name}}})
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, core.VolumeMount{Name: target.name, MountPath: target.mount})
		uid := []string{"44444444-4444-4444-8444-444444444444", "55555555-5555-4555-8555-555555555555", "66666666-6666-4666-8666-666666666666"}[index]
		objects = append(objects, unstruct(&core.PersistentVolumeClaim{TypeMeta: meta.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"}, ObjectMeta: meta.ObjectMeta{Name: target.name, Namespace: "test", UID: types.UID(uid), OwnerReferences: []meta.OwnerReference{{APIVersion: c.APIVersion, Kind: c.Kind, Name: c.Metadata.Name, UID: c.Metadata.UID, Controller: ptr(true)}}}, Spec: core.PersistentVolumeClaimSpec{VolumeMode: ptr(core.PersistentVolumeFilesystem)}}))
	}
	api := &API{Client: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), objects...), Namespaces: []string{"test"}, SecretNames: map[string][]string{"test": {"auth", "database-replication", "database-ca"}}}
	return api, c, pod
}
func apply(t *testing.T, object, patch []byte) []byte {
	t.Helper()
	p, err := jsonpatch.DecodePatch(patch)
	if err != nil {
		t.Fatal(err)
	}
	result, err := p.Apply(object)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func TestPlacementGoldenIdempotencyAndRecoveryFence(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		t.Run(map[bool]string{false: "instance", true: "recovery"}[recovery], func(t *testing.T) {
			api, c, pod := fixture(t, recovery)
			ctx := context.Background()
			original := raw(pod)
			patch, err := Place(ctx, api, c, original, "test-only-image")
			if err != nil {
				t.Fatal(err)
			}
			changed := apply(t, original, patch)
			var actual core.Pod
			if err := json.Unmarshal(changed, &actual); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual.Spec.InitContainers[0], pod.Spec.InitContainers[0]) {
				t.Fatal("bootstrap container modified")
			}
			sidecar := actual.Spec.InitContainers[len(actual.Spec.InitContainers)-1]
			if sidecar.RestartPolicy == nil || *sidecar.RestartPolicy != core.ContainerRestartPolicyAlways || sidecar.StartupProbe == nil || !*sidecar.SecurityContext.ReadOnlyRootFilesystem || *sidecar.SecurityContext.RunAsUser != 26 {
				t.Fatal("unsafe sidecar")
			}
			if strings.Contains(string(raw(actual.Spec.Containers)), "test-secret") || strings.Contains(string(raw(actual.Spec.Containers)), "cnpg-backup-projection") {
				t.Fatal("main got secret projection")
			}
			if recovery && (actual.Spec.Containers[0].Command[0] != recoveryguard.HelperPath || !strings.Contains(string(raw(actual.Spec.Containers[0].Env)), "metadata.uid")) {
				t.Fatal("unguarded recovery")
			}
			count := 0
			for _, v := range actual.Spec.Volumes {
				if v.Ephemeral != nil {
					count++
					if v.Ephemeral.VolumeClaimTemplate.Spec.StorageClassName == nil {
						t.Fatal("unbounded workspace")
					}
				}
			}
			if count != 1 {
				t.Fatal("workspace is not per-Pod generic ephemeral")
			}
			again, err := Place(ctx, api, c, changed, "test-only-image")
			if err != nil || len(again) != 0 {
				t.Fatalf("not idempotent: %s %v", again, err)
			}
			rollout, err := Place(ctx, api, c, changed, "next-test-only-image")
			if err != nil || len(rollout) == 0 {
				t.Fatal("image change did not evaluate for rollout", err)
			}
			name := "instance.json"
			if recovery {
				name = "recovery.json"
			}
			pretty, _ := json.MarshalIndent(actual, "", "  ")
			pretty = append(pretty, '\n')
			path := filepath.Join("testdata", name)
			if os.Getenv("UPDATE_GOLDEN") == "1" {
				if err := os.MkdirAll("testdata", 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, pretty, 0644); err != nil {
					t.Fatal(err)
				}
			}
			expected, err := os.ReadFile(path)
			if err != nil || string(pretty) != string(expected) {
				t.Fatal("placement differs from reviewed golden", err)
			}
		})
	}
}
func TestRecoveryJobAndAllFreshUIDBinding(t *testing.T) {
	api, c, pod := fixture(t, true)
	ctx := context.Background()
	job := batch.Job{TypeMeta: meta.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}, ObjectMeta: meta.ObjectMeta{Name: "recovery", Namespace: "test"}, Spec: batch.JobSpec{Template: core.PodTemplateSpec{Spec: pod.Spec}}}
	patch, err := Place(ctx, api, c, raw(job), "test-only-image")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(patch), "/spec/template/spec") {
		t.Fatal("Job path not patched")
	}
	if _, err := Place(ctx, api, c, apply(t, raw(job), patch), "test-only-image"); err != nil {
		t.Fatal(err)
	}
	pvc, err := api.Get(ctx, coreResource("persistentvolumeclaims"), "test", "pgdata")
	if err != nil {
		t.Fatal(err)
	}
	pvc.SetUID(types.UID(recoveryguard.NewUUID()))
	if _, err := api.Client.Resource(coreResource("persistentvolumeclaims")).Namespace("test").Update(ctx, pvc, meta.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := Place(ctx, api, c, raw(job), "test-only-image"); err == nil {
		t.Fatal("replacement PVC silently changed immutable operation targets")
	}
}
func TestPlacementRejectsUnsafeLayoutsAndDoesNotReadSourceInInstance(t *testing.T) {
	for name, change := range map[string]func(*core.Pod){
		"hostPID": func(p *core.Pod) { p.Spec.HostPID = true }, "sharedPID": func(p *core.Pod) { p.Spec.ShareProcessNamespace = ptr(true) },
		"uid": func(p *core.Pod) { p.Spec.SecurityContext.RunAsUser = ptr(int64(0)) }, "argv": func(p *core.Pod) {
			p.Spec.Containers[0].Command = append(p.Spec.Containers[0].Command, "--unrecognized")
		},
		"missingWAL": func(p *core.Pod) { p.Spec.Containers[0].VolumeMounts = p.Spec.Containers[0].VolumeMounts[:1] },
		"sidecarCollision": func(p *core.Pod) {
			p.Spec.InitContainers = append(p.Spec.InitContainers, core.Container{Name: "cnpg-backup"})
		},
	} {
		t.Run(name, func(t *testing.T) {
			api, c, pod := fixture(t, true)
			change(&pod)
			if _, err := Place(context.Background(), api, c, raw(pod), "test-only-image"); err == nil {
				t.Fatal("unsafe placement accepted")
			}
		})
	}
	api, c, pod := fixture(t, true)
	pod.Spec.Containers[0].Command = []string{"/controller/manager", "instance", "run"}
	if err := api.Client.Resource(repositories).Namespace("test").Delete(context.Background(), "source", meta.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := Place(context.Background(), api, c, raw(pod), "test-only-image"); err != nil {
		t.Fatal("ordinary instance depended on source", err)
	}
}
func TestBackupTypeHasNoFallback(t *testing.T) {
	for _, target := range []string{"", "prefer-standby", "primary"} {
		for _, kind := range []string{"", "full", "differential", "auto"} {
			err := ValidateBackup(target, map[string]string{"backupType": kind})
			want := target == "primary" && (kind == "full" || kind == "differential")
			if (err == nil) != want {
				t.Fatal(target, kind, err)
			}
		}
	}
}
