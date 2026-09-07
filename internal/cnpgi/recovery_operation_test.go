package cnpgi

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func completionFixture(t *testing.T) (*API, Cluster, batch.Job, core.Pod) {
	api, c, pod := fixture(t, true)
	patch, e := Place(context.Background(), api, c, raw(pod), "test-image")
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(apply(t, raw(pod), patch), &pod); e != nil {
		t.Fatal(e)
	}
	pod.UID = types.UID(repository.UUID())
	pod.ResourceVersion = "7"
	pod.Labels = map[string]string{"cnpg.io/cluster": c.Metadata.Name}
	job := batch.Job{TypeMeta: meta.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}, ObjectMeta: meta.ObjectMeta{Name: "recovery", Namespace: c.Metadata.Namespace, UID: types.UID(repository.UUID()), Labels: pod.Labels, OwnerReferences: []meta.OwnerReference{{APIVersion: c.APIVersion, Kind: "Cluster", Name: c.Metadata.Name, UID: c.Metadata.UID, Controller: ptr(true)}}}, Spec: batch.JobSpec{Parallelism: ptr(int32(1)), Completions: ptr(int32(1)), Template: core.PodTemplateSpec{ObjectMeta: meta.ObjectMeta{Annotations: pod.Annotations}, Spec: pod.Spec}}, Status: batch.JobStatus{Succeeded: 1, Conditions: []batch.JobCondition{{Type: batch.JobComplete, Status: core.ConditionTrue}}}}
	pod.OwnerReferences = []meta.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: ptr(true)}}
	pod.Status.Phase = core.PodSucceeded
	finish := func(containers []core.Container) []core.ContainerStatus {
		out := []core.ContainerStatus{}
		for _, c := range containers {
			out = append(out, core.ContainerStatus{Name: c.Name, ContainerID: "containerd://" + c.Name, State: core.ContainerState{Terminated: &core.ContainerStateTerminated{ExitCode: 0, FinishedAt: meta.Now()}}})
		}
		return out
	}
	pod.Status.ContainerStatuses = finish(pod.Spec.Containers)
	pod.Status.InitContainerStatuses = finish(pod.Spec.InitContainers)
	return api, c, job, pod
}
func putCompletion(t *testing.T, api *API, c Cluster, job batch.Job, pods ...core.Pod) {
	t.Helper()
	ctx := context.Background()
	if _, e := api.Client.Resource(jobsResource).Namespace(c.Metadata.Namespace).Create(ctx, unstruct(job), meta.CreateOptions{}); e != nil {
		t.Fatal(e)
	}
	for _, p := range pods {
		if _, e := api.Client.Resource(coreResource("pods")).Namespace(c.Metadata.Namespace).Create(ctx, unstruct(p), meta.CreateOptions{}); e != nil {
			t.Fatal(e)
		}
	}
}
func TestCompletionRequiresAllOriginalRetryPodsAndContainers(t *testing.T) {
	for _, name := range []string{"success", "running sidecar", "unknown pod", "watch lag", "missing old retry", "deleting pod", "node lost", "unknown termination", "missing container status", "main failed", "job not complete", "wrong job owner", "failed retry accounted", "failed retry missing count", "multiple matching jobs"} {
		t.Run(name, func(t *testing.T) {
			api, c, job, pod := completionFixture(t)
			observed := map[string]string{string(pod.UID): pod.ResourceVersion}
			pods := []core.Pod{pod}
			positive := name == "success" || name == "failed retry accounted"
			switch name {
			case "running sidecar":
				pod.Status.InitContainerStatuses[len(pod.Status.InitContainerStatuses)-1].State = core.ContainerState{Running: &core.ContainerStateRunning{StartedAt: meta.Now()}}
			case "unknown pod":
				observed = map[string]string{}
			case "watch lag":
				observed[string(pod.UID)] = "6"
			case "missing old retry":
				old := *pod.DeepCopy()
				old.UID = types.UID(repository.UUID())
				observed[string(old.UID)] = old.ResourceVersion
			case "deleting pod":
				now := meta.Now()
				pod.DeletionTimestamp = &now
			case "node lost":
				pod.Status.Reason = "NodeLost"
			case "unknown termination":
				pod.Status.InitContainerStatuses[len(pod.Status.InitContainerStatuses)-1].State.Terminated.Reason = "ContainerStatusUnknown"
			case "missing container status":
				pod.Status.InitContainerStatuses = nil
			case "main failed":
				pod.Status.ContainerStatuses[0].State.Terminated.ExitCode = 1
			case "job not complete":
				job.Status.Conditions = nil
			case "wrong job owner":
				job.OwnerReferences[0].UID = types.UID(repository.UUID())
			case "failed retry accounted", "failed retry missing count":
				old := *pod.DeepCopy()
				old.Name = "failed-retry"
				old.UID = types.UID(repository.UUID())
				old.Status.Phase = core.PodFailed
				old.Status.ContainerStatuses[0].State.Terminated.ExitCode = 1
				pods = append(pods, old)
				observed[string(old.UID)] = old.ResourceVersion
				if name == "failed retry accounted" {
					job.Status.Failed = 1
				}
			}
			pods[0] = pod
			// For all but deliberate watch-lag/absence, the watch saw these exact states.
			if name != "unknown pod" && name != "watch lag" {
				observed[string(pod.UID)] = pod.ResourceVersion
			}
			putCompletion(t, api, c, job, pods...)
			if name == "multiple matching jobs" {
				other := job.DeepCopy()
				other.Name = "retry-job"
				other.UID = types.UID(repository.UUID())
				if _, e := api.Client.Resource(jobsResource).Namespace(c.Metadata.Namespace).Create(context.Background(), unstruct(other), meta.CreateOptions{}); e != nil {
					t.Fatal(e)
				}
			}
			uid, ids, e := api.recoveryTermination(context.Background(), c, observed)
			if positive {
				if e != nil || uid != string(job.UID) || len(ids) != len(pods) {
					t.Fatal(uid, ids, e)
				}
			} else if e == nil && uid != "" {
				t.Fatal("released on incomplete/uncertain evidence")
			}
		})
	}
}
func TestOriginalObserverRecordsTerminalBeforeAutomaticStableRelease(t *testing.T) {
	api, c, job, pod := completionFixture(t)
	var releases atomic.Int32
	api.recoveryMu.Lock()
	api.recoveryLifetime = func(ctx context.Context, _ Cluster, _ configuration.Spec, release bool) error {
		if release {
			cm, e := api.Get(ctx, coreResource("configmaps"), c.Metadata.Namespace, operationName(c))
			if e != nil {
				return e
			}
			state, e := operationFrom(cm, c)
			if e != nil {
				return e
			}
			if state.State != "completed" || state.CompletedJobUID != string(job.UID) || len(state.TerminatedPodUIDs) != 1 {
				return errRecoveryClosed
			}
			releases.Add(1)
		}
		return nil
	}
	api.recoveryMu.Unlock()
	putCompletion(t, api, c, job, pod)
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		cm, e := api.Get(context.Background(), coreResource("configmaps"), c.Metadata.Namespace, operationName(c))
		if e != nil {
			t.Fatal(e)
		}
		state, e := operationFrom(cm, c)
		if e != nil {
			t.Fatal(e)
		}
		if state.LifetimeReleased {
			if state.State != "completed" || releases.Load() != 1 {
				t.Fatal(state, releases.Load())
			}
			return
		}
		if state.State == "uncertain" {
			t.Fatal("positive original watcher lost evidence")
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("matching completed Job and all terminated Pods did not release stable hold")
}
func TestOperationNeverAdoptsAnotherManagerOrChangedBootstrap(t *testing.T) {
	api, c, pod := fixture(t, true)
	if _, e := Place(context.Background(), api, c, raw(pod), "test-image"); e != nil {
		t.Fatal(e)
	}
	cm, e := api.Get(context.Background(), coreResource("configmaps"), c.Metadata.Namespace, operationName(c))
	if e != nil {
		t.Fatal(e)
	}
	original, e := operationFrom(cm, c)
	if e != nil {
		t.Fatal(e)
	}
	changed := c
	copyOf := *c.Spec.Bootstrap.Recovery
	changed.Spec.Bootstrap.Recovery = &copyOf
	changed.Spec.Bootstrap.Recovery.RecoveryTarget = map[string]any{"targetTime": "2026-09-08T00:00:00Z"}
	if changed.OperationUID() == c.OperationUID() {
		t.Fatal("bootstrap did not change authoritative operation")
	}
	if _, e = Place(context.Background(), api, changed, raw(pod), "test-image"); e == nil {
		t.Fatal("changed bootstrap reused original target set")
	}
	// New process cannot turn an old active operation into fresh observation.
	api.recoveryMu.Lock()
	api.recovery = nil
	api.recoveryMu.Unlock()
	if _, e = Place(context.Background(), api, c, raw(pod), "test-image"); e == nil {
		t.Fatal("adopted old observer")
	}
	cm, e = api.Get(context.Background(), coreResource("configmaps"), c.Metadata.Namespace, operationName(c))
	if e != nil {
		t.Fatal(e)
	}
	state, e := operationFrom(cm, c)
	if e != nil || state.State != "uncertain" || state.ObserverID != original.ObserverID || state.LifetimeReleased {
		t.Fatal(state, e)
	}
}
