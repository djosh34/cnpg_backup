package cnpgi

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	"github.com/djosh34/cnpg_backup/internal/repository"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

// Immutable Placement+Targets and monotonic terminal state share ONE owned CM.
// Only this manager writes it using resourceVersion updates. A lost observer
// cannot adopt its predecessor's termination knowledge: retain source protection.
type recoveryOperation struct {
	Placement         RecoveryPlacement    `json:"placement"`
	Targets           recoveryguard.Config `json:"targets"`
	ObserverID        string               `json:"observerID"`
	State             string               `json:"state"` // active, uncertain, completed
	CompletedJobUID   string               `json:"completedJobUID,omitempty"`
	TerminatedPodUIDs []string             `json:"terminatedPodUIDs,omitempty"`
	LifetimeReleased  bool                 `json:"lifetimeReleased"`
	LastWarningAt     string               `json:"lastWarningAt,omitempty"`
}

func operationName(c Cluster) string { return c.Metadata.Name + "-cb-recovery" }
func activeOperation(root *os.Root, p RecoveryPlacement) error {
	b, e := configuration.Read(root, "operation.json", 128<<10)
	if e != nil {
		return e
	}
	var op recoveryOperation
	if configuration.StrictJSON(b, &op) != nil || op.State != "active" || !reflect.DeepEqual(op.Placement, p) {
		return errRecoveryClosed
	}
	return nil
}
func operationFrom(o *unstructured.Unstructured, c Cluster) (recoveryOperation, error) {
	var state recoveryOperation
	owner := meta.GetControllerOf(o)
	if owner == nil || owner.UID != c.Metadata.UID || owner.Kind != "Cluster" || o.GetDeletionTimestamp() != nil {
		return state, repository.ErrIdentity
	}
	text, ok, e := unstructured.NestedString(o.Object, "data", "operation.json")
	if e != nil || !ok || len(text) > 128<<10 || configuration.StrictJSON([]byte(text), &state) != nil {
		return state, repository.ErrInvalid
	}
	if state.Placement.ClusterUID != string(c.Metadata.UID) || state.Placement.OperationID != c.OperationUID() || state.Targets.Validate() != nil {
		return state, repository.ErrIdentity
	}
	return state, nil
}
func (a *API) writeOperation(ctx context.Context, c Cluster, o *unstructured.Unstructured, next recoveryOperation) error {
	o = o.DeepCopy()
	o.Object["data"] = map[string]any{"operation.json": jsonText(next)}
	_, e := a.Client.Resource(coreResource("configmaps")).Namespace(c.Metadata.Namespace).Update(ctx, o, meta.UpdateOptions{})
	return e
}

type recoveryMonitor struct{ done bool }
type recoveryCoordinator struct {
	mu       sync.Mutex
	process  string
	monitors map[string]*recoveryMonitor
}

func (a *API) recoveryState() *recoveryCoordinator {
	a.recoveryMu.Lock()
	defer a.recoveryMu.Unlock()
	if a.recovery == nil {
		a.recovery = &recoveryCoordinator{process: repository.UUID(), monitors: map[string]*recoveryMonitor{}}
	}
	return a.recovery
}
func (a *API) ensureRecovery(ctx context.Context, c Cluster, placement RecoveryPlacement, guard recoveryguard.Config, source configuration.Spec) error {
	coordinator := a.recoveryState()
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	o, e := a.Get(ctx, coreResource("configmaps"), c.Metadata.Namespace, operationName(c))
	if apierrors.IsNotFound(e) {
		op := recoveryOperation{Placement: placement, Targets: guard, ObserverID: coordinator.process, State: "active"}
		o = &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": operationName(c), "namespace": c.Metadata.Namespace, "ownerReferences": []any{map[string]any{"apiVersion": c.APIVersion, "kind": "Cluster", "name": c.Metadata.Name, "uid": string(c.Metadata.UID), "controller": true}}}, "data": map[string]any{"operation.json": jsonText(op)}}}
		o, e = a.Client.Resource(coreResource("configmaps")).Namespace(c.Metadata.Namespace).Create(ctx, o, meta.CreateOptions{})
		if apierrors.IsAlreadyExists(e) {
			o, e = a.Get(ctx, coreResource("configmaps"), c.Metadata.Namespace, operationName(c))
		}
	}
	if e != nil {
		return e
	}
	state, e := operationFrom(o, c)
	if e != nil || !reflect.DeepEqual(state.Placement, placement) || !reflect.DeepEqual(state.Targets, guard) {
		return repository.ErrIdentity
	}
	if state.State != "active" {
		return errRecoveryClosed
	}
	if state.ObserverID != coordinator.process {
		state.State = "uncertain"
		_ = a.writeOperation(ctx, c, o, state)
		return errRecoveryClosed // never adopt another process's observations
	}
	if e = a.changeRecoveryLifetime(ctx, c, source, false); e != nil {
		now := time.Now().UTC()
		previous, _ := time.Parse(time.RFC3339, state.LastWarningAt)
		if previous.IsZero() || now.Sub(previous) >= 5*time.Minute {
			state.LastWarningAt = now.Format(time.RFC3339)
			if a.writeOperation(ctx, c, o, state) == nil {
				a.recoveryWarning(ctx, c, "RepositoryAdmissionBlocked")
			}
		}
		return errors.New("source repository admission unavailable")
	}
	if m := coordinator.monitors[c.OperationUID()]; m != nil {
		if m.done {
			return errRecoveryClosed
		}
		return nil
	}
	if len(coordinator.monitors) >= 16 {
		return repository.ErrCapacity
	}
	parent := a.recoveryContext
	if parent == nil {
		parent = context.Background()
	}
	watchCtx, cancel := context.WithCancel(parent)
	// Establish a LIST+WATCH before returning the Job creation patch. No existing
	// Pod can be silently adopted, and watch loss poisons completion knowledge.
	selector := "cnpg.io/cluster=" + c.Metadata.Name
	list, e := a.Client.Resource(coreResource("pods")).Namespace(c.Metadata.Namespace).List(ctx, meta.ListOptions{LabelSelector: selector, Limit: 1})
	if e != nil || len(list.Items) > 0 || list.GetContinue() != "" {
		cancel()
		return errors.New("recovery observation must precede all target Pods")
	}
	watchClient := a.recoveryWatch
	if watchClient == nil { // in-process Kubernetes test fixtures have no HTTP timeout
		watchClient = a.Client
	}
	// Bound only establishing the HTTP stream, never its admitted lifetime.
	startup := time.AfterFunc(10*time.Second, cancel)
	stream, e := watchClient.Resource(coreResource("pods")).Namespace(c.Metadata.Namespace).Watch(watchCtx, meta.ListOptions{LabelSelector: selector, ResourceVersion: list.GetResourceVersion(), AllowWatchBookmarks: true})
	if !startup.Stop() {
		if stream != nil {
			stream.Stop()
		}
		return context.DeadlineExceeded
	}
	if e != nil {
		cancel()
		return e
	}
	coordinator.monitors[c.OperationUID()] = &recoveryMonitor{}
	go func() { defer cancel(); a.observeRecovery(watchCtx, c, source, state, stream) }()
	return nil
}
func (a *API) changeRecoveryLifetime(ctx context.Context, c Cluster, source configuration.Spec, release bool) error {
	a.recoveryMu.Lock()
	lifetime := a.recoveryLifetime
	a.recoveryMu.Unlock()
	if lifetime != nil {
		return lifetime(ctx, c, source, release)
	}
	snap, e := a.backupSnapshot(ctx, c.Metadata.Namespace, source)
	if e != nil {
		return e
	}
	store, e := snap.Store()
	if e != nil {
		return e
	}
	defer store.Close()
	dir, e := os.MkdirTemp("/cnpg-backup/control", "gate-")
	if e != nil {
		return e
	}
	defer os.RemoveAll(dir)
	repo, e := repository.OpenSource(ctx, store, source.RepositoryID, dir)
	if e != nil {
		return e
	}
	if release {
		return repo.ReleaseLifetimeAfterTermination(ctx, string(c.Metadata.UID), c.OperationUID())
	}
	return repo.EstablishLifetime(ctx, string(c.Metadata.UID), c.OperationUID())
}

var jobsResource = schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}

func (a *API) observeRecovery(ctx context.Context, c Cluster, source configuration.Spec, state recoveryOperation, stream watch.Interface) {
	defer stream.Stop()
	durablyClosed := false
	defer func() {
		coordinator := a.recoveryState()
		coordinator.mu.Lock()
		defer coordinator.mu.Unlock()
		if durablyClosed {
			delete(coordinator.monitors, c.OperationUID())
		} else if m := coordinator.monitors[c.OperationUID()]; m != nil {
			m.done = true
		}
	}()
	pods := map[string]string{} // UID -> latest observed resourceVersion, no Pod bodies
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	uncertain := func() {
		pass, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		o, e := a.Get(pass, coreResource("configmaps"), c.Metadata.Namespace, operationName(c))
		if e != nil {
			return
		}
		got, e := operationFrom(o, c)
		if e != nil || got.ObserverID != state.ObserverID {
			return
		}
		if got.State == "uncertain" || got.State == "completed" {
			durablyClosed = true
		} else if got.State == "active" {
			got.State = "uncertain"
			if a.writeOperation(pass, c, o, got) == nil {
				// Only the durable closed operation replaces the in-memory fence.
				// Source holders remain untouched; an unacknowledged write keeps
				// the done monitor so this process cannot reopen observation.
				durablyClosed = true
				a.recoveryWarning(pass, c, "RetentionBlocked")
			}
		}
	}
	for {
		select {
		case <-ctx.Done():
			uncertain()
			return
		case event, ok := <-stream.ResultChan():
			if !ok || event.Type == watch.Error {
				uncertain()
				return
			}
			if event.Type == watch.Bookmark {
				continue
			}
			o, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				uncertain()
				return
			}
			if o.GetAnnotations()[operationAnnotation] != c.OperationUID() {
				continue
			}
			if event.Type == watch.Deleted || o.GetDeletionTimestamp() != nil {
				uncertain()
				return
			}
			if o.GetUID() == "" || o.GetResourceVersion() == "" || len(pods) >= 1024 {
				uncertain()
				return
			}
			pods[string(o.GetUID())] = o.GetResourceVersion()
			phase, _, _ := unstructured.NestedString(o.Object, "status", "phase")
			if phase != string(core.PodSucceeded) && phase != string(core.PodFailed) {
				continue
			}
			// Delivered terminal updates are an immediate opportunity to run the
			// SAME predicate. Work stays serial/bounded; no second controller.
		case <-ticker.C:
			// Fallback is required when Job Complete follows the final Pod event.
		}
		pass, cancel := context.WithTimeout(ctx, 20*time.Second)
		job, podIDs, e := a.recoveryTermination(pass, c, pods)
		if e != nil {
			cancel()
			uncertain()
			return
		}
		if job == "" {
			cancel()
			continue
		}
		o, e := a.Get(pass, coreResource("configmaps"), c.Metadata.Namespace, operationName(c))
		if e != nil {
			cancel()
			uncertain()
			return
		}
		got, e := operationFrom(o, c)
		if e != nil || got.State != "active" || got.ObserverID != state.ObserverID {
			cancel()
			uncertain()
			return
		}
		got.State = "completed"
		got.CompletedJobUID = job
		got.TerminatedPodUIDs = podIDs
		e = a.writeOperation(pass, c, o, got)
		cancel()
		if e != nil {
			uncertain()
			return
		}
		// Durable terminal close precedes release. Retry only the stable holder;
		// an uncertain/crashed process-reader is never removed by this manager.
		durablyClosed = true
		a.releaseCompletedRecovery(ctx, c, source)
		return
	}
}
func (a *API) releaseCompletedRecovery(ctx context.Context, c Cluster, source configuration.Spec) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		pass, cancel := context.WithTimeout(ctx, 30*time.Second)
		o, e := a.Get(pass, coreResource("configmaps"), c.Metadata.Namespace, operationName(c))
		if e == nil {
			state, se := operationFrom(o, c)
			if se != nil || state.State != "completed" || state.CompletedJobUID == "" || len(state.TerminatedPodUIDs) == 0 {
				cancel()
				return
			}
			if state.LifetimeReleased {
				cancel()
				return
			}
			if e = a.changeRecoveryLifetime(pass, c, source, true); e == nil {
				state.LifetimeReleased = true
				e = a.writeOperation(pass, c, o, state)
			}
		}
		cancel()
		if e == nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Require an observed successful Job AND every retry Pod's latest terminal
// status, including restartable init/ephemeral containers. Matching the final
// LIST's Pod resourceVersions to this uninterrupted ordered watch prevents
// certifying a snapshot before earlier deletion/replacement events were seen.
func (a *API) recoveryTermination(ctx context.Context, c Cluster, observed map[string]string) (string, []string, error) {
	var job *batch.Job
	e := a.visitRecoveryObjects(ctx, jobsResource, c, func(o *unstructured.Unstructured) error {
		var candidate batch.Job
		if runtime.DefaultUnstructuredConverter.FromUnstructured(o.Object, &candidate) != nil {
			return errRecoveryClosed
		}
		if candidate.Spec.Template.Annotations[operationAnnotation] != c.OperationUID() {
			return nil
		}
		owner := meta.GetControllerOf(&candidate)
		if owner == nil || owner.UID != c.Metadata.UID || candidate.DeletionTimestamp != nil || job != nil {
			return errRecoveryClosed
		}
		job = &candidate
		return nil
	})
	if e != nil {
		return "", nil, e
	}
	if job == nil {
		if len(observed) > 0 {
			return "", nil, errRecoveryClosed
		}
		return "", nil, nil
	}
	complete := false
	for _, condition := range job.Status.Conditions {
		if condition.Type == batch.JobFailed && condition.Status == core.ConditionTrue {
			return "", nil, errRecoveryClosed
		}
		if condition.Type == batch.JobComplete && condition.Status == core.ConditionTrue {
			complete = true
		}
	}
	if !complete {
		return "", nil, nil
	}
	if job.UID == "" || job.Status.Active != 0 || job.Status.Succeeded != 1 || job.Spec.Parallelism == nil || *job.Spec.Parallelism != 1 || job.Spec.Completions == nil || *job.Spec.Completions != 1 || (job.Status.UncountedTerminatedPods != nil && (len(job.Status.UncountedTerminatedPods.Succeeded) > 0 || len(job.Status.UncountedTerminatedPods.Failed) > 0)) {
		return "", nil, errRecoveryClosed
	}
	ids := []string{}
	waiting := false
	succeeded, failed := int32(0), int32(0)
	e = a.visitRecoveryObjects(ctx, coreResource("pods"), c, func(o *unstructured.Unstructured) error {
		if o.GetAnnotations()[operationAnnotation] != c.OperationUID() {
			return nil
		}
		var pod core.Pod
		if runtime.DefaultUnstructuredConverter.FromUnstructured(o.Object, &pod) != nil {
			return errRecoveryClosed
		}
		previous, ok := observed[string(pod.UID)]
		if !ok || previous != pod.ResourceVersion {
			waiting = true
			return nil
		} // watch must catch up
		owner := meta.GetControllerOf(&pod)
		if owner == nil || owner.UID != job.UID || !terminatedPod(pod) {
			return errRecoveryClosed
		}
		if pod.Status.Phase == core.PodSucceeded {
			succeeded++
		} else {
			failed++
		}
		ids = append(ids, string(pod.UID))
		return nil
	})
	if e != nil {
		return "", nil, e
	}
	if waiting {
		return "", nil, nil
	}
	if len(ids) != len(observed) || succeeded != 1 || failed != job.Status.Failed {
		return "", nil, errRecoveryClosed
	}
	sort.Strings(ids)
	return string(job.UID), ids, nil
}

// Never retain a namespace-sized Pod/Job response or certify a partial LIST.
func (a *API) visitRecoveryObjects(ctx context.Context, gvr schema.GroupVersionResource, c Cluster, visit func(*unstructured.Unstructured) error) error {
	cursor := ""
	count := 0
	for {
		list, e := a.Client.Resource(gvr).Namespace(c.Metadata.Namespace).List(ctx, meta.ListOptions{LabelSelector: "cnpg.io/cluster=" + c.Metadata.Name, Limit: 1, Continue: cursor})
		if e != nil {
			return e
		}
		for i := range list.Items {
			count++
			if count > 1024 {
				return repository.ErrCapacity
			}
			if e = visit(&list.Items[i]); e != nil {
				return e
			}
		}
		next := list.GetContinue()
		if next == "" {
			return nil
		}
		if next == cursor || len(list.Items) == 0 {
			return repository.ErrCorrupt
		}
		cursor = next
	}
}
func terminatedPod(p core.Pod) bool {
	if p.DeletionTimestamp != nil || (p.Status.Phase != core.PodSucceeded && p.Status.Phase != core.PodFailed) || p.Status.Reason == "NodeLost" || p.Status.Reason == "Shutdown" || p.Status.Reason == "Evicted" {
		return false
	}
	if p.Status.Phase == core.PodSucceeded {
		for _, s := range p.Status.ContainerStatuses {
			if s.State.Terminated == nil || s.State.Terminated.ExitCode != 0 {
				return false
			}
		}
	}
	check := func(names []string, statuses []core.ContainerStatus) bool {
		if len(names) != len(statuses) {
			return false
		}
		seen := map[string]bool{}
		for _, s := range statuses {
			if s.State.Terminated == nil || s.State.Terminated.FinishedAt.IsZero() || s.ContainerID == "" || seen[s.Name] {
				return false
			}
			if reason := s.State.Terminated.Reason; reason == "ContainerStatusUnknown" || reason == "Unknown" || reason == "NodeLost" {
				return false
			}
			seen[s.Name] = true
		}
		for _, name := range names {
			if !seen[name] {
				return false
			}
		}
		return true
	}
	names := func(containers []core.Container) []string {
		result := []string{}
		for _, c := range containers {
			result = append(result, c.Name)
		}
		return result
	}
	ephemeral := []string{}
	for _, c := range p.Spec.EphemeralContainers {
		ephemeral = append(ephemeral, c.Name)
	}
	return check(names(p.Spec.Containers), p.Status.ContainerStatuses) && check(names(p.Spec.InitContainers), p.Status.InitContainerStatuses) && check(ephemeral, p.Status.EphemeralContainerStatuses)
}
