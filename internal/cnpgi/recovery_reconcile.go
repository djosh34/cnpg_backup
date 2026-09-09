package cnpgi

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// A restart may finish a DURABLY recorded terminal release, but never recreate
// its predecessor's active observer or clear a process-reader from API status.
// One Cluster per turn bounds API/storage work and namespace memory.
func (a *API) runRecoveryOperations(ctx context.Context, metrics *backupMetrics) {
	if len(a.Namespaces) == 0 {
		return
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	ns, cursor := 0, ""
	for {
		pass, cancel := context.WithTimeout(ctx, 30*time.Second)
		list, e := a.Client.Resource(clusters).Namespace(a.Namespaces[ns]).List(pass, meta.ListOptions{Limit: 1, Continue: cursor})
		if e == nil {
			for _, o := range list.Items {
				c, e := ParseCluster([]byte(jsonText(o.Object)))
				if e != nil || c.Spec.Bootstrap.Recovery == nil {
					continue
				}
				a.reconcileRecoveryOperation(pass, c, metrics, time.Now())
			}
			cursor = list.GetContinue()
		} else {
			cursor = ""
		}
		cancel()
		if cursor == "" {
			ns = (ns + 1) % len(a.Namespaces)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Observe only validated durable state. Failed reads/persistence become Unknown;
// diagnostics cannot supply the uninterrupted observer's termination evidence.
func (a *API) reconcileRecoveryOperation(ctx context.Context, c Cluster, metrics *backupMetrics, now time.Time) {
	state := recoveryOperation{}
	known := false
	defer func() { metrics.restore(c, state, known, now) }()
	cm, err := a.Get(ctx, coreResource("configmaps"), c.Metadata.Namespace, operationName(c))
	if apierrors.IsNotFound(err) || err != nil {
		return
	}
	state, err = operationFrom(cm, c)
	if err != nil {
		return
	}
	if state.State == "active" && state.ObserverID != a.recoveryState().process {
		state.State = "uncertain"
		if a.writeOperation(ctx, c, cm, state) != nil {
			return
		}
		a.recoveryWarning(ctx, c, "RetentionBlocked")
	}
	known = state.State == "active" || state.State == "uncertain" || state.State == "completed"
	if state.State != "completed" || state.LifetimeReleased || state.CompletedJobUID == "" || len(state.TerminatedPodUIDs) == 0 {
		return
	}
	source, err := a.Repository(ctx, c.Metadata.Namespace, state.Placement.Source)
	if err != nil || source.Hash() != state.Placement.SourceConfigSHA256 {
		return
	}
	if a.changeRecoveryLifetime(ctx, c, source, true) != nil {
		return
	}
	state.LifetimeReleased = true
	if a.writeOperation(ctx, c, cm, state) != nil {
		known = false
	}
}

func (a *API) recoveryWarning(ctx context.Context, c Cluster, reason string) {
	now := time.Now().UTC().Format(time.RFC3339)
	event := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Event", "metadata": map[string]any{"generateName": c.Metadata.Name + "-recovery-", "namespace": c.Metadata.Namespace},
		"involvedObject": map[string]any{"apiVersion": c.APIVersion, "kind": "Cluster", "name": c.Metadata.Name, "namespace": c.Metadata.Namespace, "uid": string(c.Metadata.UID)},
		"type":           "Warning", "reason": reason, "message": "Recovery admission or termination is uncertain; source deletion protection is retained without expiry. Retry poisoned targets with a fresh Cluster and all fresh target PVCs.",
		"source": map[string]any{"component": "cnpg-backup"}, "firstTimestamp": now, "lastTimestamp": now, "count": int64(1),
	}}
	_, _ = a.Client.Resource(coreResource("events")).Namespace(c.Metadata.Namespace).Create(ctx, event, meta.CreateOptions{})
}
