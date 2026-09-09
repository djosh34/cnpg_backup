// Copyright 2026 cnpg_backup contributors. All rights reserved.
package cnpgi

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/retention"
	"github.com/djosh34/cnpg_backup/internal/wal"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

type retentionStatus struct {
	CheckedAt         metav1.Time  `json:"checkedAt"`
	NextRun           metav1.Time  `json:"nextRun"`
	ConfigurationHash string       `json:"configurationHash"`
	OperationID       string       `json:"operationID,omitempty"`
	Planned           int          `json:"planned"`
	DryRun            bool         `json:"dryRun"`
	LastWarningTime   *metav1.Time `json:"lastWarningTime,omitempty"`
}
type retentionObservation struct {
	result           retention.Result
	holders          int
	admissionBlocked bool
	gateObserved     bool
}
type retentionRunner func(context.Context, string, string, configuration.Spec, time.Time) (retentionObservation, error)

func (a *API) retentionBatch(ctx context.Context, namespace, writer string, spec configuration.Spec, now time.Time) (out retentionObservation, err error) {
	snap, e := a.backupSnapshot(ctx, namespace, spec)
	if e != nil {
		return out, e
	}
	store, e := snap.Store()
	if e != nil {
		return out, e
	}
	defer store.Close()
	dir, e := newRetentionWorkspace(retentionWorkspacePath)
	if e != nil {
		return out, e
	}
	defer os.RemoveAll(dir)
	r, e := repository.OpenSource(ctx, store, spec.RepositoryID, dir)
	if e != nil {
		return out, e
	}
	if r.Identity().WriterClusterUID != writer {
		return out, repository.ErrIdentity
	}
	window, _ := time.ParseDuration(spec.Retention.Window)
	out.result, err = retention.Run(ctx, r, wal.Files{Repository: r, Store: store, Workspace: dir, Compression: spec.Compression}, now, retention.Options{Enabled: spec.Retention.Enabled, DryRun: spec.Retention.DryRun, Window: window, MinimumFulls: spec.Retention.MinimumFulls})
	// Diagnostics after work; failed observation cannot change request ownership
	// or turn successfully completed destruction into a failed destructive batch.
	if gate, e := r.ObserveGate(ctx); e == nil {
		out.gateObserved = true
		out.holders = len(gate.Holders)
		out.admissionBlocked = gate.Owner != nil
		if gate.Owner != nil {
			out.result.OperationID = gate.Owner.OperationID
		}
	}
	return out, err
}

func (a *API) reconcileRetention(ctx context.Context, m *backupMetrics, c *unstructured.Unstructured, now time.Time, run retentionRunner) error {
	labels, spec, e := a.backupConfiguration(ctx, c)
	if e != nil {
		return e
	}
	// The configuration source was read afresh above. Status updates below carry
	// a resourceVersion precondition; neither status nor events grant GC admission.
	plugins, _, _ := unstructured.NestedSlice(c.Object, "spec", "plugins")
	name := ""
	for _, p := range plugins {
		v, ok := p.(map[string]any)
		if ok {
			n, _, _ := unstructured.NestedString(v, "parameters", "repository")
			if n != "" && v["name"] == "cnpg-backup.djosh34.github.io" {
				name = n
			}
		}
	}
	object, e := a.Get(ctx, repositories, c.GetNamespace(), name)
	if e != nil {
		return e
	}
	b, e := json.Marshal(object.Object["spec"])
	if e != nil {
		return e
	}
	current, e := configuration.DecodeSpec(b)
	if e != nil || current.Hash() != spec.Hash() {
		return errors.New("retention configuration changed")
	}
	var state repositoryStatus
	b, _ = json.Marshal(object.Object["status"])
	if e = json.Unmarshal(b, &state); e != nil {
		return e
	}
	if state.Retention != nil && state.Retention.ConfigurationHash == spec.Hash() && now.Before(state.Retention.NextRun.Time) {
		return nil
	}
	interval, _ := time.ParseDuration(spec.Retention.Interval)
	next := &retentionStatus{CheckedAt: metav1.NewTime(now), NextRun: metav1.NewTime(now.Add(interval)), ConfigurationHash: spec.Hash(), DryRun: spec.Retention.DryRun}
	if state.Retention != nil {
		next.LastWarningTime = state.Retention.LastWarningTime
	}
	observation := retentionObservation{}
	reason, message := "Disabled", "Automatic retention is disabled. Repository holders remain enforced."
	if spec.Retention.Enabled {
		observation, e = run(ctx, c.GetNamespace(), string(c.GetUID()), spec, now)
		next.OperationID, next.Planned = observation.result.OperationID, observation.result.Planned
		reason, message = "BatchComplete", "Exclusive retention inventory and bounded batch completed."
		if spec.Retention.DryRun {
			reason, message = "DryRun", "Exclusive inventory planned without deleting data."
		}
		if observation.result.Decision.Shortened {
			if observation.result.Decision.Current == 0 {
				message += " No usable backups; recovery window is unavailable. Only proven orphan cleanup is eligible; WAL is untouched."
			} else {
				message += " No usable pre-cutoff anchor; recovery window is shortened."
			}
		}
		if e == nil && !spec.Retention.DryRun && observation.result.Planned > 0 {
			next.NextRun = metav1.NewTime(time.Now().Add(time.Second))
		}
	}
	blocked := e != nil
	if blocked {
		reason, message = "RetentionBlocked", "Deletion stopped: repository admission, metadata, recovery coverage or storage outcome is unavailable or uncertain. Protection never expires."
	}
	condition := metav1.ConditionFalse
	if blocked {
		condition = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&state.Conditions, metav1.Condition{Type: "RetentionBlocked", Status: condition, Reason: reason, Message: message, ObservedGeneration: object.GetGeneration(), LastTransitionTime: metav1.NewTime(now)})
	admission, admissionReason := metav1.ConditionUnknown, "GateUnobserved"
	admissionMessage := "Gate was not observed. Disabling retention or failed diagnostics never clears an owner or establishes admission."
	if observation.gateObserved {
		admission, admissionReason = metav1.ConditionFalse, "GateObservation"
		admissionMessage = "A set GC owner excludes new protected work; a lost or uncertain owner never expires."
		if observation.admissionBlocked {
			admission = metav1.ConditionTrue
		}
	}
	meta.SetStatusCondition(&state.Conditions, metav1.Condition{Type: "RepositoryAdmissionBlocked", Status: admission, Reason: admissionReason, Message: admissionMessage, ObservedGeneration: object.GetGeneration(), LastTransitionTime: metav1.NewTime(now)})
	warn := blocked && (next.LastWarningTime == nil || !now.Before(next.LastWarningTime.Add(5*time.Minute)))
	if warn {
		stamp := metav1.NewTime(now)
		next.LastWarningTime = &stamp
	}
	state.Retention = next
	m.retention(labels, blocked, observation.admissionBlocked, observation.gateObserved, observation.holders, now)
	patch, _ := json.Marshal(map[string]any{"metadata": map[string]any{"resourceVersion": object.GetResourceVersion()}, "status": state})
	_, e = a.Client.Resource(repositories).Namespace(object.GetNamespace()).Patch(ctx, object.GetName(), types.MergePatchType, patch, metav1.PatchOptions{}, "status")
	if e != nil {
		return e
	}
	if warn {
		event := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Event", "metadata": map[string]any{"generateName": object.GetName() + "-retention-", "namespace": object.GetNamespace()}, "involvedObject": map[string]any{"apiVersion": object.GetAPIVersion(), "kind": "Repository", "name": object.GetName(), "namespace": object.GetNamespace(), "uid": string(object.GetUID())}, "type": "Warning", "reason": "RetentionBlocked", "message": message, "source": map[string]any{"component": "cnpg-backup"}, "firstTimestamp": now.UTC().Format(time.RFC3339), "lastTimestamp": now.UTC().Format(time.RFC3339), "count": int64(1)}}
		_, _ = a.Client.Resource(coreResource("events")).Namespace(object.GetNamespace()).Create(ctx, event, metav1.CreateOptions{})
	}
	return nil
}

// Exactly one serial worker, one bounded Cluster page per turn. A new store and
// process-specific repository handle each batch prevents accidental adoption
// across credential changes or manager restarts. Gate ownership is authoritative.
func (a *API) runRetention(ctx context.Context, m *backupMetrics) {
	if len(a.Namespaces) == 0 {
		return
	}
	namespace, continuation := 0, ""
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		pass, cancel := context.WithTimeout(ctx, 90*time.Second)
		list, e := a.Client.Resource(clusters).Namespace(a.Namespaces[namespace]).List(pass, metav1.ListOptions{Limit: 1, Continue: continuation})
		if e == nil {
			for i := range list.Items {
				_ = a.reconcileRetention(pass, m, &list.Items[i], time.Now(), a.retentionBatch)
			}
			continuation = list.GetContinue()
		} else {
			continuation = ""
		}
		cancel()
		if continuation == "" {
			namespace = (namespace + 1) % len(a.Namespaces)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
