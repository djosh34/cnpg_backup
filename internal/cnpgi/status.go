// Copyright 2026 cnpg_backup contributors. All rights reserved.
package cnpgi

import (
	"context"
	"encoding/json"
	"reflect"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

type repositoryStatus struct {
	ObservedGeneration int64              `json:"observedGeneration"`
	ConfigurationHash  string             `json:"configurationHash"`
	Conditions         []metav1.Condition `json:"conditions"`
	LastWarningTime    *metav1.Time       `json:"lastWarningTime,omitempty"`
}

// ReconcileRepositoryStatus owns configuration diagnostics only, not storage
// readiness or admission. Persisting the Warning throttle survives manager restarts.
// The resourceVersion precondition prevents recording a stale validation result.
func (a *API) ReconcileRepositoryStatus(ctx context.Context, object *unstructured.Unstructured, now time.Time) error {
	var previous repositoryStatus
	b, _ := json.Marshal(object.Object["status"])
	if err := json.Unmarshal(b, &previous); err != nil {
		return err
	}
	next := previous
	next.Conditions = append([]metav1.Condition(nil), previous.Conditions...)
	next.ObservedGeneration = object.GetGeneration()
	spec, err := a.Repository(ctx, object.GetNamespace(), object.GetName())
	valid := err == nil
	next.ConfigurationHash = ""
	if valid {
		next.ConfigurationHash = spec.Hash()
	}
	for _, c := range []struct {
		kind     string
		positive bool
	}{{"Ready", valid}, {"Invalid", !valid}} {
		value := metav1.ConditionFalse
		if c.positive {
			value = metav1.ConditionTrue
		}
		reason, message := "ConfigurationValid", "Configuration validated; data services remain unadvertised. This is not storage health or native capacity evidence."
		if !valid {
			reason, message = "ConfigurationInvalid", "Repository configuration or referenced credentials/trust are invalid or unavailable; new operations are rejected."
		}
		condition := metav1.Condition{Type: c.kind, Status: value, ObservedGeneration: object.GetGeneration(), Reason: reason, Message: message, LastTransitionTime: metav1.NewTime(now)}
		meta.SetStatusCondition(&next.Conditions, condition)
	}
	// Do not fabricate retention health before its implementation. Future storage
	// reconcilers own RetentionBlocked; preserve it if present.
	if meta.FindStatusCondition(next.Conditions, "RetentionBlocked") == nil {
		meta.SetStatusCondition(&next.Conditions, metav1.Condition{Type: "RetentionBlocked", Status: metav1.ConditionUnknown, Reason: "NotImplemented", Message: "Retention and repository admission are not implemented.", ObservedGeneration: object.GetGeneration(), LastTransitionTime: metav1.NewTime(now)})
	}
	warn := !valid && (previous.LastWarningTime == nil || !now.Before(previous.LastWarningTime.Add(5*time.Minute)))
	if warn {
		stamp := metav1.NewTime(now)
		next.LastWarningTime = &stamp
	}
	if reflect.DeepEqual(previous, next) {
		return nil
	}
	patch, _ := json.Marshal(map[string]any{"metadata": map[string]any{"resourceVersion": object.GetResourceVersion()}, "status": next})
	_, err = a.Client.Resource(repositories).Namespace(object.GetNamespace()).Patch(ctx, object.GetName(), types.MergePatchType, patch, metav1.PatchOptions{}, "status")
	if err != nil {
		return err
	}
	if warn {
		// Throttle is persisted before attempting best-effort Event delivery. Events
		// are diagnostics, never lock state; API failure cannot cause a warning storm.
		event := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Event", "metadata": map[string]any{"generateName": object.GetName() + "-invalid-", "namespace": object.GetNamespace()},
			"involvedObject": map[string]any{"apiVersion": object.GetAPIVersion(), "kind": "Repository", "name": object.GetName(), "namespace": object.GetNamespace(), "uid": string(object.GetUID())},
			"type":           "Warning", "reason": "ConfigurationInvalid", "message": "Repository configuration or referenced credentials/trust are invalid or unavailable.",
			"source": map[string]any{"component": "cnpg-backup"}, "firstTimestamp": now.UTC().Format(time.RFC3339), "lastTimestamp": now.UTC().Format(time.RFC3339), "count": int64(1),
		}}
		_, err = a.Client.Resource(coreResource("events")).Namespace(object.GetNamespace()).Create(ctx, event, metav1.CreateOptions{})
	}
	return err
}

// One bounded page per tick, serial API calls, no Secret watches, worker pool or
// per-Repository in-memory ledger. Expired continuation tokens restart that
// namespace; uncertain reads never update status. Total work has a 30s deadline.
func (a *API) RunRepositoryStatus(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	namespace, continuation := 0, ""
	for {
		pass, cancel := context.WithTimeout(ctx, 30*time.Second)
		list, err := a.Client.Resource(repositories).Namespace(a.Namespaces[namespace]).List(pass, metav1.ListOptions{Limit: 1, Continue: continuation})
		if err == nil {
			for i := range list.Items {
				if pass.Err() != nil {
					break
				}
				_ = a.ReconcileRepositoryStatus(pass, &list.Items[i], time.Now())
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
