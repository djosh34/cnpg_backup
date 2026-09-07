// Copyright 2026 cnpg_backup contributors. All rights reserved.
package cnpgi

import (
	"context"
	"errors"
	"time"

	apps "k8s.io/api/apps/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const OperatorImage = "ghcr.io/cloudnative-pg/cloudnative-pg@sha256:a2701eb97cdd2a34b1fdb2cb51987f544b706e40bec72ae7146cd8580efefebb"

// VerifyOperator checks the actual installed Deployment and completed rollout,
// not a user-declared version string or the plugin's own protocol library. The
// initial supported operator artifact is deliberately pinned just like PG18.
func (a *API) VerifyOperator(ctx context.Context) error {
	if a.OperatorNamespace == "" {
		return errors.New("CNPG operator namespace is required")
	}
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	object, err := a.Client.Resource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}).Namespace(a.OperatorNamespace).Get(bounded, "cnpg-controller-manager", meta.GetOptions{})
	if err != nil {
		return errors.New("CNPG operator version unavailable")
	}
	var d apps.Deployment
	if runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, &d) != nil {
		return errors.New("invalid CNPG Deployment")
	}
	if d.Spec.Replicas == nil || *d.Spec.Replicas < 1 || d.Status.ObservedGeneration != d.Generation || d.Status.UpdatedReplicas != *d.Spec.Replicas || d.Status.AvailableReplicas != *d.Spec.Replicas || d.Status.Replicas != *d.Spec.Replicas || len(d.Spec.Template.Spec.Containers) != 1 || d.Spec.Template.Spec.Containers[0].Image != OperatorImage {
		return errors.New("require fully rolled out pinned CNPG v1.30.0 operator")
	}
	return nil
}
