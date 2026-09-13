// Copyright 2026 cnpg_backup contributors. All rights reserved.
package cnpgi

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

var repositories = schema.GroupVersionResource{Group: configuration.Group, Version: configuration.Version, Resource: "repositories"}
var clusters = schema.GroupVersionResource{Group: "postgresql.cnpg.io", Version: "v1", Resource: "clusters"}

func coreResource(name string) schema.GroupVersionResource {
	return schema.GroupVersionResource{Version: "v1", Resource: name}
}

// API restricts Kubernetes reads to configured namespaces and Secret names.
// Install-time RBAC enforces the same allowlists.
type API struct {
	OperatorNamespace string
	Client            dynamic.Interface
	recoveryWatch     dynamic.Interface // context-owned stream, no HTTP total timeout
	Namespaces        []string
	SecretNames       map[string][]string
	recoveryMu        sync.Mutex
	recovery          *recoveryCoordinator
	recoveryContext   context.Context
	// Sole storage I/O seam for manager source lifetime operations.
	recoveryLifetime func(context.Context, Cluster, configuration.Spec, bool) error
}

func (a *API) Get(ctx context.Context, gvr schema.GroupVersionResource, namespace, name string) (*unstructured.Unstructured, error) {
	if !slices.Contains(a.Namespaces, namespace) || name == "" {
		return nil, errors.New("namespace/reference is not allowlisted")
	}
	if gvr == coreResource("secrets") && !slices.Contains(a.SecretNames[namespace], name) {
		return nil, errors.New("Secret name is not allowlisted")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return a.Client.Resource(gvr).Namespace(namespace).Get(ctx, name, meta.GetOptions{})
}
func (a *API) Repository(ctx context.Context, namespace, name string) (configuration.Spec, error) {
	object, err := a.Get(ctx, repositories, namespace, name)
	if err != nil {
		return configuration.Spec{}, errors.New("Repository unavailable")
	}
	data, err := json.Marshal(object.Object["spec"])
	if err != nil {
		return configuration.Spec{}, err
	}
	spec, err := configuration.DecodeSpec(data)
	if err != nil {
		return spec, err
	}
	_, err = a.repositorySnapshot(ctx, namespace, spec)
	return spec, err
}
func (a *API) ValidateRepositories(ctx context.Context, c Cluster) (configuration.Spec, *configuration.Spec, error) {
	destination, source, err := c.Repositories()
	if err != nil {
		return configuration.Spec{}, nil, err
	}
	dst, err := a.Repository(ctx, c.Metadata.Namespace, destination)
	if err != nil {
		return dst, nil, err
	}
	if source == "" {
		return dst, nil, nil
	}
	src, err := a.Repository(ctx, c.Metadata.Namespace, source)
	if err != nil {
		return dst, nil, err
	}
	if dst.RepositoryID == src.RepositoryID {
		return dst, nil, errors.New("restore destination must use a distinct repository lineage")
	}
	return dst, &src, nil
}
func (a *API) VerifyCluster(ctx context.Context, c Cluster) error {
	actual, err := a.Get(ctx, clusters, c.Metadata.Namespace, c.Metadata.Name)
	if err != nil {
		return err
	}
	if c.Metadata.UID == "" || actual.GetUID() != c.Metadata.UID {
		return errors.New("lifecycle Cluster identity mismatch")
	}
	return nil
}

// VerifyLivePod checks ownership without rebuilding an admitted Pod. Only
// CREATE and EVALUATE validate new placement against current configuration.
func (a *API) VerifyLivePod(ctx context.Context, c Cluster, object []byte) error {
	if err := a.VerifyCluster(ctx, c); err != nil {
		return err
	}
	var pod struct {
		meta.TypeMeta `json:",inline"`
		Metadata      meta.ObjectMeta `json:"metadata"`
	}
	if len(object) > 2<<20 || json.Unmarshal(object, &pod) != nil || pod.Kind != "Pod" || pod.APIVersion != "v1" || pod.Metadata.Namespace != c.Metadata.Namespace || pod.Metadata.UID == "" {
		return errors.New("invalid live Pod identity")
	}
	actual, err := a.Get(ctx, coreResource("pods"), c.Metadata.Namespace, pod.Metadata.Name)
	if err != nil {
		return errors.New("live Pod identity unavailable")
	}
	owner := meta.GetControllerOf(actual)
	if actual.GetUID() != pod.Metadata.UID || owner == nil || owner.UID != c.Metadata.UID {
		return errors.New("live Pod is not owned by this Cluster")
	}
	return nil
}

// EnsureProjection creates an immutable, Cluster-owned configuration snapshot.
// Existing data must match, including any target PVC identities.
func (a *API) EnsureProjection(ctx context.Context, c Cluster, name string, data map[string]string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap", "immutable": true,
		"metadata": map[string]any{"name": name, "namespace": c.Metadata.Namespace, "ownerReferences": []any{map[string]any{"apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster", "name": c.Metadata.Name, "uid": string(c.Metadata.UID), "controller": true}}},
	}}
	values := map[string]any{}
	for key, value := range data {
		values[key] = value
	}
	object.Object["data"] = values
	existing, err := a.Get(ctx, coreResource("configmaps"), c.Metadata.Namespace, name)
	if apierrors.IsNotFound(err) {
		_, err = a.Client.Resource(coreResource("configmaps")).Namespace(c.Metadata.Namespace).Create(ctx, object, meta.CreateOptions{})
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		existing, err = a.Get(ctx, coreResource("configmaps"), c.Metadata.Namespace, name)
	}
	if err != nil {
		return err
	}
	owner := meta.GetControllerOf(existing)
	immutable, _, _ := unstructured.NestedBool(existing.Object, "immutable")
	if owner == nil || owner.UID != c.Metadata.UID || !immutable || !reflect.DeepEqual(existing.Object["data"], values) {
		return errors.New("owned projection or immutable target set mismatch")
	}
	return nil
}
