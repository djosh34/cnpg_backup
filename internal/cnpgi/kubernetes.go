// Copyright 2026 cnpg_backup contributors. All rights reserved.
package cnpgi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
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

// API is an uncached client, not an informer or cluster-wide Secret cache.
// Secret names are independently constrained by install-time RBAC and this list.
type API struct {
	OperatorNamespace string
	Client            dynamic.Interface
	Namespaces        []string
	SecretNames       map[string][]string
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
	for _, selector := range []*configuration.Selector{&spec.S3.AccessKeySecret, &spec.S3.SecretKeySecret, spec.S3.SessionTokenSecret} {
		if selector == nil {
			continue
		}
		secret, err := a.Get(ctx, coreResource("secrets"), namespace, selector.Name)
		if err != nil {
			return spec, errors.New("referenced credential unavailable")
		}
		encoded, ok, _ := unstructured.NestedString(secret.Object, "data", selector.Key)
		value, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil || !ok || len(value) == 0 || len(value) > 64<<10 {
			return spec, errors.New("referenced credential key invalid")
		}
	}
	if selector := spec.S3.CAConfigMap; selector != nil {
		ca, err := a.Get(ctx, coreResource("configmaps"), namespace, selector.Name)
		if err != nil {
			return spec, errors.New("referenced CA unavailable")
		}
		data, ok, _ := unstructured.NestedString(ca.Object, "data", selector.Key)
		if !ok || len(data) > 256<<10 {
			return spec, errors.New("invalid CA projection")
		}
		if _, err := configuration.CAPool([]byte(data), false); err != nil {
			return spec, err
		}
	}
	return spec, nil
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

// VerifyLivePod binds a no-mutation hook to an existing Cluster-owned Pod.
// It deliberately does not revalidate today's Repository or regenerate yesterday's
// spec: this path grants no new projections or data access. CREATE/EVALUATE own
// placement/security validation and CNPG decides when to replace the old Pod.
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

// EnsureProjection never trusts editable ConfigMap data as configuration. It
// compares against current validated Repository specs and an exact Cluster owner.
// Immutable snapshots preserve Job references; ownership-set bindings reject
// replacement PVC UIDs rather than silently defining a new target set.
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
