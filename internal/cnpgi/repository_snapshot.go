// Copyright 2026 cnpg_backup contributors. All rights reserved.
package cnpgi

import (
	"context"
	"encoding/base64"
	"errors"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// repositorySnapshot reads each Secret once so keys in the same Secret cannot
// come from different rotations. API.Get enforces the namespace and name allowlists.
func (a *API) repositorySnapshot(ctx context.Context, namespace string, spec configuration.Spec) (*configuration.Snapshot, error) {
	snap := &configuration.Snapshot{Spec: spec}
	secrets := map[string]*unstructured.Unstructured{}
	for _, item := range []struct {
		selector *configuration.Selector
		out      *[]byte
	}{
		{&spec.S3.AccessKeySecret, &snap.AccessKey}, {&spec.S3.SecretKeySecret, &snap.SecretKey}, {spec.S3.SessionTokenSecret, &snap.SessionToken},
	} {
		if item.selector == nil {
			continue
		}
		object := secrets[item.selector.Name]
		if object == nil {
			var err error
			object, err = a.Get(ctx, coreResource("secrets"), namespace, item.selector.Name)
			if err != nil {
				return nil, errors.New("repository credentials unavailable")
			}
			secrets[item.selector.Name] = object
		}
		encoded, _, _ := unstructured.NestedString(object.Object, "data", item.selector.Key)
		if len(encoded) > 90<<10 {
			return nil, errors.New("invalid repository credentials")
		}
		data, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil || len(data) == 0 || len(data) > 64<<10 {
			return nil, errors.New("invalid repository credentials")
		}
		*item.out = data
	}
	if selector := spec.S3.CAConfigMap; selector != nil {
		object, err := a.Get(ctx, coreResource("configmaps"), namespace, selector.Name)
		if err != nil {
			return nil, errors.New("repository trust unavailable")
		}
		data, _, _ := unstructured.NestedString(object.Object, "data", selector.Key)
		snap.CA = []byte(data)
		if len(snap.CA) > 256<<10 {
			return nil, errors.New("invalid repository trust")
		}
		if _, err = configuration.CAPool(snap.CA, false); err != nil {
			return nil, err
		}
	}
	return snap, nil
}
