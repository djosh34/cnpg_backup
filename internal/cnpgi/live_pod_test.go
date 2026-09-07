// Copyright 2026 cnpg_backup contributors. All rights reserved.
package cnpgi

import (
	"context"
	"testing"

	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestLiveHookDoesNotDependOnNewConfigurationOrSerializeSpec(t *testing.T) {
	s, c, p := lifecycleFixture(t)
	p, _ = hookPod(t, s, c, p, lifecycle.OperatorOperationType_TYPE_CREATE)
	p.UID = types.UID("77777777-7777-4777-8777-777777777777")
	p.OwnerReferences = []meta.OwnerReference{{APIVersion: c.APIVersion, Kind: c.Kind, Name: c.Metadata.Name, UID: c.Metadata.UID, Controller: ptr(true)}}
	ctx := context.Background()
	if _, err := s.API.Client.Resource(coreResource("pods")).Namespace("test").Create(ctx, unstruct(p), meta.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := s.API.Client.Resource(repositories).Namespace("test").Delete(ctx, "destination", meta.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	obj := unstruct(p)
	obj.Object["spec"].(map[string]any)["futureAdmissionField"] = map[string]any{"preserve": "opaque"}
	response, err := s.LifecycleHook(ctx, &lifecycle.OperatorLifecycleRequest{OperationType: &lifecycle.OperatorOperationType{Type: lifecycle.OperatorOperationType_TYPE_PATCH}, ClusterDefinition: raw(c), ObjectDefinition: raw(obj)})
	if err != nil || len(response.GetJsonPatch()) != 0 {
		t.Fatal("live hook depended on new configuration or serialized unknown fields", err)
	}
}

func TestLiveHookRejectsWrongResourceAndIdentity(t *testing.T) {
	for name, change := range map[string]func(*core.Pod){
		"missing-UID":  func(p *core.Pod) { p.UID = "" },
		"replaced-UID": func(p *core.Pod) { p.UID = "another-pod" },
		"namespace":    func(p *core.Pod) { p.Namespace = "other" },
		"resource":     func(p *core.Pod) { p.Kind = "Job"; p.APIVersion = "batch/v1" },
		"missing-Pod":  func(p *core.Pod) { p.Name = "missing" },
	} {
		t.Run(name, func(t *testing.T) {
			s, c, p := lifecycleFixture(t)
			p.UID = "77777777-7777-4777-8777-777777777777"
			p.OwnerReferences = []meta.OwnerReference{{APIVersion: c.APIVersion, Kind: c.Kind, Name: c.Metadata.Name, UID: c.Metadata.UID, Controller: ptr(true)}}
			if _, err := s.API.Client.Resource(coreResource("pods")).Namespace("test").Create(context.Background(), unstruct(p), meta.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			change(&p)
			_, err := s.LifecycleHook(context.Background(), &lifecycle.OperatorLifecycleRequest{OperationType: &lifecycle.OperatorOperationType{Type: lifecycle.OperatorOperationType_TYPE_UPDATE}, ClusterDefinition: raw(c), ObjectDefinition: raw(p)})
			if err == nil {
				t.Fatal("invalid live identity accepted")
			}
		})
	}
}
