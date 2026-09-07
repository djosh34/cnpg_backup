// Copyright 2026 cnpg_backup contributors. All rights reserved.
package cnpgi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

// Actual sweep plus API-shaped expiration responses: a consistent LIST snapshot
// expires before 200 objects can be visited at one per five seconds. Independent
// configuration diagnostics may resume on the replacement snapshot at its cursor.
func TestRepositorySweepProgressAcrossExpiredSnapshots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{repositories: "RepositoryList"})
		counts := map[string]int{"large": 200, "small": 3}
		visited, warned := map[string]int{}, map[string]int{}
		started := time.Now()
		expirations, temporaryErrors := 0, 0
		replaced := false
		object := func(ns string, index int) *unstructured.Unstructured {
			return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "backup.cnpg-backup.djosh34.github.io/v1alpha1", "kind": "Repository", "metadata": map[string]any{"name": fmt.Sprintf("repo-%03d", index), "namespace": ns}, "spec": map[string]any{}}}
		}
		client.PrependReactor("list", "repositories", func(action ktesting.Action) (bool, runtime.Object, error) {
			options := action.(interface{ GetListOptions() meta.ListOptions }).GetListOptions()
			if options.Limit != 1 {
				t.Error("sweep lost bounded page limit")
			}
			index := 0
			if options.Continue == "" {
				started = time.Now()
			} else {
				cursor := options.Continue
				if cursor[0] == 'r' {
					cursor = cursor[1:]
					replaced = true
				}
				var err error
				index, err = strconv.Atoi(cursor)
				if err != nil {
					t.Fatal(err)
				}
				if time.Since(started) >= 5*time.Minute {
					expirations++
					started = time.Now()
					err := apierrors.NewResourceExpired("test snapshot expired")
					err.ErrStatus.Continue = "r" + strconv.Itoa(index)
					return true, nil, fmt.Errorf("wrapped API error: %w", err)
				}
				if index == 100 && temporaryErrors == 0 {
					temporaryErrors++
					return true, nil, apierrors.NewServiceUnavailable("transient test fault")
				}
			}
			list := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{*object(action.GetNamespace(), index)}}
			if index+1 < counts[action.GetNamespace()] {
				list.SetContinue(strconv.Itoa(index + 1))
			}
			return true, list, nil
		})
		client.PrependReactor("get", "repositories", func(action ktesting.Action) (bool, runtime.Object, error) {
			name := action.(ktesting.GetAction).GetName()
			index, _ := strconv.Atoi(name[len("repo-"):])
			return true, object(action.GetNamespace(), index), nil
		})
		client.PrependReactor("patch", "repositories", func(action ktesting.Action) (bool, runtime.Object, error) {
			a := action.(ktesting.PatchAction)
			var patch struct {
				Status repositoryStatus `json:"status"`
			}
			if err := json.Unmarshal(a.GetPatch(), &patch); err != nil {
				t.Fatal(err)
			}
			// Missing credentials/configuration are invalid diagnostics, never fabricated
			// storage health. Every tail resource must actually receive this status.
			invalid := false
			for _, c := range patch.Status.Conditions {
				if c.Type == "Invalid" && c.Status == meta.ConditionTrue {
					invalid = true
				}
			}
			if !invalid {
				t.Error("missing invalid configuration diagnostic")
			}
			visited[action.GetNamespace()+"/"+a.GetName()]++
			return true, object(action.GetNamespace(), 0), nil
		})
		client.PrependReactor("create", "events", func(action ktesting.Action) (bool, runtime.Object, error) {
			o := action.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured)
			name, _, _ := unstructured.NestedString(o.Object, "involvedObject", "name")
			warned[action.GetNamespace()+"/"+name]++
			return true, o, nil
		})
		api := &API{Client: client, Namespaces: []string{"large", "small"}}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); api.RunRepositoryStatus(ctx) }()
		time.Sleep(46 * time.Minute)
		cancel()
		<-done
		t.Logf("expired snapshots=%d, transient failures=%d, status/Warning recipients=%d/%d", expirations, temporaryErrors, len(visited), len(warned))
		if expirations == 0 || temporaryErrors != 1 || !replaced {
			t.Errorf("fault controls not exercised: expired=%d temporary=%d replacement=%v", expirations, temporaryErrors, replaced)
		}
		for ns, n := range counts {
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("%s/repo-%03d", ns, i)
				if visited[key] < 2 || warned[key] < 2 {
					t.Fatalf("starved status/Warning after 46 virtual minutes: %s visits=%d warnings=%d total=%d", key, visited[key], warned[key], len(visited))
				}
			}
		}
	})
}

func TestRepositorySweepErrorsWithoutReplacementYieldNamespace(t *testing.T) {
	for name, failure := range map[string]error{
		"expired-no-token": apierrors.NewResourceExpired("test expiration without replacement"),
		"transport":        errors.New("test transport failure"),
		"forbidden":        apierrors.NewForbidden(repositories.GroupResource(), "", errors.New("test denial")),
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{repositories: "RepositoryList"})
				var namespaces []string
				client.PrependReactor("list", "repositories", func(action ktesting.Action) (bool, runtime.Object, error) {
					namespaces = append(namespaces, action.GetNamespace())
					return true, nil, failure
				})
				api := &API{Client: client, Namespaces: []string{"first", "second"}}
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				go func() { defer close(done); api.RunRepositoryStatus(ctx) }()
				time.Sleep(11 * time.Second)
				cancel()
				<-done
				if len(namespaces) != 3 || namespaces[0] != "first" || namespaces[1] != "second" || namespaces[2] != "first" {
					t.Fatal("error caused busy loop or blocked another namespace", namespaces)
				}
				for _, action := range client.Actions() {
					if action.GetVerb() != "list" {
						t.Fatal("uncertain page caused status/Event write")
					}
				}
			})
		})
	}
}
