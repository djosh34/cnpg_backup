// Copyright 2026 cnpg_backup contributors. All rights reserved.
package cnpgi

import (
	"context"
	"sync"
	"time"

	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

var backups = schema.GroupVersionResource{Group: "postgresql.cnpg.io", Version: "v1", Resource: "backups"}

type backupObservation struct {
	Object               *unstructured.Unstructured
	SeenFailure, Pending bool
}

type backupObserver struct {
	api      *API
	metrics  *backupMetrics
	mu       sync.Mutex
	observed map[types.UID]backupObservation
	queue    workqueue.TypedRateLimitingInterface[types.UID]
}

func newBackupObserver(a *API, m *backupMetrics) *backupObserver {
	return &backupObserver{api: a, metrics: m, observed: map[types.UID]backupObservation{}, queue: workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[types.UID]())}
}
func backupField(b *unstructured.Unstructured, path ...string) string {
	s, _, _ := unstructured.NestedString(b.Object, path...)
	return s
}
func backupType(b *unstructured.Unstructured) string {
	if backupField(b, "spec", "method") != "plugin" || backupField(b, "spec", "pluginConfiguration", "name") != recoveryguard.PluginName {
		return ""
	}
	kind := backupField(b, "spec", "pluginConfiguration", "parameters", "backupType")
	if kind != "full" && kind != "differential" {
		return ""
	}
	return kind
}

// Transform strips error text, annotations, managed fields and unrelated status
// before caching. Informer resourceVersion/UID semantics remain intact.
func compactBackup(obj any) (any, error) {
	b, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return obj, nil
	}
	out := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "postgresql.cnpg.io/v1", "kind": "Backup"}}
	out.SetName(b.GetName())
	out.SetNamespace(b.GetNamespace())
	out.SetUID(b.GetUID())
	out.SetResourceVersion(b.GetResourceVersion())
	for _, path := range [][]string{{"spec", "method"}, {"spec", "cluster", "name"}, {"spec", "pluginConfiguration", "name"}, {"spec", "pluginConfiguration", "parameters", "backupType"}, {"status", "phase"}} {
		_ = unstructured.SetNestedField(out.Object, backupField(b, path...), path...)
	}
	return out, nil
}
func (o *backupObserver) observe(obj any, initial bool) {
	b, ok := obj.(*unstructured.Unstructured)
	if !ok || b.GetUID() == "" {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	state := o.observed[b.GetUID()]
	// Freeze the terminal observation: later resync/spec changes cannot rewrite
	// its requested type or create an empty/invalid metric label while queued.
	if !state.SeenFailure {
		state.Object = b
	}
	failed := backupField(b, "status", "phase") == "failed"
	if failed && !state.SeenFailure {
		state.SeenFailure = true
		if !initial && backupType(b) != "" {
			state.Pending = true
			o.queue.Add(b.GetUID())
		}
	}
	o.observed[b.GetUID()] = state
}
func (o *backupObserver) deleted(obj any) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	b, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	o.mu.Lock()
	delete(o.observed, b.GetUID())
	o.mu.Unlock()
	o.queue.Forget(b.GetUID())
}
func (o *backupObserver) handlers() cache.ResourceEventHandlerDetailedFuncs {
	return cache.ResourceEventHandlerDetailedFuncs{AddFunc: o.observe, UpdateFunc: func(_, next any) { o.observe(next, false) }, DeleteFunc: o.deleted}
}
func (o *backupObserver) run(ctx context.Context) {
	defer o.queue.ShutDown()
	for _, namespace := range o.api.Namespaces {
		client := o.api.Client.Resource(backups).Namespace(namespace)
		_, controller := cache.NewInformerWithOptions(cache.InformerOptions{
			ListerWatcher: cache.ToListWatcherWithWatchListSemantics(&cache.ListWatch{
				ListWithContextFunc: func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
					return client.List(ctx, opts)
				},
				WatchFuncWithContext: func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
					return client.Watch(ctx, opts)
				},
			}, o.api.Client), ObjectType: &unstructured.Unstructured{}, Handler: o.handlers(), Transform: compactBackup,
		})
		go controller.RunWithContext(ctx)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for o.next(ctx) {
		}
	}()
	<-ctx.Done()
	o.queue.ShutDown()
	<-done
}
func (o *backupObserver) next(ctx context.Context) bool {
	uid, shutdown := o.queue.Get()
	if shutdown {
		return false
	}
	defer o.queue.Done(uid)
	pass, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := o.process(pass, uid, time.Now()); err != nil && ctx.Err() == nil {
		o.queue.AddRateLimited(uid)
	} else {
		o.queue.Forget(uid)
	}
	return true
}
func (o *backupObserver) process(ctx context.Context, uid types.UID, now time.Time) error {
	o.mu.Lock()
	state, ok := o.observed[uid]
	o.mu.Unlock()
	if !ok || !state.Pending {
		return nil
	}
	b := state.Object
	cluster, err := o.api.Get(ctx, clusters, b.GetNamespace(), backupField(b, "spec", "cluster", "name"))
	if err != nil {
		return err
	}
	labels, spec, err := o.api.backupConfiguration(ctx, cluster)
	if err != nil {
		return err
	}
	labels.Type = backupType(b)
	o.mu.Lock()
	state, ok = o.observed[uid]
	if !ok || !state.Pending {
		o.mu.Unlock()
		return nil
	}
	state.Pending = false
	o.observed[uid] = state
	o.mu.Unlock()
	o.metrics.configure(labels, spec.BackupFreshness)
	if o.metrics.failure(labels, now) {
		o.warning(ctx, b, labels.Type, now)
	}
	return nil
}
func (o *backupObserver) warning(ctx context.Context, b *unstructured.Unstructured, kind string, now time.Time) {
	// Constant redacted diagnostics: CNPG owns detailed Backup status. Never copy
	// status.error, endpoint, key, credential or native subprocess output here.
	event := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Event", "metadata": map[string]any{"generateName": "cnpg-backup-failed-", "namespace": b.GetNamespace()},
		"involvedObject": map[string]any{"apiVersion": "postgresql.cnpg.io/v1", "kind": "Backup", "name": b.GetName(), "namespace": b.GetNamespace(), "uid": string(b.GetUID())},
		"type":           "Warning", "reason": "BackupFailed", "message": "Requested " + kind + " backup invocation failed; inspect CNPG Backup status. Durable commit history is reported independently.",
		"source": map[string]any{"component": "cnpg-backup"}, "firstTimestamp": now.UTC().Format(time.RFC3339), "lastTimestamp": now.UTC().Format(time.RFC3339), "count": int64(1),
	}}
	_, _ = o.api.Client.Resource(coreResource("events")).Namespace(b.GetNamespace()).Create(ctx, event, metav1.CreateOptions{})
}
