// Copyright 2026 cnpg_backup contributors. All rights reserved.
package cnpgi

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	"github.com/djosh34/cnpg_backup/internal/repository"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var errBackupUnconfigured = errors.New("backup Repository not configured")

func backupRepositoryName(c *unstructured.Unstructured) (string, error) {
	plugins, _, _ := unstructured.NestedSlice(c.Object, "spec", "plugins")
	name := ""
	found := false
	for _, p := range plugins {
		value, ok := p.(map[string]any)
		if !ok {
			continue
		}
		if value["name"] == recoveryguard.PluginName {
			if found {
				return "", errors.New("duplicate backup plugin")
			}
			found = true
			name, _, _ = unstructured.NestedString(value, "parameters", "repository")
		}
	}
	if name == "" {
		return "", errBackupUnconfigured
	}
	return name, nil
}

func (a *API) backupConfiguration(ctx context.Context, c *unstructured.Unstructured) (backupLabels, configuration.Spec, error) {
	var labels backupLabels
	var spec configuration.Spec
	name, err := backupRepositoryName(c)
	if err != nil {
		return labels, spec, err
	}
	object, err := a.Get(ctx, repositories, c.GetNamespace(), name)
	if err != nil {
		return labels, spec, err
	}
	b, err := json.Marshal(object.Object["spec"])
	if err != nil {
		return labels, spec, err
	}
	spec, err = configuration.DecodeSpec(b)
	if err != nil {
		return labels, spec, err
	}
	return backupLabels{Repository: spec.RepositoryID, Namespace: c.GetNamespace(), Cluster: c.GetName()}, spec, nil
}

func (a *API) readBackupHistory(ctx context.Context, namespace, writerUID string, spec configuration.Spec) (repository.BackupHistory, error) {
	snap, err := a.repositorySnapshot(ctx, namespace, spec)
	if err != nil {
		return repository.BackupHistory{}, err
	}
	store, err := snap.Store()
	if err != nil {
		return repository.BackupHistory{}, err
	}
	defer store.Close()
	return repository.ReadBackupHistory(ctx, store, spec.RepositoryID, writerUID)
}

type backupHistoryReader func(context.Context, string, string, configuration.Spec) (repository.BackupHistory, error)

func (a *API) reconcileBackupHistory(ctx context.Context, m *backupMetrics, c *unstructured.Unstructured, read backupHistoryReader) (backupLabels, error) {
	labels, spec, err := a.backupConfiguration(ctx, c)
	if err != nil {
		return labels, err
	}
	m.configure(labels, spec.BackupFreshness)
	h, err := read(ctx, c.GetNamespace(), string(c.GetUID()), spec)
	m.history(labels, h, err, time.Now())
	// A storage failure still leaves the configured series present and unknown.
	return labels, nil
}

// runBackupHistory reads one Cluster per tick, separately from metric scrapes.
func (a *API) runBackupHistory(ctx context.Context, m *backupMetrics) {
	if len(a.Namespaces) == 0 {
		return
	}
	namespace, continuation := 0, ""
	seen := map[backupLabels]bool{}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		pass, cancel := context.WithTimeout(ctx, 60*time.Second)
		ns := a.Namespaces[namespace]
		list, err := a.Client.Resource(clusters).Namespace(ns).List(pass, metav1.ListOptions{Limit: 1, Continue: continuation})
		if err == nil {
			for i := range list.Items {
				labels, e := a.reconcileBackupHistory(pass, m, &list.Items[i], a.readBackupHistory)
				if e == nil {
					seen[labels] = true
				} else if !errors.Is(e, errBackupUnconfigured) {
					// Keep counters on read failure, but mark cached success unknown.
					m.mu.Lock()
					for key, s := range m.series {
						if key.Namespace == ns && key.Cluster == list.Items[i].GetName() {
							s.Known = false
							m.series[key] = s
							key.Type = ""
							seen[key] = true
						}
					}
					m.mu.Unlock()
				}
			}
			continuation = list.GetContinue()
			if continuation == "" {
				m.mu.Lock()
				for labels := range m.series {
					base := labels
					base.Type = ""
					if labels.Namespace == ns && !seen[base] {
						delete(m.series, labels)
					}
				}
				m.mu.Unlock()
			}
		} else {
			continuation = ""
		}
		cancel()
		if continuation == "" {
			namespace = (namespace + 1) % len(a.Namespaces)
			seen = map[backupLabels]bool{}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
