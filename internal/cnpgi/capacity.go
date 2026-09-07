// Copyright 2026 cnpg_backup contributors. All rights reserved.
package cnpgi

import (
	"encoding/json"
	"errors"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Placement records declared upper limits, not an assertion that the PVC
// provisioner actually enforces them. CheckCapacity verifies the mounted kernel
// filesystems at the operation boundary. Exact restore phase allocations belong
// to the selected native plan, never to a guessed compression ratio.
func placementCapacity(c Cluster, s configuration.Spec) ([]configuration.FilesystemBudget, error) {
	result := []configuration.FilesystemBudget{}
	add := func(path, size string) error {
		q, e := resource.ParseQuantity(size)
		if e != nil || q.Value() < configuration.GiB || q.Value() > 8192*configuration.GiB {
			return errors.New("each target requires an explicit supported finite capacity")
		}
		result = append(result, configuration.FilesystemBudget{Mount: path, LimitBytes: q.Value()})
		return nil
	}
	if e := add("/cnpg-backup/work", s.Workspace.Size); e != nil {
		return nil, e
	}
	if e := add("/var/lib/postgresql/data", c.Spec.Storage.Size); e != nil {
		return nil, e
	}
	if len(c.Spec.WALStorage) > 0 && string(c.Spec.WALStorage) != "null" {
		var wal struct {
			Size string `json:"size"`
		}
		if json.Unmarshal(c.Spec.WALStorage, &wal) != nil {
			return nil, errors.New("invalid WAL storage")
		}
		if e := add("/var/lib/postgresql/wal", wal.Size); e != nil {
			return nil, e
		}
	}
	for _, t := range c.Spec.Tablespaces {
		if e := add("/var/lib/postgresql/tablespaces/"+t.Name, t.Storage.Size); e != nil {
			return nil, e
		}
	}
	return result, nil
}

// CheckMountedCapacity is executable in a live sidecar without contacting PG or
// running a native writer. Probe/startup deliberately do not run a phase-space
// reservation: that would deadlock bootstrap or make WAL depend on backup space.
func CheckMountedCapacity() error {
	budgets, err := configuration.LoadCapacity("/cnpg-backup/projection")
	if err != nil {
		return err
	}
	return configuration.CheckCapacity(budgets)
}
