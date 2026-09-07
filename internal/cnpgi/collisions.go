// Copyright 2026 cnpg_backup contributors. All rights reserved.
package cnpgi

import (
	"errors"
	core "k8s.io/api/core/v1"
)

func uniquePlacementNames(spec *core.PodSpec) error {
	names := map[string]bool{}
	for _, v := range spec.Volumes {
		if names[v.Name] {
			return errors.New("duplicate volume name")
		}
		names[v.Name] = true
	}
	names = map[string]bool{}
	containers := append(append([]core.Container(nil), spec.Containers...), spec.InitContainers...)
	for _, c := range containers {
		if names[c.Name] {
			return errors.New("duplicate container name")
		}
		names[c.Name] = true
		paths := map[string]bool{}
		for _, m := range c.VolumeMounts {
			if paths[m.MountPath] {
				return errors.New("duplicate mount path")
			}
			paths[m.MountPath] = true
		}
	}
	return nil
}
