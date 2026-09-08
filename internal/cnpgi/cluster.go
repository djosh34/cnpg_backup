// Copyright 2026 cnpg_backup contributors. All rights reserved.
package cnpgi

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	"github.com/djosh34/cnpg_backup/internal/repository"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const DatabaseDigest = "sha256:de8dc8b70c7f26b6d353cc64b67d38037b9a94592ba8b789ca4d6f44762d6fa4"

type Plugin struct {
	Name          string            `json:"name"`
	IsWALArchiver bool              `json:"isWALArchiver"`
	Parameters    map[string]string `json:"parameters"`
}
type Cluster struct {
	meta.TypeMeta `json:",inline"`
	Metadata      meta.ObjectMeta `json:"metadata"`
	Spec          struct {
		ImageName string `json:"imageName"`
		Storage   struct {
			Size string `json:"size"`
		} `json:"storage"`
		PostgreSQL struct {
			Parameters map[string]string `json:"parameters"`
		} `json:"postgresql"`
		PostgresUID int64           `json:"postgresUID"`
		PostgresGID int64           `json:"postgresGID"`
		Plugins     []Plugin        `json:"plugins"`
		Replica     json.RawMessage `json:"replica"`
		WALStorage  json.RawMessage `json:"walStorage"`
		Tablespaces []struct {
			Name    string `json:"name"`
			Storage struct {
				Size string `json:"size"`
			} `json:"storage"`
		} `json:"tablespaces"`
		Bootstrap struct {
			Recovery *struct {
				Source         string         `json:"source"`
				RecoveryTarget map[string]any `json:"recoveryTarget"`
			} `json:"recovery,omitempty"`
		} `json:"bootstrap"`
		ExternalClusters []struct {
			Name   string  `json:"name"`
			Plugin *Plugin `json:"plugin"`
		} `json:"externalClusters"`
		Certificates struct {
			ReplicationTLSSecret string `json:"replicationTLSSecret"`
			ServerCASecret       string `json:"serverCASecret"`
			ClientCASecret       string `json:"clientCASecret"`
		} `json:"certificates"`
	} `json:"spec"`
}

func ParseCluster(data []byte) (Cluster, error) {
	var c Cluster
	if len(data) > 1<<20 || json.Unmarshal(data, &c) != nil || c.APIVersion != "postgresql.cnpg.io/v1" || c.Kind != "Cluster" || len(validation.IsDNS1123Subdomain(c.Metadata.Name)) != 0 || c.Metadata.Name == "" || c.Metadata.Namespace == "" {
		return c, errors.New("invalid CNPG Cluster definition")
	}
	if c.Spec.PostgresUID == 0 {
		c.Spec.PostgresUID = 26
	}
	if c.Spec.PostgresGID == 0 {
		c.Spec.PostgresGID = 26
	}
	return c, nil
}
func (c Cluster) Repositories() (destination, source string, err error) {
	fail := func() (string, string, error) { return "", "", errors.New("unsupported Cluster plugin configuration") }
	if c.Spec.PostgresUID <= 0 || c.Spec.PostgresGID <= 0 || len(c.Spec.Tablespaces) > 64 || (len(c.Spec.Replica) > 0 && string(c.Spec.Replica) != "null") {
		return fail()
	}
	if !strings.HasSuffix(c.Spec.ImageName, "@"+DatabaseDigest) {
		return "", "", errors.New("initial support requires the pinned PostgreSQL18.6 image digest")
	}
	seenTablespaces := map[string]bool{}
	for _, t := range c.Spec.Tablespaces {
		name := tablespaceVolume(t.Name)
		if !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]{0,62}$`).MatchString(t.Name) || seenTablespaces[name] {
			return "", "", errors.New("unsupported or colliding managed tablespace name")
		}
		seenTablespaces[name] = true
	}
	// Lifecycle does not require a running server. Explicit incompatible physical
	// settings are rejected now; native preflight must still query actual settings
	// (including defaulted values) before a capture can run.
	p := c.Spec.PostgreSQL.Parameters
	if (p["wal_level"] != "" && p["wal_level"] != "replica" && p["wal_level"] != "logical") || (p["full_page_writes"] != "" && p["full_page_writes"] != "on") || (p["summarize_wal"] != "" && p["summarize_wal"] != "on") {
		return "", "", errors.New("incompatible PostgreSQL WAL settings")
	}
	for _, p := range c.Spec.Plugins {
		if p.Name != recoveryguard.PluginName {
			if p.IsWALArchiver {
				return fail()
			}
			continue
		}
		if destination != "" || len(p.Parameters) != 1 || p.Parameters["repository"] == "" {
			return fail()
		}
		destination = p.Parameters["repository"]
	}
	if destination == "" {
		return fail()
	}
	if recovery := c.Spec.Bootstrap.Recovery; recovery != nil {
		for _, external := range c.Spec.ExternalClusters {
			if external.Name == recovery.Source && external.Plugin != nil && external.Plugin.Name == recoveryguard.PluginName {
				if source != "" || len(external.Plugin.Parameters) != 1 {
					return fail()
				}
				source = external.Plugin.Parameters["repository"]
			}
		}
		if source == "" || source == destination {
			return fail()
		}
		if _, e := recoveryTarget(recovery.RecoveryTarget); e != nil {
			return fail()
		}
	}
	for _, name := range []string{destination, source} {
		if name != "" && len(validation.IsDNS1123Subdomain(name)) != 0 {
			return fail()
		}
	}
	if c.Metadata.Annotations["cnpg.io/skipEmptyWalArchiveCheck"] != "" {
		return fail()
	}
	return destination, source, nil
}
func tablespaceVolume(name string) string {
	if strings.HasPrefix(name, "_") {
		name = "1" + name[1:]
	}
	return "tbs-" + strings.ToLower(strings.NewReplacer("_", "-", "$", "-").Replace(name))
}

func (c Cluster) BootstrapFingerprint() string {
	bootstrap, _ := json.Marshal(c.Spec.Bootstrap)
	hash := sha256.Sum256(bootstrap)
	return fmt.Sprintf("%x", hash)
}
func (c Cluster) OperationUID() string {
	id, _ := repository.RestoreOperationID(string(c.Metadata.UID), c.BootstrapFingerprint())
	return id
}
func ValidateBackup(target string, parameters map[string]string) error {
	if target != "primary" || len(parameters) != 1 || !slices.Contains([]string{"full", "differential"}, parameters["backupType"]) {
		return errors.New("plugin backups require target primary and explicit backupType full or differential")
	}
	return nil
}
