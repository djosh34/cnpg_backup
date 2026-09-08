package cnpgi

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	"github.com/djosh34/cnpg_backup/internal/repository"
	"github.com/djosh34/cnpg_backup/internal/s3store"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const helperPlanPath = "/cnpg-backup/state/recovery.json"
const recoveryPlanLimit = 16 << 20
const operationAnnotation = "cnpg-backup.djosh34.github.io/recovery"

// RecoveryPlan is a credential-free exact selection, not a storage capability.
// The original process must still admit the tuple and positively check its hold.
type RecoveryPlan struct {
	Plan              repository.Plan              `json:"plan"`
	Tuple             recoveryguard.Tuple          `json:"tuple"`
	ClusterDefinition json.RawMessage              `json:"cluster_definition"`
	Bundled           map[string]s3store.Integrity `json:"bundled"`
	Materialized      bool                         `json:"materialized"`
}

// Recovery keeps only validated semantic fields needed by this plugin. Never
// copy arbitrary annotations, other plugins' parameters or PostgreSQL connection
// GUCs into a helper document: those may contain unrelated inline credentials.
func recoveryCluster(c Cluster) Cluster {
	c.Metadata = meta.ObjectMeta{Name: c.Metadata.Name, Namespace: c.Metadata.Namespace, UID: c.Metadata.UID}
	c.Spec.ImageName = "ghcr.io/cloudnative-pg/postgresql@" + DatabaseDigest
	plugins := c.Spec.Plugins
	c.Spec.Plugins = nil
	for _, p := range plugins {
		if p.Name == recoveryguard.PluginName {
			c.Spec.Plugins = append(c.Spec.Plugins, p)
		}
	}
	external := c.Spec.ExternalClusters
	c.Spec.ExternalClusters = nil
	for _, p := range external {
		if c.Spec.Bootstrap.Recovery != nil && p.Name == c.Spec.Bootstrap.Recovery.Source && p.Plugin != nil && p.Plugin.Name == recoveryguard.PluginName {
			c.Spec.ExternalClusters = append(c.Spec.ExternalClusters, p)
		}
	}
	c.Spec.PostgreSQL.Parameters = nil
	if len(c.Spec.WALStorage) > 0 && string(c.Spec.WALStorage) != "null" {
		c.Spec.WALStorage = json.RawMessage(`{}`)
	}
	c.Spec.Certificates.ReplicationTLSSecret = ""
	c.Spec.Certificates.ServerCASecret = ""
	c.Spec.Certificates.ClientCASecret = ""
	return c
}

type RecoveryPlacement struct {
	ClusterUID              string `json:"clusterUID"`
	Namespace               string `json:"namespace"`
	Cluster                 string `json:"cluster"`
	OperationID             string `json:"operationID"`
	BootstrapSHA256         string `json:"bootstrapSHA256"`
	Source                  string `json:"source"`
	Destination             string `json:"destination"`
	SourceConfigSHA256      string `json:"sourceConfigSHA256"`
	DestinationConfigSHA256 string `json:"destinationConfigSHA256"`
}

func recoveryTarget(fields map[string]any) (repository.Target, error) {
	t := repository.Target{Kind: "latest"}
	for k, v := range fields {
		switch k {
		case "exclusive":
			b, ok := v.(bool)
			if !ok {
				return t, repository.ErrInvalid
			}
			t.Exclusive = b
		case "backupID":
			s, ok := v.(string)
			if !ok || s == "" {
				return t, repository.ErrInvalid
			}
			t.BackupUID = &s
		case "targetTLI":
			s, ok := v.(string)
			if !ok {
				return t, repository.ErrInvalid
			}
			n, e := strconv.ParseUint(s, 10, 32)
			if e != nil || n == 0 || strconv.FormatUint(n, 10) != s {
				return t, repository.ErrInvalid
			}
			t.Timeline = uint32(n)
		case "targetTime", "targetLSN", "targetName", "targetXID":
			s, ok := v.(string)
			if !ok || s == "" || t.Kind != "latest" {
				return t, repository.ErrInvalid
			}
			t.Kind = map[string]string{"targetTime": "time", "targetLSN": "lsn", "targetName": "name", "targetXID": "xid"}[k]
			t.Value = s
		case "targetImmediate":
			b, ok := v.(bool)
			if !ok {
				return t, repository.ErrInvalid
			}
			if b {
				if t.Kind != "latest" {
					return t, repository.ErrInvalid
				}
				t.Kind = "immediate"
			}
		default:
			return t, repository.ErrInvalid
		}
	}
	validate := t
	if validate.Timeline == 0 {
		validate.Timeline = 1
	}
	if e := repository.ValidateTarget(validate); e != nil {
		return t, e
	}
	return t, nil
}
func (p RecoveryPlan) validate() error {
	if p.Plan.Validate() != nil {
		return repository.ErrInvalid
	}
	c, e := ParseCluster(p.ClusterDefinition)
	if e != nil {
		return e
	}
	if jsonText(recoveryCluster(c)) != string(p.ClusterDefinition) {
		return repository.ErrInvalid
	}
	dst, src, e := c.Repositories()
	if e != nil || dst == src || c.Spec.Bootstrap.Recovery == nil || string(c.Metadata.UID) != p.Plan.TargetClusterUID || c.OperationUID() != p.Plan.OperationID || c.BootstrapFingerprint() != p.Plan.BootstrapSHA256 {
		return repository.ErrIdentity
	}
	t, e := recoveryTarget(c.Spec.Bootstrap.Recovery.RecoveryTarget)
	if e != nil {
		return e
	}
	if t.Timeline == 0 {
		t.Timeline = p.Plan.Target.Timeline
	}
	if !reflect.DeepEqual(t, p.Plan.Target) {
		return repository.ErrIdentity
	}
	owner := p.Tuple.Owner
	if p.Tuple.Validate() != nil || owner.ClusterUID != p.Plan.TargetClusterUID || owner.OperationUID != p.Plan.OperationID {
		return repository.ErrIdentity
	}
	if len(p.Bundled) > 262144 {
		return repository.ErrCapacity
	}
	if p.Materialized && len(p.Bundled) == 0 {
		return repository.ErrCorrupt
	}
	for name, v := range p.Bundled {
		_, _, e := p.Plan.Coverage(name)
		hash, he := hex.DecodeString(v.SHA256)
		// Extra authenticated streamed files may be present, but only manifest
		// interval coverage can authorize local fallback in restoreSourceWAL.
		if e != nil || len(name) != 24 || v.Size != p.Plan.Source.WALSegmentBytes || he != nil || len(hash) != 32 || hex.EncodeToString(hash) != v.SHA256 {
			return repository.ErrCorrupt
		}
	}
	return nil
}
func saveRecovery(directory string, p RecoveryPlan) error {
	if e := p.validate(); e != nil {
		return e
	}
	b, e := json.Marshal(p)
	if e != nil || len(b) > recoveryPlanLimit {
		return repository.ErrCapacity
	}
	root, e := os.OpenRoot(directory)
	if e != nil {
		return e
	}
	defer root.Close()
	name := "recovery-" + repository.UUID() + ".json"
	f, e := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer root.Remove(name)
	if _, e = f.Write(b); e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e != nil {
		return e
	}
	if ce != nil {
		return ce
	}
	if e = root.Rename(name, "recovery.json"); e != nil {
		return e
	}
	d, e := root.Open(".")
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func readRecovery(directory string) (RecoveryPlan, error) {
	var p RecoveryPlan
	root, e := os.OpenRoot(directory)
	if e != nil {
		return p, e
	}
	defer root.Close()
	f, e := root.Open("recovery.json")
	if e != nil {
		return p, e
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil || !st.Mode().IsRegular() || st.Size() > recoveryPlanLimit {
		return p, repository.ErrInvalid
	}
	b, e := io.ReadAll(io.LimitReader(f, recoveryPlanLimit+1))
	if e != nil {
		return p, e
	}
	if e = configuration.StrictJSON(b, &p); e != nil {
		return p, e
	}
	return p, p.validate()
}
func recoveryDirectory(operation string) string {
	return "/var/lib/postgresql/data/.cnpg-backup/" + operation
}
func physicalWAL(c Cluster) string {
	if len(c.Spec.WALStorage) > 0 && string(c.Spec.WALStorage) != "null" {
		return "/var/lib/postgresql/wal/pg_wal"
	}
	return pgdataPath + "/pg_wal"
}

// PostgreSQL expands %p to pg_wal/RECOVERYXLOG (relative to PGDATA). No cwd,
// symlink or caller-supplied root is trusted to determine the physical target.
func recoveryDestination(c Cluster, name, destination string) (string, error) {
	if !repository.ValidWALFilename(name) {
		return "", repository.ErrInvalid
	}
	if strings.HasPrefix(destination, "pg_wal/") {
		destination = pgdataPath + "/" + destination
	}
	if e := validateWALPaths([]byte(jsonText(c)), destination, name, true); e != nil {
		return "", e
	}
	return filepath.Base(destination), nil
}
func restoreConfig(timeline uint32) string {
	return fmt.Sprintf("restore_command = 'exec /cnpg-backup/bin/cnpg-backup wal-fetch --plan /cnpg-backup/state/recovery.json -- \"%%f\" \"%%p\"'\nrecovery_target_action = 'promote'\nrecovery_target_timeline = '%d'\n", timeline)
}
func taskContext(ctx context.Context, task *recoveryguard.Task) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(task.Context, cancel)
	return ctx, func() { stop(); cancel() }
}

var errRecoveryClosed = errors.New("recovery is closed or ownership is uncertain")
