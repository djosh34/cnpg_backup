// Copyright 2026 cnpg_backup contributors. All rights reserved.
// Package recoveryguard owns the local target fence, not repository admission.
package recoveryguard

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

const (
	PluginName = "cnpg-backup.djosh34.github.io"
	SocketPath = "/plugins/" + PluginName
	HelperPath = "/cnpg-backup/bin/cnpg-backup"
	ConfigPath = "/cnpg-backup/config/guard.json"
	maxMessage = 32 << 10
)

// Config must come from an owned, read-only lifecycle projection. PVCUID is
// resolved by an uncached API GET before injection, never inferred from a name.
// The entire set is immutable and exclusively assigned to one Cluster.
// This package cannot establish those Kubernetes ownership prerequisites.
type Config struct {
	ClusterUID   string   `json:"clusterUID"`
	OperationUID string   `json:"operationUID"`
	Targets      []Target `json:"targets"`
}
type Target struct {
	PVCUID string `json:"pvcUID"`
	Mount  string `json:"mount"`
}
type Owner struct {
	ClusterUID   string `json:"clusterUID"`
	OperationUID string `json:"operationUID"`
	PodUID       string `json:"podUID"`
	GuardUID     string `json:"guardUID"`
}
type Tuple struct {
	Owner      Owner  `json:"owner"`
	SidecarUID string `json:"sidecarUID"`
}

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var tablespacePattern = regexp.MustCompile(`^/var/lib/postgresql/tablespaces/[a-zA-Z_][a-zA-Z0-9_$]{0,62}$`)

// Validate checks tuple syntax only, never establishes or adopts ownership.
func (t Tuple) Validate() error {
	for _, id := range []string{t.Owner.ClusterUID, t.Owner.OperationUID, t.Owner.PodUID, t.Owner.GuardUID, t.SidecarUID} {
		if !uuidPattern.MatchString(id) {
			return errors.New("invalid recovery tuple")
		}
	}
	return nil
}

func NewUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("random identity unavailable")
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
func (c Config) Validate() error {
	if !uuidPattern.MatchString(c.ClusterUID) || !uuidPattern.MatchString(c.OperationUID) || len(c.Targets) < 1 || len(c.Targets) > 66 {
		return errors.New("invalid recovery identity or target count")
	}
	uids, mounts := map[string]bool{}, map[string]bool{}
	for _, t := range c.Targets {
		if !uuidPattern.MatchString(t.PVCUID) || uids[t.PVCUID] || mounts[t.Mount] {
			return errors.New("duplicate or invalid target")
		}
		if t.Mount != "/var/lib/postgresql/data" && t.Mount != "/var/lib/postgresql/wal" && !tablespacePattern.MatchString(t.Mount) {
			return errors.New("unsupported target mount")
		}
		uids[t.PVCUID], mounts[t.Mount] = true, true
	}
	if !mounts["/var/lib/postgresql/data"] {
		return errors.New("PGDATA target missing")
	}
	return nil
}
func (c Config) owner(pod, guard string) (Owner, error) {
	if err := c.Validate(); err != nil {
		return Owner{}, err
	}
	if !uuidPattern.MatchString(pod) || !uuidPattern.MatchString(guard) {
		return Owner{}, errors.New("invalid Pod or guard identity")
	}
	return Owner{c.ClusterUID, c.OperationUID, pod, guard}, nil
}
func (c Config) sortedTargets() []Target {
	result := slices.Clone(c.Targets)
	slices.SortFunc(result, func(a, b Target) int { return strings.Compare(a.PVCUID, b.PVCUID) })
	return result
}
func Decode(data []byte, out any) error {
	if len(data) > maxMessage {
		return errors.New("local control document too large")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return errors.New("invalid local control document")
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing local control document")
	}
	return nil
}
func LoadConfig(path string) (Config, error) {
	var c Config
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxMessage+1))
	if err != nil {
		return c, err
	}
	if err = Decode(b, &c); err != nil {
		return c, err
	}
	return c, c.Validate()
}

// WrapArgv admits only the exact v1.30 recovery generator's argv. In particular,
// no configurable executable, shell, alternate bootstrap or extra flag is run.
func WrapArgv(command, args []string, separateWAL bool) ([]string, error) {
	original := append(slices.Clone(command), args...)
	expected := []string{"/controller/manager", "instance", "restore"}
	if separateWAL {
		expected = append(expected, "--pg-wal", "/var/lib/postgresql/wal/pg_wal")
	}
	if len(original) < len(expected) || !slices.Equal(original[:len(expected)], expected) {
		return nil, errors.New("unexpected CNPG recovery argv")
	}
	// jobs.go also calls addManagerLoggingOptions; machinery v0.5.0 emits
	// these optional flags in this exact order. Preserve their original bytes.
	remaining := original[len(expected):]
	for _, flag := range []string{"--log-level=", "--log-field-level=", "--log-field-timestamp="} {
		if len(remaining) == 0 || !strings.HasPrefix(remaining[0], flag) {
			continue
		}
		value := strings.TrimPrefix(remaining[0], flag)
		if flag == "--log-level=" {
			if !slices.Contains([]string{"error", "warning", "info", "debug", "trace"}, value) {
				return nil, errors.New("unexpected CNPG log level")
			}
		} else if !regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_.-]{0,63}$`).MatchString(value) {
			return nil, errors.New("unsupported CNPG log field")
		}
		remaining = remaining[1:]
	}
	if len(remaining) != 0 {
		return nil, errors.New("unexpected CNPG recovery flags")
	}
	return append([]string{HelperPath, "recovery-guard", "--"}, original...), nil
}
func (c Config) ValidateArgv(argv []string) error {
	wal := slices.ContainsFunc(c.Targets, func(t Target) bool { return t.Mount == "/var/lib/postgresql/wal" })
	_, err := WrapArgv(argv, nil, wal)
	return err
}

// InstallHelper publishes the exact running executable before socket readiness.
// No executable or state document is placed in /plugins.
func InstallHelper(source, destination string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	dir := filepath.Dir(destination)
	f, err := os.CreateTemp(dir, ".helper-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	n, err := io.Copy(f, io.LimitReader(in, (128<<20)+1))
	if err != nil {
		return err
	}
	if n > 128<<20 {
		return errors.New("helper exceeds executable size limit")
	}
	if err = f.Chmod(0555); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), destination); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
