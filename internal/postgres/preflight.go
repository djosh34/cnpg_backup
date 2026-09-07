// Copyright 2026 cnpg_backup contributors. All rights reserved.
// Package postgres owns fixed, bounded native PostgreSQL metadata operations.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
)

const tools = "/usr/lib/postgresql/18/bin/"

// Connection is a nonsecret lifecycle projection. hostaddr is never configurable:
// every libpq connection must stay on this Pod while verifying the Service SAN.
type Connection struct {
	Host        string            `json:"host"`
	Tablespaces map[string]string `json:"tablespaces"`
}

type serverState struct {
	Version        int               `json:"version"`
	Role           string            `json:"role"`
	Primary        bool              `json:"primary"`
	BlockSize      int64             `json:"blockSize"`
	SegmentSize    int64             `json:"segmentSize"`
	FullPageWrites string            `json:"fullPageWrites"`
	WALLevel       string            `json:"walLevel"`
	SummarizeWAL   string            `json:"summarizeWAL"`
	SummaryMinutes int64             `json:"summaryMinutes"`
	ArchiveSeconds int64             `json:"archiveSeconds"`
	ArchiveMode    string            `json:"archiveMode"`
	Tablespaces    map[string]string `json:"tablespaces"`
	TablespaceOIDs map[string]uint32 `json:"tablespaceOIDs"`
	Postmaster     string            `json:"postmaster"`
	Clock          string            `json:"clock"`
}

// All functions/settings here are public to CNPG's replication role. Privileged
// physical identity is read separately using pg_controldata, never SQL superuser.
const preflightSQL = `SELECT json_build_object(
 'version', current_setting('server_version_num')::int,
 'postmaster', to_char(pg_postmaster_start_time() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
 'clock', to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
 'tablespaceOIDs', COALESCE((SELECT json_object_agg(spcname,oid) FROM pg_tablespace WHERE spcname NOT IN ('pg_default','pg_global')), '{}'::json),
 'role', current_user, 'primary', NOT pg_is_in_recovery(),
 'blockSize', current_setting('block_size')::bigint,
 'segmentSize', pg_size_bytes(current_setting('segment_size')),
 'fullPageWrites', current_setting('full_page_writes'),
 'walLevel', current_setting('wal_level'), 'summarizeWAL', current_setting('summarize_wal'),
 'summaryMinutes', (SELECT setting::bigint FROM pg_settings WHERE name='wal_summary_keep_time'),
 'archiveSeconds', (SELECT setting::bigint FROM pg_settings WHERE name='archive_timeout'),
 'archiveMode', current_setting('archive_mode'),
 'tablespaces', COALESCE((SELECT json_object_agg(spcname,pg_tablespace_location(oid)) FROM pg_tablespace
 WHERE spcname NOT IN ('pg_default','pg_global')), '{}'::json));`

func validateState(state serverState, c Connection, n configuration.Native) error {
	if state.Version != 180006 || state.Role != "streaming_replica" || !state.Primary || state.BlockSize != 8192 || state.SegmentSize != 1<<30 || state.FullPageWrites != "on" || (state.WALLevel != "replica" && state.WALLevel != "logical") || state.SummarizeWAL != "on" {
		return errors.New("unsupported actual PostgreSQL version/role/primary/physical WAL settings")
	}
	age, err := time.ParseDuration(n.MaxReferenceAge)
	if err != nil {
		return err
	}
	timeout, err := time.ParseDuration(n.CaptureTimeout)
	if err != nil {
		return err
	}
	if state.SummaryMinutes <= int64((age+timeout+24*time.Hour)/time.Minute) || state.SummaryMinutes > int64((3650*24*time.Hour)/time.Minute) || state.ArchiveSeconds < 1 || state.ArchiveSeconds > 60 {
		return errors.New("actual WAL summary retention/archive timeout outside supported profile")
	}
	if !reflect.DeepEqual(state.Tablespaces, c.Tablespaces) {
		return errors.New("actual PostgreSQL tablespace layout differs from CNPG-managed targets")
	}
	return nil
}

type boundedOutput struct {
	data     []byte
	exceeded bool
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	n := len(p)
	left := (1 << 20) - len(b.data)
	if n > left {
		b.exceeded = true
		p = p[:left]
	}
	b.data = append(b.data, p...)
	return n, nil
}

func command(ctx context.Context, env []string, tool string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return runTool(ctx, env, tool, args...)
}

// Check runs real certificate-authenticated local-primary SQL plus mounted
// control metadata checks. It does not capture data or advertise a Backup RPC.
// Native handlers must first reserve full phase capacity using PreflightCapture,
// then perform this identity check before and after their operation.
func Check(ctx context.Context, projection string) error { return check(ctx, projection, false) }

// CheckWAL permits standby archiving after promotion/demotion, but retains the
// actual version/role/format/settings checks. Archive mode must really be on;
// lifecycle declarations alone cannot establish it.
func CheckWAL(ctx context.Context, root *os.Root) (Control, error) {
	if err := checkFromRoot(ctx, root, true); err != nil {
		return Control{}, err
	}
	return ReadControl(ctx)
}
func check(ctx context.Context, projection string, wal bool) error {
	root, err := configuration.Projection(projection)
	if err != nil {
		return err
	}
	defer root.Close()
	return checkFromRoot(ctx, root, wal)
}
func checkFromRoot(ctx context.Context, root *os.Root, wal bool) error {
	snapshot, err := configuration.LoadCaptureSnapshotFromRoot(root)
	if err != nil {
		return err
	}
	data, err := configuration.Read(root, "native/connection.json", 64<<10)
	if err != nil {
		return err
	}
	var connection Connection
	if configuration.StrictJSON(data, &connection) != nil || len(connection.Tablespaces) > 64 || !validHost(connection.Host) {
		return errors.New("invalid native connection projection")
	}
	data, err = configuration.Read(root, "capacity.json", 64<<10)
	if err != nil {
		return err
	}
	var budgets []configuration.FilesystemBudget
	if configuration.StrictJSON(data, &budgets) != nil {
		return errors.New("invalid native capacity projection")
	}
	workspace := false
	for i := range budgets {
		if wal {
			// Archive's metadata probe only reads source filesystems. Keep
			// their finite backing checks, not any projected output budget.
			budgets[i].RequiredBytes = 0
		}
		if budgets[i].Mount == "/cnpg-backup/work" {
			budgets[i].RequiredBytes = 1 << 20
			workspace = true
		}
	}
	if !workspace {
		return errors.New("native workspace unavailable")
	}
	if err = configuration.CheckCapacity(budgets); err != nil {
		return err
	}
	expectedWAL := "/var/lib/postgresql/data/pgdata/pg_wal"
	for _, budget := range budgets {
		if budget.Mount == "/var/lib/postgresql/wal" {
			expectedWAL = budget.Mount + "/pg_wal"
		}
	}
	if actual, err := filepath.EvalSymlinks("/var/lib/postgresql/data/pgdata/pg_wal"); err != nil || actual != expectedWAL {
		return errors.New("actual WAL layout differs from declared CNPG volume")
	}
	paths := []string{"/var/lib/postgresql/data/pgdata"}
	for _, path := range connection.Tablespaces {
		paths = append(paths, path)
	}
	for _, path := range paths {
		if actual, err := filepath.EvalSymlinks(path); err != nil || actual != path {
			return errors.New("unmanaged actual data/tablespace symlink layout")
		}
	}
	directory, err := os.MkdirTemp("/cnpg-backup/work", "native-auth-")
	if err != nil {
		return errors.New("native private workspace unavailable")
	}
	defer os.RemoveAll(directory)
	for name, value := range map[string][]byte{"client.crt": snapshot.Certificate, "client.key": snapshot.Key, "server-ca.crt": snapshot.ServerCA} {
		if err = os.WriteFile(filepath.Join(directory, name), value, 0600); err != nil {
			return errors.New("native private snapshot write failed")
		}
	}
	service := "[local]\nhost=" + connection.Host + "\nhostaddr=127.0.0.1\nport=5432\nuser=streaming_replica\ndbname=postgres\nsslmode=verify-full\nconnect_timeout=10\nsslcert=" + directory + "/client.crt\nsslkey=" + directory + "/client.key\nsslrootcert=" + directory + "/server-ca.crt\n"
	if os.WriteFile(filepath.Join(directory, "service.conf"), []byte(service), 0600) != nil {
		return errors.New("native service snapshot write failed")
	}
	env := []string{"LANG=C", "LC_ALL=C", "HOME=/nonexistent", "PGSERVICE=local", "PGSERVICEFILE=" + filepath.Join(directory, "service.conf"), "PGPASSFILE=/nonexistent", "PGOPTIONS=-c search_path=pg_catalog -c statement_timeout=30000", "PGAPPNAME=cnpg-backup-preflight"}
	output, err := command(ctx, env, "psql", "-X", "-A", "-t", "--no-password", "-v", "ON_ERROR_STOP=1", "-c", preflightSQL)
	if err != nil {
		return err
	}
	var state serverState
	if json.Unmarshal(output, &state) != nil {
		return errors.New("invalid native metadata response")
	}
	if wal {
		if state.ArchiveMode != "on" && state.ArchiveMode != "always" {
			return errors.New("actual archive_mode must be on or always")
		}
		state.Primary = true // standby/old-primary WAL flushing is supported, not capture
	}
	if err = validateState(state, connection, snapshot.Repository.Spec.Native); err != nil {
		return err
	}
	output, err = command(ctx, []string{"LANG=C", "LC_ALL=C"}, "pg_controldata", "/var/lib/postgresql/data/pgdata")
	if err != nil {
		return err
	}
	return validateControl(string(output))
}

func validHost(host string) bool {
	if len(host) < 1 || len(host) > 253 || !strings.HasSuffix(host, ".svc") {
		return false
	}
	for _, r := range host {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '.') {
			return false
		}
	}
	return true
}

func validateControl(output string) error {
	fields := map[string]string{}
	for _, line := range strings.Split(output, "\n") {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			fields[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	for name, value := range map[string]string{"Database block size": "8192", "Blocks per segment of large relation": "131072", "WAL block size": "8192"} {
		if fields[name] != value {
			return errors.New("unsupported actual PostgreSQL control layout")
		}
	}
	system, err := strconv.ParseUint(fields["Database system identifier"], 10, 64)
	if err != nil || system == 0 {
		return errors.New("invalid actual PostgreSQL system identity")
	}
	wal, err := strconv.ParseInt(fields["Bytes per WAL segment"], 10, 64)
	if err != nil || wal < 1<<20 || wal > 1<<30 || wal&(wal-1) != 0 {
		return errors.New("unsupported actual WAL segment size")
	}
	if checksum := fields["Data page checksum version"]; checksum != "0" && checksum != "1" {
		return errors.New("unsupported actual checksum layout")
	}
	return nil
}
