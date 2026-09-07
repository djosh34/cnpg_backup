// Package repository owns v1 keys, publication and deletion admission. Callers
// supply verified native captures; PostgreSQL execution and retention policy are
// deliberately separate. All original project work is all rights reserved.
package repository

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrInvalid    = errors.New("InvalidRepositoryMetadata")
	ErrIdentity   = errors.New("BackupIdentityConflict")
	ErrCorrupt    = errors.New("RepositoryCorruption")
	ErrCapacity   = errors.New("RepositoryCapacityExceeded")
	ErrBlocked    = errors.New("RepositoryAdmissionBlocked")
	ErrClosed     = errors.New("RepositoryOperationClosed")
	ErrUncertain  = errors.New("RepositoryOperationUncertain")
	ErrRetired    = errors.New("BackupAlreadyExpired")
	ErrContention = errors.New("RepositoryContention")
)

const (
	smallLimit        int64 = 64 << 10
	gateLimit         int64 = 1 << 20
	commitLimit       int64 = 4 << 20
	MaxManifestBytes  int64 = 64 << 20
	MaxBackupBytes    int64 = 1 << 40
	MaxArtifactBytes  int64 = 512 << 30
	MaxCatalogRecords       = 1_000_000
)

var uuidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var hashRE = regexp.MustCompile(`^[0-9a-f]{64}$`)
var lsnRE = regexp.MustCompile(`^(0|[1-9A-F][0-9A-F]{0,7})/(0|[1-9A-F][0-9A-F]{0,7})$`)

func UUID() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
func validID(s string) bool {
	return uuidRE.MatchString(s) && s != "00000000-0000-0000-0000-000000000000"
}
func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func decimal(s string) (uint64, bool) {
	n, e := strconv.ParseUint(s, 10, 64)
	return n, e == nil && strconv.FormatUint(n, 10) == s
}
func ParseLSN(s string) (uint64, error) {
	if !lsnRE.MatchString(s) {
		return 0, ErrInvalid
	}
	p := strings.Split(s, "/")
	a, _ := strconv.ParseUint(p[0], 16, 32)
	b, _ := strconv.ParseUint(p[1], 16, 32)
	return a<<32 | b, nil
}
func stamp(s string) (time.Time, bool) {
	t, e := time.Parse(time.RFC3339Nano, s)
	return t, e == nil && t.Year() >= 2000 && s == t.UTC().Format(time.RFC3339Nano)
}
func validText(s string, n int) bool {
	return s != "" && len(s) <= n && utf8.ValidString(s) && !strings.ContainsAny(s, "\x00\r\n")
}

// strict rejects duplicate/unknown/missing fields, invalid UTF-8, null for
// nonnullable fields, excessive nesting, and trailing JSON before typed decode.
// Every tagged field is required, including explicitly nullable pointers.
func strict(b []byte, max int64, out any) error {
	if int64(len(b)) > max {
		return ErrCapacity
	}
	if !utf8.Valid(b) || !validJSONUnicode(b) {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if e := shape(d, reflect.TypeOf(out).Elem(), 0); e != nil {
		return e
	}
	if _, e := d.Token(); e != io.EOF {
		return ErrInvalid
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e := d.Decode(out); e != nil {
		return ErrInvalid
	}
	return nil
}
func shape(d *json.Decoder, t reflect.Type, depth int) error {
	if depth > 24 {
		return ErrCapacity
	}
	tok, e := d.Token()
	if e != nil {
		return ErrInvalid
	}
	if t.Kind() == reflect.Pointer {
		if tok == nil {
			return nil
		}
		t = t.Elem()
	} else if tok == nil {
		return ErrInvalid
	}
	switch t.Kind() {
	case reflect.Struct:
		if tok != json.Delim('{') {
			return ErrInvalid
		}
		fields := map[string]reflect.Type{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			fields[f.Tag.Get("json")] = f.Type
		}
		seen := map[string]bool{}
		for d.More() {
			k, e := d.Token()
			if e != nil {
				return ErrInvalid
			}
			s, ok := k.(string)
			ft, exists := fields[s]
			if !ok || !exists || seen[s] {
				return ErrInvalid
			}
			seen[s] = true
			if e = shape(d, ft, depth+1); e != nil {
				return e
			}
		}
		if len(seen) != len(fields) {
			return ErrInvalid
		}
		end, e := d.Token()
		if e != nil || end != json.Delim('}') {
			return ErrInvalid
		}
	case reflect.Slice:
		if tok != json.Delim('[') {
			return ErrInvalid
		}
		for d.More() {
			if e = shape(d, t.Elem(), depth+1); e != nil {
				return e
			}
		}
		end, e := d.Token()
		if e != nil || end != json.Delim(']') {
			return ErrInvalid
		}
	case reflect.String:
		if _, ok := tok.(string); !ok {
			return ErrInvalid
		}
	case reflect.Bool:
		if _, ok := tok.(bool); !ok {
			return ErrInvalid
		}
	default:
		if _, ok := tok.(json.Number); !ok {
			return ErrInvalid
		}
	}
	return nil
}

type Identity struct {
	Schema           int    `json:"schema"`
	RepositoryID     string `json:"repository_id"`
	PostgresMajor    int    `json:"postgres_major"`
	SystemIdentifier string `json:"system_identifier"`
	WALSegmentBytes  int64  `json:"wal_segment_bytes"`
	WriterClusterUID string `json:"writer_cluster_uid"`
	CreatedAt        string `json:"created_at"`
}

func (v Identity) Validate() error {
	n, ok := decimal(v.SystemIdentifier)
	_, ts := stamp(v.CreatedAt)
	w := v.WALSegmentBytes
	if v.Schema != 1 || !validID(v.RepositoryID) || v.PostgresMajor != 18 || !ok || n == 0 || !validID(v.WriterClusterUID) || !ts || w < 1<<20 || w > 1<<30 || w&(w-1) != 0 {
		return ErrInvalid
	}
	return nil
}

type Request struct {
	Schema           int     `json:"schema"`
	RepositoryID     string  `json:"repository_id"`
	BackupUID        string  `json:"backup_uid"`
	WriterClusterUID string  `json:"writer_cluster_uid"`
	RequestedKind    string  `json:"requested_kind"`
	RootBackupUID    *string `json:"root_backup_uid"`
	ConfigSHA256     string  `json:"config_sha256"`
}

func (v Request) validate(id Identity) error {
	if v.Schema != 1 || v.RepositoryID != id.RepositoryID || !validID(v.BackupUID) || v.WriterClusterUID != id.WriterClusterUID || !hashRE.MatchString(v.ConfigSHA256) {
		return ErrIdentity
	}
	if v.RequestedKind == "full" && v.RootBackupUID == nil {
		return nil
	}
	if v.RequestedKind == "differential" && v.RootBackupUID != nil && validID(*v.RootBackupUID) && *v.RootBackupUID != v.BackupUID {
		return nil
	}
	return ErrInvalid
}

type Claim struct {
	Schema        int    `json:"schema"`
	RepositoryID  string `json:"repository_id"`
	BackupUID     string `json:"backup_uid"`
	AttemptID     string `json:"attempt_id"`
	ProcessID     string `json:"process_id"`
	RequestSHA256 string `json:"request_sha256"`
}
type Tablespace struct {
	OID  uint32 `json:"oid"`
	Name string `json:"name"`
}
type Artifact struct {
	Index         int     `json:"index"`
	Role          string  `json:"role"`
	TablespaceOID *uint32 `json:"tablespace_oid"`
	Compression   string  `json:"compression"`
	StoredBytes   int64   `json:"stored_bytes"`
	RawBytes      int64   `json:"raw_bytes"`
	StoredSHA256  string  `json:"stored_sha256"`
	RawSHA256     string  `json:"raw_sha256"`
}
type WALRange struct {
	Timeline uint32 `json:"timeline"`
	StartLSN string `json:"start_lsn"`
	EndLSN   string `json:"end_lsn"`
}
type Commit struct {
	Schema              int          `json:"schema"`
	RepositoryID        string       `json:"repository_id"`
	BackupUID           string       `json:"backup_uid"`
	AttemptID           string       `json:"attempt_id"`
	RequestSHA256       string       `json:"request_sha256"`
	Kind                string       `json:"kind"`
	ParentBackupUID     *string      `json:"parent_backup_uid"`
	RootBackupUID       string       `json:"root_backup_uid"`
	SystemIdentifier    string       `json:"system_identifier"`
	PostgresMajor       int          `json:"postgres_major"`
	ToolVersion         string       `json:"tool_version"`
	Timeline            uint32       `json:"timeline"`
	ChecksumVersion     int          `json:"checksum_version"`
	CaptureInstanceUID  string       `json:"capture_instance_uid"`
	PostmasterStartedAt string       `json:"postmaster_started_at"`
	StartedAt           string       `json:"started_at"`
	StoppedAt           string       `json:"stopped_at"`
	StartLSN            string       `json:"start_lsn"`
	StopLSN             string       `json:"stop_lsn"`
	RedoLSN             string       `json:"redo_lsn"`
	BundledWALStartLSN  string       `json:"bundled_wal_start_lsn"`
	BundledWALEndLSN    string       `json:"bundled_wal_end_lsn"`
	WALRanges           []WALRange   `json:"wal_ranges"`
	BackupLabel         string       `json:"backup_label"`
	TablespaceMap       string       `json:"tablespace_map"`
	RootManifestSHA256  *string      `json:"root_manifest_sha256"`
	ManifestBytes       int64        `json:"manifest_bytes"`
	ManifestSHA256      string       `json:"manifest_sha256"`
	Tablespaces         []Tablespace `json:"tablespaces"`
	Artifacts           []Artifact   `json:"artifacts"`
}

func (v Commit) validate(id Identity) error {
	if v.Schema != 1 || v.RepositoryID != id.RepositoryID || v.SystemIdentifier != id.SystemIdentifier || v.PostgresMajor != 18 || v.ToolVersion != "18.6" || !validID(v.BackupUID) || !validID(v.AttemptID) || !validID(v.CaptureInstanceUID) || !hashRE.MatchString(v.RequestSHA256) || v.Timeline == 0 || v.ChecksumVersion < 0 || v.ChecksumVersion > 1 {
		return ErrInvalid
	}
	start, ok := stamp(v.StartedAt)
	stop, ok2 := stamp(v.StoppedAt)
	pm, ok3 := stamp(v.PostmasterStartedAt)
	if !ok || !ok2 || !ok3 || stop.Before(start) || pm.After(start) {
		return ErrInvalid
	}
	vals := []string{v.RedoLSN, v.StartLSN, v.StopLSN, v.BundledWALStartLSN, v.BundledWALEndLSN}
	n := make([]uint64, 5)
	for i, s := range vals {
		var e error
		n[i], e = ParseLSN(s)
		if e != nil {
			return e
		}
	}
	if n[0] > n[1] || n[1] >= n[2] || n[3] > n[0] || n[4] != n[2] || len(v.WALRanges) != 1 || v.WALRanges[0] != (WALRange{v.Timeline, v.BundledWALStartLSN, v.BundledWALEndLSN}) {
		return ErrInvalid
	}
	if v.BackupLabel == "" || len(v.BackupLabel) > 64<<10 || !utf8.ValidString(v.BackupLabel) || strings.ContainsRune(v.BackupLabel, 0) || len(v.TablespaceMap) > 64<<10 || !utf8.ValidString(v.TablespaceMap) || strings.ContainsRune(v.TablespaceMap, 0) || v.ManifestBytes < 1 || v.ManifestBytes > MaxManifestBytes || !hashRE.MatchString(v.ManifestSHA256) {
		return ErrInvalid
	}
	switch v.Kind {
	case "full":
		if v.ParentBackupUID != nil || v.RootBackupUID != v.BackupUID || v.RootManifestSHA256 != nil {
			return ErrInvalid
		}
	case "differential":
		if v.ParentBackupUID == nil || !validID(*v.ParentBackupUID) || *v.ParentBackupUID != v.RootBackupUID || v.RootBackupUID == v.BackupUID || v.RootManifestSHA256 == nil || !hashRE.MatchString(*v.RootManifestSHA256) {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	if v.Tablespaces == nil || len(v.Tablespaces) > 64 || len(v.Artifacts) != len(v.Tablespaces)+2 || len(v.Artifacts) > 66 {
		return ErrCapacity
	}
	oids := map[uint32]bool{}
	names := map[string]bool{}
	for _, t := range v.Tablespaces {
		if t.OID == 0 || oids[t.OID] || !validText(t.Name, 63) || names[t.Name] {
			return ErrInvalid
		}
		oids[t.OID] = true
		names[t.Name] = true
	}
	base, wal := 0, 0
	raw := v.ManifestBytes
	stored := int64(0)
	used := map[uint32]bool{}
	for i, a := range v.Artifacts {
		if a.Index != i || a.StoredBytes < 1 || a.StoredBytes > MaxArtifactBytes || a.RawBytes < 1 || a.RawBytes > MaxBackupBytes || !hashRE.MatchString(a.StoredSHA256) || !hashRE.MatchString(a.RawSHA256) {
			return ErrInvalid
		}
		if a.Compression != "none" && a.Compression != "gzip" {
			return ErrInvalid
		}
		if a.Compression == "none" && (a.RawBytes != a.StoredBytes || a.RawSHA256 != a.StoredSHA256) {
			return ErrInvalid
		}
		raw += a.RawBytes
		stored += a.StoredBytes
		switch a.Role {
		case "base":
			base++
			if a.TablespaceOID != nil {
				return ErrInvalid
			}
		case "wal":
			wal++
			if a.TablespaceOID != nil || a.RawBytes > 256<<30 {
				return ErrInvalid
			}
		case "tablespace":
			if a.TablespaceOID == nil || !oids[*a.TablespaceOID] || used[*a.TablespaceOID] {
				return ErrInvalid
			}
			used[*a.TablespaceOID] = true
		default:
			return ErrInvalid
		}
	}
	if base != 1 || wal != 1 || raw > MaxBackupBytes || stored > MaxBackupBytes+MaxBackupBytes/100+1<<30 {
		return ErrCapacity
	}
	return nil
}
func validateParent(c, p Commit) error {
	if c.Kind != "differential" || p.Kind != "full" || c.ParentBackupUID == nil || *c.ParentBackupUID != p.BackupUID || c.RootBackupUID != p.BackupUID || c.RepositoryID != p.RepositoryID || c.SystemIdentifier != p.SystemIdentifier || c.PostgresMajor != p.PostgresMajor || c.Timeline != p.Timeline || c.ChecksumVersion != p.ChecksumVersion || c.CaptureInstanceUID != p.CaptureInstanceUID || c.PostmasterStartedAt != p.PostmasterStartedAt || c.RootManifestSHA256 == nil || *c.RootManifestSHA256 != p.ManifestSHA256 {
		return ErrCorrupt
	}
	a, _ := ParseLSN(p.StopLSN)
	b, _ := ParseLSN(c.StartLSN)
	pt, _ := stamp(p.StoppedAt)
	ct, _ := stamp(c.StartedAt)
	if a > b || ct.Before(pt) {
		return ErrCorrupt
	}
	return nil
}

type Retirement struct {
	Schema        int    `json:"schema"`
	RepositoryID  string `json:"repository_id"`
	BackupUID     string `json:"backup_uid"`
	CommitSHA256  string `json:"commit_sha256"`
	GCOperationID string `json:"gc_operation_id"`
}

func (v Retirement) validate(id Identity, uid, hash string) error {
	if v.Schema != 1 || v.RepositoryID != id.RepositoryID || v.BackupUID != uid || v.CommitSHA256 != hash || !validID(v.GCOperationID) {
		return ErrCorrupt
	}
	return nil
}
