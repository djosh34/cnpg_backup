// Copyright 2026 cnpg_backup contributors. All rights reserved.
package configuration

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

const Group = "backup.cnpg-backup.djosh34.github.io"
const Version = "v1alpha1"
const GiB int64 = 1 << 30

type Selector struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}
type S3 struct {
	Endpoint           string    `json:"endpoint"`
	Bucket             string    `json:"bucket"`
	Prefix             string    `json:"prefix"`
	Signature          string    `json:"signature"`
	Addressing         string    `json:"addressing"`
	Region             string    `json:"region"`
	AccessKeySecret    Selector  `json:"accessKeySecret"`
	SecretKeySecret    Selector  `json:"secretKeySecret"`
	SessionTokenSecret *Selector `json:"sessionTokenSecret,omitempty"`
	CAConfigMap        *Selector `json:"caConfigMap,omitempty"`
	Encryption         string    `json:"encryption"`
}
type IO struct {
	ConnectTimeout     string `json:"connectTimeout"`
	MetadataTimeout    string `json:"metadataTimeout"`
	DataRequestTimeout string `json:"dataRequestTimeout"`
	OperationTimeout   string `json:"operationTimeout"`
	WALUploadTimeout   string `json:"walUploadTimeout"`
	ArtifactUploads    int    `json:"artifactUploads"`
	PartWorkers        int    `json:"partWorkers"`
	WALUploads         int    `json:"walUploads"`
}
type Retention struct {
	Enabled      bool   `json:"enabled"`
	DryRun       bool   `json:"dryRun"`
	Window       string `json:"window,omitempty"`
	MinimumFulls int    `json:"minimumFulls"`
	Interval     string `json:"interval"`
}
type Workspace struct {
	StorageClassName string `json:"storageClassName"`
	Size             string `json:"size"`
}
type Native struct {
	MaxBackupBytes       int64  `json:"maxBackupBytes"`
	MaxBootstrapWALBytes int64  `json:"maxBootstrapWALBytes"`
	MaxRestoredBytes     int64  `json:"maxRestoredBytes"`
	MaxReferenceAge      string `json:"maxReferenceAge"`
	CaptureTimeout       string `json:"captureTimeout"`
}
type Freshness struct {
	FullMaxAge         string `json:"fullMaxAge,omitempty"`
	DifferentialMaxAge string `json:"differentialMaxAge,omitempty"`
}
type Spec struct {
	RepositoryID    string                    `json:"repositoryID"`
	S3              S3                        `json:"s3"`
	Compression     string                    `json:"compression"`
	IO              IO                        `json:"io"`
	Retention       Retention                 `json:"retention"`
	Workspace       Workspace                 `json:"workspace"`
	Native          Native                    `json:"native"`
	Resources       core.ResourceRequirements `json:"resources"`
	BackupFreshness Freshness                 `json:"backupFreshness,omitempty"`
}

func Defaults() Spec {
	return Spec{S3: S3{Signature: "v4", Addressing: "path", Region: "us-east-1", Encryption: "bucket-default"}, Compression: "gzip",
		IO: IO{"10s", "30s", "15m", "24h", "120s", 2, 2, 2}, Retention: Retention{DryRun: true, MinimumFulls: 2, Interval: "1h"},
		Workspace: Workspace{Size: "256Gi"}, Native: Native{32 * GiB, 8 * GiB, 32 * GiB, "192h", "6h"},
		Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("100m"), core.ResourceMemory: resource.MustParse("256Mi")}, Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("2"), core.ResourceMemory: resource.MustParse("3Gi")}}}
}

var uuid = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func duration(value string, min, max time.Duration) bool {
	d, err := time.ParseDuration(value)
	return err == nil && d >= min && d <= max
}
func (s Selector) Validate() bool {
	return len(validation.IsDNS1123Subdomain(s.Name)) == 0 && s.Name != "" && len(validation.IsConfigMapKey(s.Key)) == 0 && s.Key != ""
}
func (s Spec) Validate() error {
	fail := func(field string) error { return fmt.Errorf("invalid Repository.spec.%s", field) }
	if !uuid.MatchString(s.RepositoryID) {
		return fail("repositoryID")
	}
	u, err := url.Parse(s.S3.Endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") {
		return fail("s3.endpoint")
	}
	if len(validation.IsDNS1123Subdomain(s.S3.Bucket)) != 0 || len(s.S3.Bucket) < 3 || len(s.S3.Bucket) > 63 {
		return fail("s3.bucket")
	}
	// Match s3store's safe printable-ASCII path contract; Repository requires
	// a nonempty prefix even though the low-level adapter permits bucket root.
	if s.S3.Prefix == "" || len(s.S3.Prefix) > 128 || path.Clean(s.S3.Prefix) != s.S3.Prefix || strings.HasPrefix(s.S3.Prefix, "/") || strings.ContainsAny(s.S3.Prefix, "\\%?#") || s.S3.Prefix == "." || s.S3.Prefix == ".." || strings.HasPrefix(s.S3.Prefix, "../") {
		return fail("s3.prefix")
	}
	for _, c := range s.S3.Prefix {
		if c < 32 || c > 126 {
			return fail("s3.prefix")
		}
	}
	if s.S3.Signature != "v2" && s.S3.Signature != "v4" {
		return fail("s3.signature")
	}
	if s.S3.Addressing != "path" || s.S3.Encryption != "bucket-default" || s.S3.Region == "" || len(s.S3.Region) > 64 {
		return fail("s3.addressing/encryption/region")
	}
	if !s.S3.AccessKeySecret.Validate() || !s.S3.SecretKeySecret.Validate() {
		return fail("s3 credential references")
	}
	if s.S3.SessionTokenSecret != nil && (s.S3.Signature != "v4" || !s.S3.SessionTokenSecret.Validate()) {
		return fail("s3.sessionTokenSecret")
	}
	if s.S3.CAConfigMap != nil && !s.S3.CAConfigMap.Validate() {
		return fail("s3.caConfigMap")
	}
	if s.Compression != "gzip" && s.Compression != "none" {
		return fail("compression")
	}
	for _, v := range []struct {
		s        string
		min, max time.Duration
	}{
		{s.IO.ConnectTimeout, time.Second, time.Minute}, {s.IO.MetadataTimeout, time.Second, 2 * time.Minute},
		{s.IO.DataRequestTimeout, 10 * time.Second, time.Hour}, {s.IO.OperationTimeout, time.Minute, 7 * 24 * time.Hour},
		{s.IO.WALUploadTimeout, 10 * time.Second, 15 * time.Minute}, {s.Native.MaxReferenceAge, time.Hour, 30 * 24 * time.Hour},
		{s.Native.CaptureTimeout, time.Minute, 6 * time.Hour}, {s.Retention.Interval, 5 * time.Minute, 24 * time.Hour},
	} {
		if !duration(v.s, v.min, v.max) {
			return fail("duration bounds")
		}
	}
	for _, n := range []int{s.IO.ArtifactUploads, s.IO.PartWorkers, s.IO.WALUploads} {
		if n < 1 || n > 2 {
			return fail("io concurrency")
		}
	}
	if s.Retention.MinimumFulls < 1 || s.Retention.MinimumFulls > 100 || (s.Retention.Enabled && s.Retention.Window == "") || (s.Retention.Window != "" && !duration(s.Retention.Window, time.Hour, 3650*24*time.Hour)) {
		return fail("retention")
	}
	for _, v := range []string{s.BackupFreshness.FullMaxAge, s.BackupFreshness.DifferentialMaxAge} {
		if v != "" && !duration(v, time.Minute, 3650*24*time.Hour) {
			return fail("backupFreshness")
		}
	}
	if s.Native.MaxBackupBytes < 1 || s.Native.MaxBackupBytes > 1024*GiB || s.Native.MaxBootstrapWALBytes < 1 || s.Native.MaxBootstrapWALBytes > 256*GiB || s.Native.MaxBootstrapWALBytes > s.Native.MaxBackupBytes || s.Native.MaxRestoredBytes < 1 || s.Native.MaxRestoredBytes > 1024*GiB {
		return fail("native byte budgets")
	}
	size, err := resource.ParseQuantity(s.Workspace.Size)
	if err != nil || size.Value() < GiB || size.Value() > 8192*GiB || s.Workspace.StorageClassName == "" || len(validation.IsDNS1123Subdomain(s.Workspace.StorageClassName)) != 0 {
		return fail("workspace")
	}
	if len(s.Resources.Claims) != 0 || len(s.Resources.Requests) != 2 || len(s.Resources.Limits) != 2 {
		return fail("resources")
	}
	for _, v := range []struct {
		name core.ResourceName
		max  resource.Quantity
	}{{core.ResourceCPU, resource.MustParse("2")}, {core.ResourceMemory, resource.MustParse("3Gi")}} {
		req, ok1 := s.Resources.Requests[v.name]
		lim, ok2 := s.Resources.Limits[v.name]
		if !ok1 || !ok2 || req.Sign() <= 0 || lim.Sign() <= 0 || req.Cmp(lim) > 0 || lim.Cmp(v.max) > 0 {
			return fail("resources")
		}
	}
	return nil
}

// StrictJSON rejects duplicate/unknown keys, invalid UTF-8, trailing documents,
// oversized input and excessive nesting before decoding any trusted config.
func StrictJSON(data []byte, out any) error {
	if len(data) > 256<<10 || !utf8.Valid(data) {
		return errors.New("invalid bounded JSON")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	var scan func(int) error
	scan = func(depth int) error {
		if depth > 32 {
			return errors.New("JSON nesting limit")
		}
		token, err := d.Token()
		if err != nil {
			return err
		}
		switch token {
		case json.Delim('{'):
			seen := map[string]bool{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return err
				}
				s, ok := key.(string)
				if !ok || seen[s] {
					return errors.New("duplicate JSON field")
				}
				seen[s] = true
				if err := scan(depth + 1); err != nil {
					return err
				}
			}
			_, err = d.Token()
			return err
		case json.Delim('['):
			for d.More() {
				if err := scan(depth + 1); err != nil {
					return err
				}
			}
			_, err = d.Token()
			return err
		}
		return nil
	}
	if err := scan(0); err != nil {
		return errors.New("invalid JSON document")
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing JSON document")
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return errors.New("invalid or unknown configuration field")
	}
	return nil
}
func DecodeSpec(data []byte) (Spec, error) {
	s := Defaults()
	if err := StrictJSON(data, &s); err != nil {
		return s, err
	}
	return s, s.Validate()
}
func (s Spec) Hash() string {
	data, _ := json.Marshal(s)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func (s Spec) SameStorage(other Spec) bool {
	return s.RepositoryID == other.RepositoryID && s.S3.Endpoint == other.S3.Endpoint && s.S3.Bucket == other.S3.Bucket && s.S3.Prefix == other.S3.Prefix && s.S3.Signature == other.S3.Signature && s.S3.Addressing == other.S3.Addressing && s.S3.Region == other.S3.Region
}
