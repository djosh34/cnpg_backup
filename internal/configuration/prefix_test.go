package configuration

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

func TestRepositoryPrefixStorageAgreement(t *testing.T) {
	data, err := os.ReadFile("../../config/repository-crd.json")
	if err != nil {
		t.Fatal(err)
	}
	var crd map[string]any
	if err := json.Unmarshal(data, &crd); err != nil {
		t.Fatal(err)
	}
	version := crd["spec"].(map[string]any)["versions"].([]any)[0].(map[string]any)
	properties := func(node map[string]any, key string) map[string]any {
		return node["properties"].(map[string]any)[key].(map[string]any)
	}
	schema := version["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)
	prefix := properties(properties(properties(schema, "spec"), "s3"), "prefix")
	pattern, _ := prefix["pattern"].(string)
	match := regexp.MustCompile(pattern)
	if prefix["minLength"] != float64(1) || prefix["maxLength"] != float64(128) {
		t.Error("CRD prefix must require 1..128 ASCII bytes")
	}
	if _, ok := prefix["default"]; ok {
		t.Error("prefix must be explicitly supplied, not defaulted")
	}
	if !strings.Contains(fmt.Sprint(prefix["x-kubernetes-validations"]), "self == oldSelf") {
		t.Error("prefix lost storage identity immutability")
	}
	cases := []struct {
		value string
		valid bool
	}{
		{"a", true}, {strings.Repeat("a", 128), true}, {"test/repository", true},
		{".../a..b/.hidden/..suffix/end.", true}, {" space /!\"$&'()*+,-:;<=>@[]^_`{|}~", true},
		{strings.Repeat("a", 129), false}, {"café", false}, {"备份", false},
		{"", false}, {".", false}, {"..", false}, {"/a", false}, {"a/", false},
		{"a//b", false}, {"a/./b", false}, {"a/../b", false}, {"../a", false},
		{"a%20b", false}, {"a?b", false}, {"a#b", false}, {"a\\b", false},
	}
	for c := byte(0); c < 32; c++ {
		cases = append(cases, struct {
			value string
			valid bool
		}{"a" + string(c) + "b", false})
	}
	cases = append(cases, struct {
		value string
		valid bool
	}{"a\x7fb", false})
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%q", tc.value), func(t *testing.T) {
			spec := validSpec()
			spec.S3.Prefix = tc.value
			data, err := json.Marshal(spec)
			if err != nil {
				t.Fatal(err)
			}
			_, err = DecodeSpec(data)
			if (err == nil) != tc.valid {
				t.Errorf("DecodeSpec error = %v, want valid=%v", err, tc.valid)
			}
			if err != nil && err.Error() != "invalid Repository.spec.s3.prefix" {
				t.Errorf("wrong field error: %v", err)
			}
			admitted := len(tc.value) >= int(prefix["minLength"].(float64)) && len(tc.value) <= int(prefix["maxLength"].(float64)) && match.MatchString(tc.value)
			if admitted != tc.valid {
				t.Errorf("CRD accepted=%v, want %v", admitted, tc.valid)
			}
			store, err := s3store.New(s3store.Config{Endpoint: spec.S3.Endpoint, Bucket: spec.S3.Bucket, Prefix: tc.value,
				Signature: spec.S3.Signature, Addressing: spec.S3.Addressing, Region: spec.S3.Region,
				AccessKey: "fixture-access", SecretKey: "fixture-secret"})
			if store != nil {
				store.Close()
			}
			// The low-level adapter also supports bucket-root keys; Repository
			// deliberately requires an explicit nonempty prefix. No I/O is sent.
			if (err == nil) != (tc.valid || tc.value == "") {
				t.Errorf("actual constructor error = %v", err)
			}
			other := spec
			other.S3.Prefix = "different-prefix"
			if spec.SameStorage(other) {
				t.Error("prefix change retained storage identity")
			}
		})
	}
}
