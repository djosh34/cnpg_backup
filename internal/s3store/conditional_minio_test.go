package s3store

import (
	"context"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// The pinned MinIO erasure-object.go putObject path returns ObjectNotFound
// (HTTP 404 XML NoSuchKey), not 412, for If-Match on a missing object:
// https://github.com/minio/minio/blob/07c3a429bfed433e49018cb0f78a52145d4bedeb/cmd/erasure-object.go#L1281-L1285
// Exercise that response through TLS, the real SDK and the complete probe.
func TestMinIOMissingMatchProbe(t *testing.T) {
	for _, signature := range []string{"v2", "v4"} {
		t.Run(signature, func(t *testing.T) {
			backend := &objectServer{}
			var missing atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "PUT" && strings.HasSuffix(r.URL.Path, "/missing") && r.Header.Get("If-Match") != "" {
					missing.Add(1)
					s3error(w, http.StatusNotFound, "NoSuchKey")
					return
				}
				backend.ServeHTTP(w, r)
			}))
			defer server.Close()
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			c := config(server.URL, ca)
			c.Signature = signature
			s, err := New(c)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			keys, err := s.CheckConditions(context.Background(), "probes", t.TempDir())
			if err != nil {
				t.Fatal("conditional probe", err)
			}
			if len(keys) != 2 || missing.Load() != 1 {
				t.Fatalf("cleanup keys=%d missing-match requests=%d", len(keys), missing.Load())
			}
			backend.mu.Lock()
			defer backend.mu.Unlock()
			if backend.deletes != 0 || len(backend.objects) != 1 {
				t.Fatal("probe cleaned up or created missing CAS target")
			}
		})
	}
}

// Missing-key CAS rejection is not write success, general mutation absence,
// or proof that any earlier ambiguous request drained.
func TestMissingMatchResponseClassification(t *testing.T) {
	for _, tc := range []struct {
		name, operation, body string
		status                int
		short                 bool
		kind                  Kind
		ambiguous             bool
	}{
		{"missing-match", "match", "<Error><Code>NoSuchKey</Code></Error>", 404, false, Precondition, false},
		{"create", "create", "<Error><Code>NoSuchKey</Code></Error>", 404, false, Unknown, true},
		{"delete", "delete", "<Error><Code>NoSuchKey</Code></Error>", 404, false, Unknown, true},
		{"missing-bucket", "match", "<Error><Code>NoSuchBucket</Code></Error>", 404, false, Unknown, true},
		{"wrong-status", "match", "<Error><Code>NoSuchKey</Code></Error>", 400, false, Unknown, true},
		{"empty", "match", "", 404, false, Unknown, true},
		{"malformed", "match", "<Error><Code>NoSuchKey</Code>", 404, false, Unknown, true},
		{"wrong-root", "match", "<Other><Code>NoSuchKey</Code></Other>", 404, false, Unknown, true},
		{"short", "match", "<Error><Code>NoSuchKey</Code></Error>", 404, true, Transient, true},
		{"server-error", "match", "<Error><Code>NoSuchKey</Code></Error>", 500, false, Transient, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			s, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.Header().Set("Content-Type", "application/xml")
				if tc.short {
					w.Header().Set("Content-Length", fmt.Sprint(len(tc.body)+1))
				}
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			f, expected := spool(t, []byte("replacement"))
			var err error
			if tc.operation == "delete" {
				err = s.Delete(context.Background(), "key")
			} else {
				condition := Condition{Match: "old-etag"}
				if tc.operation == "create" {
					condition = Condition{Create: true}
				}
				_, err = s.PutFile(context.Background(), "key", f, expected, condition, nil)
			}
			mustKind(t, err, tc.kind, tc.ambiguous)
			if requests.Load() != 1 {
				t.Fatalf("mutation attempts=%d", requests.Load())
			}
		})
	}
}
