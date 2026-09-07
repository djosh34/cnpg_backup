package wal

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

// Actual SDK/private-CA HTTP path: a bare HEAD404 is not itself absence. Only
// its exact authenticated GET NoSuchKey can be; malformed/bucket errors remain
// fatal, and no confirming GET may erase a preceding non-404 HEAD failure.
func TestHEADConfirmationPreservesFirstFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		head int
		body string
		want s3store.Kind
		gets int32
	}{
		{"authenticated key absence", 404, "<Error><Code>NoSuchKey</Code></Error>", s3store.NotFound, 1},
		{"missing bucket", 404, "<Error><Code>NoSuchBucket</Code></Error>", s3store.Unknown, 1},
		{"bare404", 404, "", s3store.Unknown, 1},
		{"malformed XML", 404, "<Error><Code>NoSuchKey</Code>", s3store.Unknown, 1},
		{"auth then GET miss", 403, "<Error><Code>NoSuchKey</Code></Error>", s3store.Auth, 0},
		{"exhausted transport then GET miss", 503, "<Error><Code>NoSuchKey</Code></Error>", s3store.Transient, 0},
		{"corrupt metadata then GET miss", 200, "<Error><Code>NoSuchKey</Code></Error>", s3store.Corrupt, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gets atomic.Int32
			w, _ := setup(t, 1<<20, func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(out http.ResponseWriter, r *http.Request) {
					if !strings.Contains(r.URL.Path, "/wal/") {
						next.ServeHTTP(out, r)
						return
					}
					if r.Method == "HEAD" {
						if tc.head == 200 {
							// Valid HTTP/native metadata framing but deliberately
							// absent ETag reaches the adapter's corruption check.
							out.Header().Set("Content-Length", "1")
							out.Header().Set("Last-Modified", "Mon, 07 Sep 2026 00:00:00 GMT")
						}
						out.WriteHeader(tc.head)
						return
					}
					if r.Method == "GET" {
						gets.Add(1)
						out.Header().Set("Content-Type", "application/xml")
						out.WriteHeader(404)
						fmt.Fprint(out, tc.body)
						return
					}
					t.Error("unexpected mutation", r.Method)
				})
			})
			root, _ := local(t)
			e := w.Restore(context.Background(), "000000010000000000000001", root, "RECOVERYXLOG")
			if !s3store.Is(e, tc.want) || gets.Load() != tc.gets {
				t.Fatalf("error=%v GETs=%d want=%s/%d", e, gets.Load(), tc.want, tc.gets)
			}
		})
	}
}
