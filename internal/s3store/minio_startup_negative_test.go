package s3store

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
)

func TestMinIOFixtureDoesNotRetryUnrelatedFailures(t *testing.T) {
	for _, code := range []string{"ServiceUnavailable", "AccessDenied", "BucketAlreadyOwnedByYou"} {
		t.Run(code, func(t *testing.T) {
			var calls atomic.Int32
			s, server := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				httpCode := 503
				if code == "AccessDenied" {
					httpCode = 403
				}
				if code == "BucketAlreadyOwnedByYou" {
					httpCode = 409
				}
				s3error(w, httpCode, code)
			}))
			e := fixtureCreateBucket(context.Background(), s, config(server.URL, nil))
			if e == nil || calls.Load() != 1 {
				t.Fatal("fixture broadened startup retry", calls.Load())
			}
		})
	}
}
