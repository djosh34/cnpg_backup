// Copyright 2026 cnpg_backup contributors. All rights reserved.
package miniofixture

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func setupCore(t *testing.T, reply func(int) (int, string, error)) (*minio.Core, *int) {
	t.Helper()
	// The pinned SDK sleeps with jitter after even its final failed attempt.
	// Remove only that SDK cooldown in this isolated test process: request-count
	// assertions still catch every extra dispatch, and our startup waits remain.
	unit := minio.DefaultRetryUnit
	minio.DefaultRetryUnit = 0
	t.Cleanup(func() { minio.DefaultRetryUnit = unit })
	calls := new(int)
	core, err := minio.NewCore("127.0.0.1:1", &minio.Options{
		Secure: true, Region: "us-east-1", BucketLookup: minio.BucketLookupPath, MaxRetries: 1,
		Creds: credentials.NewStaticV4("test-only", "test-only", ""),
		Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
			*calls++
			if r.Method != http.MethodPut || r.URL.Path != "/fixture-bucket/" {
				t.Error("startup unexpectedly dispatched a non-bucket operation")
			}
			status, code, err := reply(*calls)
			if err != nil {
				return nil, err
			}
			body := ""
			if code != "" {
				body = fmt.Sprintf("<Error><Code>%s</Code><Message>fixture</Message></Error>", code)
			}
			return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/xml"}},
				Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
		}),
	})
	if err != nil {
		t.Fatal("test SDK construction failed")
	}
	return core, calls
}

func TestCreateBucketWaitsForInitializedAPI(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		core, calls := setupCore(t, func(n int) (int, string, error) {
			if n <= 3 {
				return 503, "XMinioServerNotInitialized", nil
			}
			return 200, "", nil
		})
		start := time.Now()
		if err := CreateBucket(t.Context(), core, "fixture-bucket", "us-east-1"); err != nil {
			t.Fatalf("startup stopped before initialized API: sdk_code=%s", minio.ToErrorResponse(err).Code)
		}
		if *calls != 4 || time.Since(start) != 300*time.Millisecond {
			t.Fatal("startup did not use exactly three bounded waits then one successful create")
		}
	})
}

func TestCreateBucketDoesNotRetryUnrelatedOrAmbiguousErrors(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		status     int
		err        error
	}{
		{name: "auth", status: 403, code: "AccessDenied"},
		{name: "signature", status: 403, code: "SignatureDoesNotMatch"},
		{name: "unrelated-503", status: 503, code: "SlowDown"},
		{name: "wrong-status", status: 403, code: "XMinioServerNotInitialized"},
		{name: "existing-bucket", status: 409, code: "BucketAlreadyOwnedByYou"},
		{name: "response-lost", err: io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				core, calls := setupCore(t, func(int) (int, string, error) { return tc.status, tc.code, tc.err })
				start := time.Now()
				if CreateBucket(t.Context(), core, "fixture-bucket", "us-east-1") == nil || *calls != 1 || time.Since(start) != 0 {
					t.Fatal("unrelated/ambiguous failure was hidden, retried or delayed")
				}
			})
		})
	}
}

func TestCreateBucketStartupDeadlineAndCancellation(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			core, calls := setupCore(t, func(int) (int, string, error) { return 503, "XMinioServerNotInitialized", nil })
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if canceled {
				cancel()
			}
			start := time.Now()
			err := CreateBucket(ctx, core, "fixture-bucket", "us-east-1")
			if canceled {
				if !errors.Is(err, context.Canceled) || *calls != 0 {
					t.Fatal("canceled startup dispatched work")
				}
				// At exactly 30s the retry timer and context deadline may both
				// become runnable; either ordering must stop at that same instant.
			} else if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 30*time.Second || *calls < 300 || *calls > 301 {
				t.Fatalf("uninitialized server escaped deadline: elapsed=%s calls=%d error_type=%T deadline=%t", time.Since(start), *calls, err, errors.Is(err, context.DeadlineExceeded))
			}
		})
	}
}
