package s3store

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/djosh34/cnpg_backup/hack/miniofixture"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func fixtureCreateBucket(ctx context.Context, s *Store, c Config) error {
	// Bucket creation is fixture setup, not a product primitive. The product
	// transport deliberately redacts all503 response bodies to Transient; that
	// would hide the ONE exact startup code this fixture may retry. Use the same
	// verified TLS transport with a plain single-attempt SDK setup client instead.
	core, e := minio.NewCore(strings.TrimPrefix(c.Endpoint, "https://"), &minio.Options{Secure: true, Region: c.Region, BucketLookup: minio.BucketLookupPath, MaxRetries: 1, Creds: credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""), Transport: s.transport})
	if e != nil {
		return e
	}
	return miniofixture.CreateBucket(ctx, core, c.Bucket, c.Region)
}

func TestMinIOBucketStartupRetainsExactFixtureCode(t *testing.T) {
	var calls atomic.Int32
	s, server := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" || r.URL.Path != "/test-bucket/" {
			t.Errorf("unexpected setup request %s %s", r.Method, r.URL.Path)
		}
		if calls.Add(1) == 1 {
			s3error(w, 503, "XMinioServerNotInitialized")
			return
		}
		w.WriteHeader(200)
	}))
	c := config(server.URL, nil)
	if e := fixtureCreateBucket(context.Background(), s, c); e != nil || calls.Load() != 2 {
		t.Fatalf("exact startup503 barrier: calls=%d error_type=%T", calls.Load(), e)
	}
}
