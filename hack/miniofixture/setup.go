// Copyright 2026 cnpg_backup contributors. All rights reserved.
// Package miniofixture is test setup only, never imported by product code.
package miniofixture

import (
	"context"
	"net/http"
	"time"

	"github.com/minio/minio-go/v7"
)

// CreateBucket establishes an initialized API before product assertions begin.
// Callers supply only their disposable pinned MinIO and use SDK MaxRetries=1.
// Pinned /minio/health/ready can return 200 while ObjectAPI is still nil. The
// bucket handler then returns this explicit pre-mutation initialization error.
// Never retry auth, generic 503, existing-bucket or ambiguous transport errors;
// this is NOT a product operation/mutation retry policy.
func CreateBucket(ctx context.Context, core *minio.Core, bucket, region string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := core.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: region})
		response := minio.ToErrorResponse(err)
		if response.Code != "XMinioServerNotInitialized" || response.StatusCode != http.StatusServiceUnavailable {
			return err
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
