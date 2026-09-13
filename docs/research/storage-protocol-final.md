# S3 and MinIO upstream semantics

These notes explain the storage assumptions behind [s3-adapter.md](../s3-adapter.md) and [repository.md](../repository.md). Those documents describe the product protocol. SDK references use minio-go v7.3.0 at `ce0e323c55c64964e6ad820ef0c6f5b286446aae`. MinIO references use `RELEASE.2025-09-07T16-13-09Z` at `07c3a429bfed433e49018cb0f78a52145d4bedeb`.

## Conditional writes and consistency

The repository needs atomic conditional single-object PUT, atomic visibility, and strong GET, HEAD, and LIST after completed mutations. A prior HEAD followed by an unconditional PUT cannot replace `If-None-Match: *` or ETag-based `If-Match`. ETags are opaque concurrency tokens, not content checksums.

AWS conditional-write documentation requires SigV4. MinIO accepts the tested conditional operations with both SigV2 and SigV4. MinIO-tested V2 behavior is not an AWS V2 compatibility claim. Other endpoints must satisfy the same consistency and conditional-write requirements. A finite probe can detect broken semantics but cannot prove all failure interleavings.

A 412 response requires reconciliation, not automatic success. Conditional-write conflicts can return 409. Multipart completion conflicts can require a new upload. Versioned buckets can expose delete markers and old versions, which do not satisfy the product's permanent current-key retirement assumptions.

Sources:

- AWS [conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html) and [consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)
- MinIO [conditional header handling](https://github.com/minio/minio/blob/07c3a429bfed433e49018cb0f78a52145d4bedeb/cmd/object-handlers-common.go) and [conditions under namespace locks](https://github.com/minio/minio/blob/07c3a429bfed433e49018cb0f78a52145d4bedeb/cmd/erasure-object.go)

## SDK behavior that affects safety

- `Options.MaxRetries: 1` means one total SDK attempt. Zero selects the SDK default, not zero retries.
- `PutObjectOptions.SetMatchETagExcept("*")` and `SetMatchETag(etag)` set conditional headers and add ETag quoting.
- High-level multipart `PutObject` can abort an upload from its error path. That would perform destructive cleanup outside repository admission. The product uses explicit `Core.NewMultipartUpload`, `PutObjectPart`, `CompleteMultipartUpload`, and gate-owned abort.
- `SendContentMd5` can buffer a whole single-PUT input if the reader lacks the expected ReaderAt and Seeker behavior. A seekable WAL spool avoids whole-WAL buffering through a gzip pipe.
- GET errors can arrive while reading the returned object. Successful construction of the SDK object does not establish successful retrieval.
- A multipart completion HTTP 200 can contain an embedded error. Mutation code must consume and validate the terminal response body.
- SDK retry limits do not prevent retries in an HTTP transport or proxy. Go can retry some requests on reused connections. Destructive request configuration must account for all layers.
- The optional RDMA build imports C. Default non-RDMA builds avoid it; production dependency checks must inspect the linked package graph.

Sources:

- SDK [conditional options](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/api-put-object.go), [single-PUT buffering and multipart cleanup](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/api-put-object-streaming.go), and [Core operations](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/core.go)
- SDK [retry configuration](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/api.go), [GET implementation](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/api-get-object.go), and [non-RDMA implementation](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/rdma_stub.go)
- AWS [CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)
- Go 1.27.1 [HTTP transport](https://github.com/golang/go/blob/go1.27.1/src/net/http/transport.go) and [request replayability](https://github.com/golang/go/blob/go1.27.1/src/net/http/request.go)

## Destructive-request uncertainty

An HTTP timeout, cancellation, or lost response does not prove a remote request stopped. A successful DELETE retry and a subsequent absent HEAD also do not prove that the original request drained. S3 offers no operation that certifies all earlier requests from a failed process have completed. An MPU abort can race in-flight part uploads.

This is why the repository retains an uncertain GC owner rather than clearing it on a timer or reconstructing authority from a log. Successful holder admission and destructive ownership must be mutually exclusive. Kubernetes Lease expiry cannot fence an old S3 request, and a source cluster's Lease is not shared with a disaster-recovery cluster.

Sources:

- AWS [DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html) and [AbortMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html)
- Kubernetes [Lease scope](https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/lease-v1/) and [client-go leader election fencing limitation](https://github.com/kubernetes/client-go/blob/master/tools/leaderelection/leaderelection.go)

Maintained tests exercise these rules through the actual adapter and repository in `internal/s3store`, `internal/repository`, and `internal/retention`. MinIO remains a test dependency with its own AGPL obligations, not a shipped product server. SDK licensing remains Apache-2.0.
