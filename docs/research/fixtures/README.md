# PostgreSQL same-segment review regression

`same-segment-pg18.tar.gz` preserves the disposable, synthetic-data PG18.6 capture from [review corrections](../review-corrections.md). It is research input, not a product backup or qualification result. It contains the native original full backup, a copy whose final bundled WAL is synthetically padded after manifest End-LSN, the unmodified archive segment, and the WAL-dump restore-point record. No application data or S3 credentials are involved. The database is deliberately configured for test-only trust authentication; the runner overrides it to a private Unix socket with no TCP listener.

SHA256: `f9f82e6900b42a996670130e3dfa1d70d61f10df7fdaaf9f7837538342f5c1ca`.

This 7.8 MiB compressed fixture avoids repeating a timing-sensitive capture and preserves the distinction between native full archive bytes and controlled padding. Both copies pass manifest-range verification; only the full archive reaches the real post-EndLSN restore point in that same segment. The original archive/bundle and padded hashes, exact LSNs and limitations are recorded in the correction report. Source version/package is pinned there. Paths in captured configuration are historical test paths, overridden before startup.

With the report's PG tool environment set, run as a non-root user:

```sh
printf '%s  %s\n' f9f82e6900b42a996670130e3dfa1d70d61f10df7fdaaf9f7837538342f5c1ca \
  docs/research/fixtures/same-segment-pg18.tar.gz | sha256sum --check
fixture=$(mktemp -d)
tar --no-same-owner -xzf docs/research/fixtures/same-segment-pg18.tar.gz -C "$fixture"
RECOVERY_FIXTURE="$fixture" python3 docs/research/experiments/recovery-review-pg.py
```

The script preserves command inventories and logs in its reported artifact directory and removes stopped restore copies to bound disk/inode use. This does not replace actual CNPG/MinIO/Go helper or shell-free-image regressions.
