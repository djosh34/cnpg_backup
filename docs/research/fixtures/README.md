# PostgreSQL same-segment regression fixture

`same-segment-pg18.tar.gz` contains synthetic PG18.6 data, not a product backup or qualification result. It preserves an original full backup, a copy padded after the final manifest End-LSN, the unmodified archive segment, and a WAL-dump restore-point record. Both copies pass manifest-range verification. Only the full archive reaches the post-EndLSN restore point in that same segment.

The fixture avoids a timing-sensitive capture. Its database uses test-only trust authentication and the role `starlord`. The runner overrides historical paths and uses a private Unix socket with no TCP listener. The fixture contains no S3 credentials or application data.

## Input identity

| Input | Value |
| --- | --- |
| Native package | PGDG `18.6-3.pgdg24.04+1`, Linux amd64 |
| PostgreSQL source | `724edf9bde9d356724ad384a2e196edc3c9f80f7` |
| Compressed fixture SHA256 | `f9f82e6900b42a996670130e3dfa1d70d61f10df7fdaaf9f7837538342f5c1ca` |
| Timeline and manifest range | Timeline 1, `0/4057538` to `0/4401660` |
| Restore point | `r6_7595`, starts at `0/4401660` and extends beyond End-LSN |
| Final WAL segment | `000000010000000000000004`, 16,777,216 bytes |
| Original archive and bundle SHA256 | `dfc0178fd70e29855e5280e8c99141c73d661fc7e25553bcfb5b78c08f1af138` |
| Padded bundle SHA256 | `599f3dac56a6ceab16ebbf44e52382ade2e089e7f6fd5e13001e4a1e74ef312f` |

Padding is a controlled test mutation from End-LSN to segment end, not naturally captured padding. The immediate-target positive control uses the intact original bundle.

## Run the regression

Set `PG_BIN` to matching PostgreSQL tools, `PG_SHARE` to their share directory, and `LD_LIBRARY_PATH` if their libraries require it. Use a non-root account and a short disk-backed `TMPDIR` with space for extracted database copies. From the repository root:

```sh
printf '%s  %s\n' f9f82e6900b42a996670130e3dfa1d70d61f10df7fdaaf9f7837538342f5c1ca \
  docs/research/fixtures/same-segment-pg18.tar.gz | sha256sum --check
fixture=$(mktemp -d)
tar --no-same-owner -xzf docs/research/fixtures/same-segment-pg18.tar.gz -C "$fixture"
RECOVERY_FIXTURE="$fixture" python3 docs/research/experiments/recovery-review-pg.py
```

The runner checks archive preference, allowed local fallback, fatal required absence, incorrect bundle-as-archive behavior, and direct WAL verification. It keeps logs and command results at the printed path and removes stopped restore copies. Without `RECOVERY_FIXTURE`, it creates a fresh post-backup named-point case. `experiments/cnpgi-wal-exit.py` separately demonstrates latest-recovery data loss when an outage incorrectly returns exit 1 rather than 255.

These native tests do not establish CNPG, MinIO, Go-helper, or shell-free-image qualification. See the [latest-recovery endpoint oracle](../s1-switch-latest.md) for a case where SQL rows alone cannot detect missing WAL.
