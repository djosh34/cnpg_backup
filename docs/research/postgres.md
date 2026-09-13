# PostgreSQL native-tool notes

These upstream details explain the [capture and reconstruction rules](../design.md#full-and-differential-capture). Sources use PostgreSQL 18.6 at `724edf9bde9d356724ad384a2e196edc3c9f80f7`. Build inputs and package checksums live in [build/inputs.lock.json](../../build/inputs.lock.json).

## Full-reference differentials

`pg_basebackup --incremental` accepts an earlier backup's original manifest. Supplying the same full manifest to D1 and D2 makes both depend directly on F. `pg_combinebackup F D2` then reconstructs D2 without D1. PostgreSQL has no separate differential command.

Required local WAL summaries cover the reference backup's start through the new backup's start. Missing summaries cause failure, not automatic full fallback. Remote archived WAL is not a substitute. PostgreSQL supports some cross-timeline incremental chains, but this product's same-timeline and postmaster-continuity restrictions are narrower support rules.

Sources:

- [Incremental backup requirements](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/doc/src/sgml/backup.sgml)
- [Server summary and identity checks](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/backup/basebackup_incremental.c)
- [Upstream cross-timeline tests](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/bin/pg_combinebackup/t/003_timeline.pl)

## Capture and WAL verification

Tar output supports tablespaces and streamed WAL through local `base.tar`, `<OID>.tar`, `pg_wal.tar`, and `backup_manifest` files. Stdout tar cannot support extra tablespaces or WAL streaming. `--tablespace-mapping` is ignored in tar mode, and `--waldir` applies only to plain mode.

`--wal-method=stream` uses two replication connections and a temporary replication slot by default. `--max-rate` does not throttle streamed WAL. Stream mode does not wait for a second archive copy of bundled WAL before backup completion.

PG18 verifies tar backups, including incrementals, but cannot parse WAL inside tar. The plain-directory verifier launches `pg_waldump` through `system()`, requiring a shell. The shell-free plugin instead always uses `pg_verifybackup --no-parse-wal` and directly invokes `pg_waldump` for every validated manifest range:

```sh
pg_waldump --quiet --path=WAL_DIRECTORY --timeline=TIMELINE \
  --start=START_LSN --end=END_LSN
```

WAL files are not individually protected by the manifest's file checksums. Range parsing and artifact hashes have separate purposes. A manifest's End-LSN is its consistency boundary, not a wall-clock completion time or the end of the last segment. The manifest has no backup-completion timestamp. Time selection therefore needs a conservative source-server observation after capture, rather than a local file timestamp.

Sources:

- [pg_basebackup options](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/doc/src/sgml/ref/pg_basebackup.sgml) and [stream completion](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/bin/pg_basebackup/pg_basebackup.c)
- [pg_verifybackup format support](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/doc/src/sgml/ref/pg_verifybackup.sgml) and [shell invocation](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/bin/pg_verifybackup/pg_verifybackup.c#L1200-L1215)
- [Manifest schema and WAL ranges](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/doc/src/sgml/backup-manifest.sgml)
- [Archive-before-bundle behavior](cnpg.md#wal-failures-and-archive-order)

## Reconstruction and layout transforms

Verification of the combined output does not authenticate its inputs. `pg_combinebackup` can generate a new manifest over corrupted incremental data that then passes `pg_verifybackup`. Each original full and differential must pass verification before combination. `hack/recovery.py` retains this executable negative control.

For tablespaces, combine's old mapping path is the link target in the last input, not the original server path or F's extracted path. Combine copies `tablespace_map` without rewriting it to the new mapping. The plugin verifies that file first, then removes it before startup so PostgreSQL keeps the trusted target links instead of recreating historical paths.

Combine skips ordinary symlinks, including a `pg_wal` symlink. Input WAL must remain a directory. The combined output contains the selected differential's WAL, not a union of all input WAL. Separate-volume relocation happens after verification and must account for a copy across filesystems rather than assuming atomic rename.

Combine does not repair page checksums after checksum-mode changes. A small differential also does not bound reconstructed output size. Native tools interpret incremental headers; the plugin uses an explicit restored-byte budget rather than its own header parser. Filesystem capacity or quota provides the hard native-writer limit. Kubernetes `emptyDir.sizeLimit` is not an instantaneous allocation limit.

Sources:

- [Combine options and verification limits](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/doc/src/sgml/ref/pg_combinebackup.sgml)
- [Tablespace, symlink, and checksum implementation](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/bin/pg_combinebackup/pg_combinebackup.c)
- [Upstream input integrity tests](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/bin/pg_combinebackup/t/005_integrity.pl)
- [Kubernetes local storage enforcement](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#local-ephemeral-storage)

The maintained native test entry point is [hack/test](../../hack/test). The [same-segment fixture](fixtures/README.md) provides an additional standalone recovery regression. Native-only results do not establish CNPG, MinIO, or image qualification.
