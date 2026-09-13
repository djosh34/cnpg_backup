# Same-segment latest-recovery oracle

A latest restore can promote with correct SQL rows while silently omitting WAL records. `hack/s1_switch_probe.py` distinguishes that failure using a real SWITCH record after the backup's End-LSN in the same final segment.

The probe captures fresh PG18 native inputs and retains the original manifest and archive WAL. It pads only the test bundle after End-LSN. Both inputs pass manifest-range verification, but only the archive includes the later SWITCH. The probe compares these controls:

| Control | Required observation |
| --- | --- |
| Full archive | Promotion, expected SQL, and replay endpoint at the SWITCH's logical end |
| Padded bundle returned as archive success | The same SQL can pass, but the replay endpoint is too early |
| Required archive absent with exit 255 | Fatal startup failure without promotion |
| Full archive after the negatives | The original healthy endpoint and SQL checks pass again |

The replay endpoint must come from the same PostgreSQL startup or the new timeline's history. `pg_last_wal_replay_lsn()` becomes NULL after a later normal restart. CNPG replays and stops PostgreSQL in its recovery Job, so an ordinary instance's insert or checkpoint LSN is not a substitute. Bind the history endpoint to the observed new timeline and source ancestry.

In the CNPG campaign, use a fresh product backup and confirm the real SWITCH occurs after End-LSN in that same archived file. Preserve the admitted archive frontier and native manifest ranges. Apply the same endpoint oracle to healthy and faulty recovery on fresh PVCs. Required absence, corruption, authentication, TLS, and transport faults must remain active through helper replay and terminal failure, not only a pre-start probe.

## Reproduce the native control

With matching PG18 tools and their library environment, run as a non-root user:

```sh
PG_BIN=/absolute/pg18/bin PG_SHARE=/absolute/pg18/share \
  python3 hack/s1_switch_probe.py
```

The probe uses private Unix sockets and writes native inputs, command logs, and cleanup results under `.work`. Its shell callback is test-only. This native control does not replace actual Go-helper, CNPG, MinIO, or shell-free-image tests. The separate [named-point fixture](fixtures/README.md) preserves a captured same-segment target without a timing-sensitive workload.

## PostgreSQL sources

References use PostgreSQL 18.6 at `724edf9bde9d356724ad384a2e196edc3c9f80f7`.

- [BACKUP_END precedes SWITCH](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlog.c#L9300-L9317).
- [SWITCH advances logical next LSN to segment end](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlogreader.c#L875-L910). Padding does not authorize that advance.
- [Replay publishes the applied record's end](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlogrecovery.c#L2034-L2041).
- [Replay LSN is NULL after a normal start](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/doc/src/sgml/func.sgml#L29234-L29239).
- [Recovery rereads the last applied record](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlogrecovery.c#L1527-L1552), and [timeline history records EndOfLog](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlog.c#L5986-L6021).
