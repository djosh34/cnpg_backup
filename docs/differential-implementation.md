# Native differential backups (PR H)

Request `backupType: differential` on a primary-target CNPG Backup or
ScheduledBackup. There is no fallback option. The newest eligible committed
**original full** is selected under the existing backup holder. `request.json`
freezes that root for the Backup UID; retries cannot switch to a newer full or
an intervening differential. Durable winners still replay without a live source.

The native caller compares PG18.6/system/timeline/checksum, capture Pod UID and
postmaster continuity, reference age and the existing physical/settings checks.
Missing actual summary coverage is PostgreSQL's error from the one
`pg_basebackup --incremental=<exact original manifest>` command, never a request
to retry as full. Postflight and commit-last checks remain the F protocol.

Restore retains G's protected catalog/plan, target guard and PITR helper. It
accepts only F or F+D. Both original archives are scanned and verified before
combination; their separately extracted originals are verified again. Native
ordinary-copy combine maps **D's extracted tablespace link targets**, not source
paths or F's links. Synthetic output is verified before the existing trusted
map removal, cross-volume WAL copy/hash/fsync and recovery transforms.
Every verifier uses `--no-parse-wal` and direct matching `pg_waldump` over the
validated range. No shell, runtime CGO or incremental-header parser is added.

A real local distinguishing test found that PG18.6 combine lists copied WAL in
its synthetic manifest **without a file checksum**. The synthetic scanner admits
only that precise `pg_wal/<24-hex-segment>` exception; original manifests still
require SHA256. Direct WAL parsing and final selected-bundle hashes remain
mandatory. The initial failed test and fixture are retained in H's report.

Capacity checks reserve both stored/raw download peaks, both extracted inputs,
verification WAL, allocation overhead and free margin on the finite workspace.
The native output is capped by `maxRestoredBytes`, monitored while combining and
checked before verification. Since there is no native output-size oracle, each
PGDATA/tablespace filesystem conservatively reserves the whole output maximum
plus metadata and margin; separate WAL reserves its configured maximum too.
This may reject a layout with insufficient per-volume capacity rather than
assume a differential is small. The mounted finite filesystems remain the hard
allocation stops. Only private scratch is cleaned; failed targets remain under
the unchanged guard uncertainty rules.

Capture/restore logs report reserved capacity and transfer inventory bytes.
Backup result metadata reports exact committed `rawBytes` and `storedBytes`,
including the original manifest, for both fresh results and replays. Existing
manager metrics/Warning/alert machinery already tracks both requested types:
H enables real differential successes and failures without a second metric
ledger, new labels or altered freshness semantics.

Local native component tests exercise F+D1+D2 (both directly F), delete D1,
reconstruct only F+D2, verify original corruption rejection and synthetic WAL,
and check update/delete/truncate/drop-recreate plus whole-data hash. They are
feedback, **not** CNPG/MinIO or immutable-image acceptance. Current exact-image,
real campaign and independent-review gates are recorded in the H delivery
report; I/J/K and release qualification are not part of H.
