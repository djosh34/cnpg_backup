# S1 SWITCH/latest distinguishing native regression

Scoped test-arrangement correction after the six ineffective named-point capture
attempts in candidate runs34152587311 and34153285181. Their failures remain
**fixture construction failures**, not product failures or waived coverage.
Independent conditional contract adjudication and the parent's native-evidence
disposition authorize this internal arrangement; design §5/issue#10 remain the
external contract. **These native results are not product acceptance.**

## Actual red/green

`hack/s1_switch_probe.py` creates fresh PG18.6 native tar/stream/spread full inputs,
never an old planning fixture. It retains original manifest/source WAL, pads only
the test bundle after unchanged EndLSN, and verifies both original and padded
inputs with `pg_verifybackup --no-parse-wal` plus direct `pg_waldump` ranges.
Three first native captures established the distinction; the portable checked-in
probe independently repeated **3/3**, adding a healthy-after-negative control.
Exact manifest/WAL hashes, SQL, history bytes and per-control verdicts are in
[evidence/s1-switch-latest.json](evidence/s1-switch-latest.json). Raw temporary
native archives are not checked in or counted as product inputs.

Every portable trial had real BACKUP_END ending at **E=0/2000120**, followed by a
real SWITCH starting at E in **000000010000000000000002**. No native boundary,
manifest or archive was fabricated. The switch's logical replay end is
**B=0/3000000**. Each recovery used ordinary latest, not an equality LSN selector.

| Same-input control | Actual result in all3 |
|---|---|
| Full archive preferred | Promoted; base SQL row present; same-postmaster replay LSN=B; new timeline2/source-parent1 history fork=B |
| Wrong padded bundle returned as archive-success | **Also promoted with identical SQL rows**, but replay LSN=E and history fork=E; same healthy endpoint oracle rejects |
| Required archive absence returning255 | Startup fails with FATAL/255; no promotion |
| Full archive again after negatives | Original healthy endpoint/SQL oracle passes |
| Later normal restart | `pg_last_wal_replay_lsn()` is NULL; history endpoint survives |

The initial scratch script collided its archive-directory and restore-directory
names before any replay. That first failure is retained locally as
`s1-switch-latest-first-failure-4.log`, separate from successful execution; the
checked-in probe uses separate names. No eventual-green race retry was used.

Reproduce with the pinned read-only native tools and their appropriate library
closure (no installed packages, root service, production endpoint or Docker):

```sh
PG_BIN=/absolute/pinned-pg18/bin PG_SHARE=/absolute/pinned-pg18/share \
  LD_LIBRARY_PATH=/absolute/pinned-compatible-libraries \
  python3 hack/s1_switch_probe.py
```

The script uses private Unix sockets, two CPUs, up to3 tiny captures, bounded
native/startup timeouts, and `.work` disk. It stops every owned server and retains
commands/results/native inputs there. The native callback is test-only shell;
it is not the shipped Go helper or a shell-free-image qualification.

## Why the oracle distinguishes

Pinned PostgreSQL `724edf9bde9d356724ad384a2e196edc3c9f80f7`:

- [xlog.c9300–9317](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlog.c#L9300-L9317)
  writes BACKUP_END then requests SWITCH.
- [xlogreader.c875–910](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlogreader.c#L875-L910)
  validates the actual record, then rounds SWITCH's logical next LSN to segment
  end. Zero padding is not a switch and does not authorize that advance.
- [xlogrecovery.c2034–2041](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlogrecovery.c#L2034-L2041)
  publishes the successfully applied record's end; SQL reports it.
- [func.sgml29234–29239](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/doc/src/sgml/func.sgml#L29234-L29239)
  documents NULL after a later normal start. CNPG replays/stops in its Job, so the
  normal instance's current insert/checkpoint LSN is **not** a substitute.
- [xlogrecovery.c1527–1552](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlogrecovery.c#L1527-L1552)
  rereads the last applied record;
  [xlog.c5986–6021](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlog.c#L5986-L6021)
  records EndOfLog in the new native timeline history. The actual tests above
  establish healthy B versus incorrect E, rather than assuming this endpoint.

## Actual campaign conditions (still require hosted execution)

Each candidate captures a **new product full** in supported CNPG layout with
original identities. Decode its actually archived final file; assert real SWITCH
start S>=E in that same filename. Fail an ineffective precondition rather than
silently substitute another test. Pad only the test bundle outside the native
range, reconciling only fixture artifact hashes. Actual product materialization
must reverify those inputs/ranges in the unchanged subject image.

Use explicit-backup/latest. Record the real admitted required interval covering
[E,B), not a fabricated or shortened frontier. If later archives are admitted,
the healthy endpoint must also reach their actual admitted frontier. Bind the
PostgreSQL-generated history to the observed new timeline and exact source
ancestry; pair it with actual CNPG completion and recovered SQL/promotion. Observe
the actual product-delivered archive hash before the faulty observer can replace
it. Apply the **same endpoint oracle** to healthy, wrong-bundle, and a subsequent
healthy control on fresh target PVCs.

Required same-file absence/corruption/auth/TLS/transport faults stay active
through real PostgreSQL helper replay and terminal255/no-promotion observation;
a manual helper probe cleared before startup is insufficient. All31 families,
other named/time/LSN/XID/immediate/sentinel/DROP cases, ordinary local fallback,
ownership and reader tests remain mandatory. H–K/release qualification are not
implied. The selected row-silent SWITCH is a WAL coverage witness, not a claim of
additional committed application rows.
