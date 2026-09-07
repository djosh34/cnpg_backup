# Planning review corrections: S1, S2/K1, R0

2026-09-07; scoped correction of reviewed base **bdcdb6ade386d8dde3115873b7a7a0851940f629**. Original accepted review IDs remain **S1**, **S2** (`/home/starlord/cnpg-run/reports/review-spec.txt`) and **K1** (`review-standards.txt`); **R0** is the parent's additional source finding. This records proposed corrections/evidence, not new independent review, READY or product qualification. No delegation, GitHub/main changes or product implementation. Owner answers remain binding; unavailable Checkmarx is not a gate. Parent's separate `2a201c1` changes were not incorporated or modified here.

## Corrected contracts and sources

- **S1:** PG18 [tries archive before local WAL](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlogrecovery.c#L4390-L4409). Authenticated absence of an archive duplicate may return **1** when the actual local bundle remains verified and no required remote interval intersects that segment. Required post-backup coverage is interval-based, bounded by selected manifest End-LSN and admitted archive frontier, not a filename-only set. Prefer verified archive contents; never return padded bundle bytes as archive success. Required remote absence, corruption and TLS/auth/transport failures remain **255**, including when bundled. Initial plans always attempt archive lookup, even immediate. PG still enforces target attainment. [Backup stop writes BACKUP_END before requesting a segment switch](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlog.c#L9300-L9317): other records can follow End-LSN in that same segment.
- **S2/K1:** [CNPG preflight precedes Restore RPC](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cmd/manager/instance/restore/restore.go#L72-L83) and can [rename/remove targets](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/management/postgres/initdb.go#L150-L194). The narrow Go main PID1 guard acquires locks plus durable markers before launching original CNPG argv; retains ownership through replay, all main descendants and the original sidecar's closed/drained target writes. Existing-socket local Begin/Drain binds Pod/guard/sidecar incarnations. Crashes/uncertainty leave markers, even after flock releases; retry uses fresh Cluster/all-fresh PVCs. Exact argv/probe/admission/termination/release sequence is in [CNPG §3](cnpgi-contract-final.md#target-ownership-before-the-main-command-ks2-correction). Linux foundations: [PID namespace init/reparenting/death](https://man7.org/linux/man-pages/man7/pid_namespaces.7.html), [kill namespace scope](https://man7.org/linux/man-pages/man2/kill.2.html), [flock descriptor lifetime](https://man7.org/linux/man-pages/man2/flock.2.html). No PID-death/timer/Pod-deletion takeover or sidecar-lock-only claim.
- **R0:** [pg_verifybackup L1200–1215](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/bin/pg_verifybackup/pg_verifybackup.c#L1200-L1215) calls `system()` for WAL parsing. **All** production tar/original/synthetic verification now specifies `--no-parse-wal` plus direct Go `exec.Command(pg_waldump, ...)` for **every validated manifest range**. No WAL verification is removed and no shell is added to the data image. Historical native research used the host shell on its plain-directory verifier path; its original script/hashes/results remain unchanged and are explicitly disclosed in the native report. PostgreSQL's restore-command shell runs in the existing CNPG main database image, not the plugin data image.

Design, all affected final reports, testing and PR D/F/G/H acceptance are reconciled.

## Actual commands and results

Read-only tools: PG18.6, Ubuntu **18.6-3.pgdg24.04+1**. Research uses Python and shell callbacks only for fixtures, not production code. Full direct argv/exit/output inventories and SQL assertions are retained in each PG run's `commands.json`, alongside `result.json`, PostgreSQL logs and callback request logs.

```sh
export PG_BIN=/tmp/cnpg-native-research/pgroot/usr/lib/postgresql/18/bin
export PG_SHARE=/tmp/cnpg-native-research/pgroot/usr/share/postgresql/18
export LD_LIBRARY_PATH=/tmp/cnpg-native-research/pgroot/usr/lib/x86_64-linux-gnu
python3 docs/research/experiments/recovery-review-pg.py
RECOVERY_FIXTURE=/tmp/recovery-review-pg-_db70vra \
  python3 docs/research/experiments/recovery-review-pg.py
python3 docs/research/experiments/recovery-owner-model.py
python3 -m py_compile docs/research/experiments/recovery-review-pg.py \
  docs/research/experiments/recovery-owner-model.py
git diff --check
```

**Both PG runs PASS:** deterministic default `/tmp/recovery-review-pg-7yxc74cz`; captured same-segment fixture `/tmp/recovery-review-pg-_aw0_may`.

| Distinguishing assertion (each run) | Observed |
|---|---|
| Intact bundle, callback archive miss 1, immediate target | Promoted; sentinel 1 present |
| Same intact bundle, incorrect fatal255 for duplicate miss | Startup rejected, FATAL/255 |
| Full archive preferred, named post-backup target | Promoted at target; default SQL rows 1,2 |
| Required archive unavailable, callback255 | Startup rejected, FATAL/255 |
| Incorrect bundle-as-archive-success instead of longer archive | Target not reached; startup rejected |
| Injected transport-class255 with intact bundle | Startup rejected, FATAL/255 |
| `pg_verifybackup --no-parse-wal` + direct subprocess `pg_waldump` | Both original and test input passed |
| Withhold required bundled WAL from direct parser | Parser rejected |

Default native range: **0/2000028–0/2000120**, target `postbackup_target` in later archived WAL. Same-segment captured range: **0/4057538–0/4401660**, timeline 1; real restore point **r6_7595** starts at **0/4401660** (its record extends beyond End-LSN) in **000000010000000000000004**. Full archive replay stopped at that point; the padded fixture could not. The original archive and bundle were identical, 16,777,216 bytes, SHA256 `dfc0178fd70e29855e5280e8c99141c73d661fc7e25553bcfb5b78c08f1af138`. The researcher **synthetically zeroed bytes from End-LSN to segment end**, preserving the manifest range; padded SHA256 `599f3dac56a6ceab16ebbf44e52382ade2e089e7f6fd5e13001e4a1e74ef312f`. This is controlled padding, **not naturally captured padding**. Original fixture, archive and WAL dump remain in `/tmp/recovery-review-pg-_db70vra`; final script replays it without a race workload. Default script requires no retained fixture.

**Ownership model/OS PASS:** paused main, delayed response, replay/shutdown block replacement mutation; sidecar crash/changed incarnation poison; pending writes prevent release; closed admission rejects stale tuples after clean drain. Actual separate Linux processes demonstrate live flock exclusion, clean release permitting preflight, and SIGKILL releasing flock while the fsynced marker still blocks preflight. `/tmp/recovery-owner-model-final.log` records the rerun and artifact directory.

## First failures and limits (not hidden by PASS)

- `/tmp/recovery-review-pg-33uf_051`: synthetic truncation exactly at End-LSN was unsuitable for the **immediate** positive control: PG needed another record to recognize that stop. Positive no-archive cases now use intact native bundles, not that synthetic fixture.
- `/tmp/recovery-review-pg-36hvlh23`: bounded natural same-segment race did not fire. A subsequent bounded capture produced the retained fixture; no race generation remains in the final script. `/tmp/recovery-review-pg-_db70vra` then hit temporary-file quota on its final clone. Only this task's stopped database copies were removed; logs/commands and selected input fixture were preserved. Final script removes stopped restore copies between cases.
- `/tmp/recovery-review-pg-oh_tmelh` and `-rd8xvmbv`: `pg_ctl` reported hot-standby readiness before promotion. The harness now waits a bounded five seconds for the actual SQL recovery-state oracle instead of assuming readiness means completion. Final runs passed without weakening that oracle. All owned PostgreSQL servers stopped; process listing found none remaining.
- This is local PG/filesystem evidence, **not actual S3 TLS/auth fault injection, Go helper/gRPC integration or CNPG guardian execution**. Ownership interleavings are a small model; actual OS evidence covers flock/marker/crash only, not PID1 descendant reaping, Kubernetes restartable-sidecar ordering, multi-PVC locks or real Begin/Drain. Those are mandatory PR D/G regressions.
- Direct-invocation checks here cover plain original/controlled full fixtures and missing WAL, not a newly executed tar/F+D/synthetic-output campaign or a shell-free product image. Historical tar/F+D evidence is retained; the corrected all-range/no-shell commands are mandatory PR F/H image acceptance. No qualification, READY or new owner approval is claimed.
