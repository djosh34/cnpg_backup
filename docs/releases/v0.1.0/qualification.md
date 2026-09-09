# v0.1.0 qualification decision

Subject identity: [trusted release record](../../../.github/release-subjects/v0.1.0.json).
The record deliberately retains the existing schema and `release_qualified=false`
from its original run. **The release decision is this explicit, independently
reviewed causal accounting, not a raw flag flip or a new full-run PASS.**
Publication remains conditional on fresh narrow K review and merge. No binaries,
images, runtime code, dependency pins or test scenarios change in K.

## Accepted causal recovery evidence

[Full run 34335186360/1](https://github.com/djosh34/cnpg_backup/actions/runs/34335186360)
remains **FAILED**: 36/37 families and 58/59 branches passed, teardown complete.
The sole failure was an already-owned retired PVC disappearing between list and
delete, after stale-RPC refusal, unchanged-target assertions, recovered SQL and
original process/lifetime completion. That failure did not establish per-target
reclaim or post-disposal source-holder equality.

The reviewed c9b2f3c harness correction tolerates only NotFound on that owned PVC
delete; UID, live-consumer, backing/mount, quiescence and other error checks remain.
Same-image execution `d6d20087d1e94775b862684843a30e38` selected only
`stale-tuple-rejected` and passed its actual RPC/SQL/quiescence checks, **real
reclaim, source-holder equality and target-retired-after-evidence**, with teardown.
Its raw `scope_passed=true`, `release_qualified=false` remain unchanged.
Manifest SHA256: `0addd8244cb16c0d77d58846bd152ac25143078e27ede4c5526c5fdba7739846`.

Two fresh J SPEC/KISS reviews accepted the unchanged 58 branches and the
specific distinguishing replay; the parent independently verified raw receipts.
[Review disposition](https://github.com/djosh34/cnpg_backup/pull/40#issuecomment-5601652641),
[causal decision](https://github.com/djosh34/cnpg_backup/pull/40#issuecomment-5601689089),
[parent verification](https://github.com/djosh34/cnpg_backup/pull/40#issuecomment-5601714083).
This closes the identified mandatory oracle gap; it is not arbitrary aggregation
of selected runs. `recovery-evidence.tar.gz` preserves the original failed full,
selected replay, raw flags, first failure, review reports and
`manager-stale-causal-supplement.json` without rewriting them.

## Other applicable gates (already executed)

- Exact-image security job **102413996937** in the full run: govulncheck, both
  Trivy scans, three SBOMs, native inventory/corresponding sources; gate PASS,
  no blockers. gRPC **1.83.2** fixes the real HIGH. Six exact SSH/OpenPGP
  absent-code dispositions retain their raw findings and built package closure;
  see [security dispositions](../../security-dispositions.md) and
  `security-evidence.tar.gz`. This is not a zero-findings claim.
- Source foundation/integration **34335186219 / 34335190696**, guard
  **34335186104 / 34335190641**, lifecycle smoke
  **34335186111 / 34335190629**: PASS on da4. Source smoke used separate
  smoke images: it is ordinary source/integration evidence, **not** canonical
  release-image qualification. `build-test-evidence.tar.gz` preserves producer,
  race, focused harness/DST and check receipts. Existing source remains unchanged.
- Native Go fuzz: eight existing targets ×150 seconds = **1200 seconds**,
  **2,829,310 executions**, all eight passed, clean source/input hashes before
  and after. NativeParentGraph recorded **9 executions**, not an inflated count.
  `fuzz-evidence.tar.gz`, `fuzz-corpus.tar.gz` and the source archive retain raw
  logs, corpus and checked-in minimized regressions. No rerun for packaging.
- Both operational families passed on the **final da4 images in the original
  full**, not merely on historical b408. Measured 223,346,688-byte fixture,
  309,592,064 transferred artifact bytes, 110.153-second capture; WAL callback
  **0.753 seconds** during sustained transfer, observed backlog 0→0.
  Manager idle Go RSS 38,662,144 bytes; transfer Go RSS max 36,065,280 bytes;
  transfer cgroup peak 1,279,340,544 bytes, restore peak 1,705,193,472 bytes,
  under 3 GiB with zero OOMs. Data workspace capacity 8,350,298,112 bytes,
  sampled transfer use max 931,205,120 bytes; one native work group maximum.
  Raw resource samples, finite-filesystem evidence, rotation overlap and SQL
  oracles remain in full-run events. These are sampled observations, not an
  unlimited database-size/RTO or manager-volume quota claim.
- Actual pre-release I→da4 rolling update restored retained full+differential+
  remote WAL after source namespace loss: rows `1:base,2:before`, matching
  differential hash `59998:4168c715fa2ee524bb900c18edd27905`, timeline 2;
  restore 78.047 seconds. Full-run fixture-014 archive SHA256
  `ef9e0f1c91dbe6e6e0ea1c73051312ff744f60f15748a391f7063d34b82f043f`
  (21,505,704 bytes, 43 objects) is retained inside recovery evidence.
  The earlier initial-format fixture is also published separately: SHA256
  `9da398658f18b4c519d4e4b7f6d98f4f29497558fd74d38a12e45aa3268da927`
  (21,388,950 bytes, 43 objects). Both have **pre-release I producers**, not a
  fictional previous release. First-release predecessor testing is **N-A**.

J merged as `e28e276a0efddf6e8747ab162ea18592c81f9102`; canonical J metadata
already matches the da4 subject. [Final J check disposition](https://github.com/djosh34/cnpg_backup/pull/40#issuecomment-5601756640)
retains canceled redundant CI as **canceled, not PASS**; both c9 guards actually
passed. Prior b408 failed scans/full/selected-only provenance failures remain
historical, not qualification of da4. Main Dependabot's incompatible unpinned
latest-k8s upgrade attempt is not a failure of the tested pinned graph and is
outside this release. No dependency upgrade work is included.

Unsigned release is the explicit [owner decision](https://github.com/djosh34/cnpg_backup/issues/25#issuecomment-5598041120):
no signing/attestation/verification jobs. **Checkmarx UNAVAILABLE / N-A; no scan**.
Exact tested manifests/binary, checksums, traceability and third-party obligations
remain mandatory. No production deployment.
