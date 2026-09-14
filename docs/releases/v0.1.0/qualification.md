# v0.1.0 qualification

The [published release](https://github.com/djosh34/cnpg_backup/releases/tag/v0.1.0) uses the exact images in the [release subject record](../../../.github/release-subjects/v0.1.0.json). Its raw `release_qualified=false` flag is preserved. The release decision combines the original evidence with a reviewed distinguishing replay; it does not claim a new full-run PASS.

This page summarizes the immutable release evidence. The original [published qualification asset](https://github.com/djosh34/cnpg_backup/releases/download/v0.1.0/qualification.md), evidence archives, and `SHA256SUMS` retain the complete records and their checksums.

## Recovery evidence

[Full run 34335186360](https://github.com/djosh34/cnpg_backup/actions/runs/34335186360) remains FAILED. It passed 36 of 37 families and 58 of 59 branches, with teardown complete. The failure was an owned retired PVC disappearing between list and delete, after successful stale-RPC refusal, unchanged-target assertions, recovered SQL, and original process/lifetime completion. It left per-target reclaim and post-disposal source-holder equality unproven.

The correction tolerates only NotFound on that already-owned PVC deletion. UID, live-consumer, backing/mount, quiescence, and other error checks remain. A same-image `stale-tuple-rejected` replay passed the RPC, SQL, quiescence, real reclaim, source-holder equality, and target-retired-after-evidence checks, with teardown. Its raw `scope_passed=true` and `release_qualified=false` remain unchanged.

The [review disposition](https://github.com/djosh34/cnpg_backup/pull/40#issuecomment-5601652641), [causal decision](https://github.com/djosh34/cnpg_backup/pull/40#issuecomment-5601689089), and [evidence verification](https://github.com/djosh34/cnpg_backup/pull/40#issuecomment-5601714083) accepted the unchanged branches and the specific replay. `recovery-evidence.tar.gz` retains both raw outcomes, the first failure, review records, and `manager-stale-causal-supplement.json`. This is a disposition of one identified oracle gap, not general aggregation of selected runs.

## Other gates

| Gate | Evidence and result |
|---|---|
| Security | Exact-image job 102413996937 in the full run passed govulncheck, both Trivy scans, three SBOMs, native inventory, and corresponding-source checks. Six applied SSH/OpenPGP absent-code dispositions across the images retain raw findings and executable package closure. No zero-findings claim |
| Foundation and integration | Runs 34335186219 and 34335190696 passed |
| Guard | Runs 34335186104 and 34335190641 passed |
| Lifecycle smoke | Runs 34335186111 and 34335190629 passed. Their separate smoke images provide source/integration evidence, not canonical release-image qualification |
| Go fuzzing | Eight existing targets ran 150 seconds each, 1,200 seconds total, with 2,829,310 executions and all targets passing. The native parent-graph target recorded nine executions |
| Operational tests | Both operational families passed on the final release images in the original full run, including resource behavior, credential/CA overlap, and SQL recovery |
| Compatibility | A pre-release producer's retained full, differential, and remote WAL recovered after rolling update and source namespace loss. There is no supported prior release, so predecessor-release testing is inapplicable |

`build-test-evidence.tar.gz`, `security-evidence.tar.gz`, `fuzz-evidence.tar.gz`, and `fuzz-corpus.tar.gz` retain the corresponding records. [Security dispositions](../../security-dispositions.md) require absence of affected executable packages. gRPC 1.83.2 fixes the reachable transport vulnerability; no gRPC exception applies. Canceled or failed runs remain canceled or failed, not passing evidence.

## Resource observations

The operational fixture contained 223,346,688 bytes. Capture transferred 309,592,064 artifact bytes in 110.153 seconds. A WAL callback completed in 0.753 seconds during sustained transfer, with observed backlog zero before and after.

| Measurement | Observed value |
|---|---:|
| Manager idle Go RSS | 38,662,144 bytes |
| Transfer Go RSS maximum | 36,065,280 bytes |
| Transfer cgroup peak | 1,279,340,544 bytes |
| Restore cgroup peak | 1,705,193,472 bytes |
| Data workspace capacity | 8,350,298,112 bytes |
| Sampled transfer workspace use maximum | 931,205,120 bytes |
| Native work groups | At most one |
| OOM events | Zero |

The pre-release compatibility recovery completed in 78.047 seconds and verified expected rows, differential hash, and timeline 2. Its fixture remains inside recovery evidence. The separately published `initial-v1-backup-wal.tar.gz` is also from a pre-release producer, not a fictional previous release.

These are sampled observations on finite disposable filesystems, not an unlimited database-size/RTO promise or proof of every transient peak. Single-node kind does not establish multi-node hardware resilience, physical power-loss behavior, or consumer CNI enforcement.

## Distribution limits

Artifacts are unsigned. Exact tested manifests, binary checksums, traceability, and third-party notices/sources remain mandatory. Qualification did not deploy the product into production. See the [support matrix](README.md#compatibility) and [release policy](../../release-policy.md).
