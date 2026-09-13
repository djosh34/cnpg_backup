# Testing reference

[./hack/test](../hack/test) is the local and CI entry point. MinIO is the required integration backend. Real-system profiles use disposable PostgreSQL 18, CNPG, and Kubernetes fixtures, never production endpoints or credentials. [Build prerequisites](build-and-harness.md) and [recovery campaign commands](recovery-campaign.md) describe setup.

## Local-first feedback

Run the cheapest test that distinguishes the change before an expensive image or cluster campaign.

| Command | Coverage and prerequisites |
|---|---|
| `./hack/test harness` | Python fixture/oracle tests, loopback curl fixtures, metrics self-test, CRD generation check. No tool downloads or Docker |
| `./hack/test harness test_lifecycle_harness` | Selected unittest module, class, or method |
| `./hack/test unit` | Harness plus pinned Go unit tests, regression corpus, deterministic simulations, vet, and Promtool. No native build |
| `./hack/test fast` | Unit profile plus native input, static build, dependency, and prepared image-root checks |
| `./hack/test integration --seed 1806` | Actual host PG18 and MinIO, both SDK signers/private CA, native F+D reconstruction and SQL checks |
| `./hack/test integration --seed 1806 --images` | Integration plus actual shell-free images and exported filesystem checks |
| `./hack/test guard` | Real Docker PID 1/private-namespace ownership tests |
| `./hack/test cnpg-smoke` | Actual kind, CNPG, MinIO, lifecycle, WAL, and backup smoke tests |
| `./hack/test race` | Separate race-detector job with test-only `CGO_ENABLED=1` and a C compiler |
| `./hack/test fuzz-smoke` | Short native manifest/archive fuzzing |
| `./hack/test campaign-focused` | Repeated focused completion, drain, actor, and fixture regressions |

For a Go change, first run its package/test directly with the pinned toolchain, then the applicable profiles. Harness tests run before expensive setup. A Python mock is not Kubernetes evidence, and native-only recovery is not MinIO or CNPG coverage. A profile reports only its executed scope.

Production builds and dependency validation use `CGO_ENABLED=0`. The race detector's C toolchain does not authorize CGO in shipped code. MinIO and other unavailable integration tools cause explicit unexecuted coverage, not an inferred pass.

## Deterministic simulation

Simulations execute actual repository, WAL, and retention orchestration against controlled storage durability and response delivery. The seed, operation count, and implementation define replay length, not elapsed wall time. Storage completion ordering is explicit. Map-derived operations are sorted. Independent oracles inspect durable bytes and dependencies rather than calling production selection or retention twice.

Required failure classes include remote success with lost response, duplicate and differing-content retry, interrupted multipart, incomplete publication, partial lists, corruption, cancellation, restart, and restore/retention interleavings. Restart discards process state but retains fake durable objects. Tests check that no incomplete backup becomes visible, WAL is never acknowledged early, retained plans keep their parents and required WAL, and uncertain owners prevent deletion.

Retention tests exercise the actual runner through complete and slow inventories, late list failures, valid-empty orphan cleanup, ambiguous destructive responses, admission races, batching, and conclusive release. Generated fork graphs independently check replay intervals. Negative controls remove required fork WAL or introduce dishonest acknowledgments to prove that the oracle detects the failure class.

Safety under uncertainty differs from progress after prerequisites recover. Lost holders are not expected to expire. Test-only clocks and schedulers do not add a runtime lease service or production fault-control endpoint. `testing/synctest` does not make arbitrary external I/O deterministic.

## Real recovery oracle

The real-system campaign consumes existing product image digests. It generates a seeded single-writer SQL workload and retains its transaction journal outside the source database. Ambiguous commit outcomes are resolved or excluded from oracle facts. LSN barriers, named points, and PostgreSQL timestamps establish ordering instead of guessed sleeps.

Each recovery uses a fresh Cluster and PVCs. SQL assertions require pre-target effects present and post-target effects absent, plus correct role, timeline, and target attainment. Ready, RestoreResponse, and `pg_combinebackup` exit status alone cannot pass recovery. Latest tests compare against a known durable archive barrier, not unarchived commits.

Faults must have observed preconditions and receipts, such as active upload, established backlog, paused replay, or a requested WAL filename. A fault that never fired is not covered. Kernel and network scheduling are not deterministic, so real-system reproduction needs the event/fault trace as well as the seed.

## Required safety coverage

The executable scenario registry is [hack/campaign_plan.py](../hack/campaign_plan.py). The following obligations also have package and native component regressions.

- Full capture, two differentials from one full, deletion of the unused differential, and full-plus-selected-differential SQL reconstruction. Missing summaries, parent, continuity, or supported settings must fail a differential without a full fallback.
- Original-input corruption rejection before combine. Shell-free `pg_verifybackup --no-parse-wal` plus direct `pg_waldump` rejects missing or damaged WAL in every accepted range.
- WAL replay beyond the backup bundle, including post-EndLSN records in the same final bundled filename. Required archive absence/corruption fails recovery. Verified local bundle fallback gets exit 1 only when no required remote interval depends on that archive segment. TLS/auth/transport errors remain exit 255 with a bundle present.
- Time, LSN, named-point, XID, immediate, and latest selection, with inclusive/exclusive boundaries and earlier-base selection when the newest is too new. A deliberate DROP followed by pre-DROP PITR demonstrates SQL inclusion/exclusion.
- Source namespace/catalog loss with recovery from repository configuration alone, then destination WAL publication into a different UUID. Failover exercises history and supported timeline paths.
- Guard ownership before CNPG preflight on PGDATA, WAL, and every tablespace. Busy or poisoned replacements cannot rename, delete, or reconstruct targets. Guard/sidecar crashes, detached descendants, delayed callbacks, incarnation mismatch, pending writes, pause, and clean drain use real PID-namespace tests and CNPG replay.
- Backup and WAL response loss, process death, multipart interruption, repeated callbacks, MinIO restart/outage, resets, and slow transfers. WAL retains independent capacity during sustained artifact transfer. Recovery source errors cannot cause false latest promotion.
- Retention preserves full-parent and WAL dependencies, window anchors, minimum roots, and the last usable full. Tests race destructive requests with admission, pause and crash readers, restart the manager, and attempt fresh cross-cluster recovery. Only the original process releases its reader, and only proven terminal completion releases the stable lifetime holder.
- Finite workspace exhaustion, actual cgroup OOM and cancellation, per-filesystem reservations, tablespaces, separate WAL, credential and CA overlap, and no secret leakage. Tests use bounded filesystems rather than filling the host root disk.
- Per-type success freshness, terminal failure counting, Warning events, unknown history, and schedule-aware alerts. Failed uncommitted backups do not advance success. Lost-response durable commits can coexist with failed invocations.
- Rolling updates and recovery of retained older full, differential, and WAL bytes. The first release has no supported predecessor; its initial fixtures establish the next release's compatibility tests.

## Fuzzing

Go fuzz targets cover native manifests, archive paths and extraction, parent graphs, WAL/history names, compressed input bounds, and recovery selection. Extraction tests attempt escapes and verify that nothing outside the destination changes. Minimized failures become checked-in regression inputs. A fuzz duration is a search budget, not deterministic replay; the failing input is the reproduction artifact.

Release qualification requires 20 minutes total across the named targets, the fixed production-module simulation corpus, recorded exploration, and every applicable real-system scenario. A short fuzz-smoke run does not satisfy the release budget.

## Evidence and pass criteria

Each run records its subject revision and actual image digests once, with a distinct harness identity if different. It records version pins, seed, operation count, profile, phase timing, resource limits, selected/executed/blocked cases, fault receipts, SQL assertions, and reproduction commands. Checksums remain required for backup inputs, fixture bytes, and delivered artifacts. Dirty builds are diagnostic and cannot qualify a release.

Required failures remain failures. Missing prerequisites block dependent cases. Optional bounded forensic collection errors are diagnostics, but missing SQL, WAL, identity, holder, ownership, or teardown evidence cannot pass an oracle. Preserve the first failure before reset or disposal. A later successful run does not silently erase it.

Campaigns budget 120 minutes with a 150-minute hosted hard timeout and reserved collection/teardown time. Incomplete mandatory coverage is not qualified. Ordinary Actions artifacts expire after 14 days. Release evidence and minimized synthetic fixtures persist in release assets. No credentials, kubeconfigs, private keys, raw production data, or unrelated sessions belong in artifacts.

The [release policy](release-policy.md) also requires exact-image security, resource evidence, and independent review. A workflow file, elapsed test budget, or source-only green check does not qualify rebuilt image bytes. Single-node kind evidence does not establish multi-node hardware resilience, physical power-loss behavior, or CNI enforcement.
