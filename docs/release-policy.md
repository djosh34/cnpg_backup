# Release policy

GitHub [versioned releases](https://github.com/djosh34/cnpg_backup/releases) are the canonical distribution record. The current release is [v0.1.0](releases/v0.1.0/README.md). Publication does not deploy the product into a consumer's production environment or grant a general project license.

## Artifacts and compatibility

Each release contains the static Linux executable, both portable OCI archives, checksums, install/example manifests, runbooks, dependency notices, corresponding native sources, SBOMs, source/build records, qualification evidence, and retained synthetic regression fixtures.

Registry destinations are `ghcr.io/djosh34/cnpg-backup-manager` and `ghcr.io/djosh34/cnpg-backup-pg18`. Candidates are immutable. Qualification consumes their exact digests, and version tags promote those same manifests without rebuilding. A published version tag is never reused for changed bytes. Install manifests pin digests, not floating `latest` tags.

GHCR visibility is independent of source visibility. Consumers may need `read:packages` authentication. Public release OCI archives provide a registry-independent path. Mirroring must preserve manifest digests rather than recompressing layers or wrapping a manifest in another index.

The published compatibility matrix defines support. There is no implicit support for other PostgreSQL majors, architectures, operator/Kubernetes versions, standby captures, or untested layouts. Compatible patch updates require updated pins and qualification. Support is not an SLA, fixed outage RPO, or unlimited database-size/restore-time promise.

Patch versions contain compatible fixes. Minor versions add features or explicitly documented pre-1.0 configuration breaks. Repository format compatibility is independent of package versioning. Unknown formats fail closed, with no implicit destructive migration. Later releases must recover retained prior-release full, differential, and WAL fixtures. v0.1.0 has no supported predecessor and establishes initial fixtures.

## Integrity and licensing

Artifacts are unsigned. No artifact/image signing, Sigstore/OIDC attestation, or signature-verification gate applies. Checksums detect altered downloads but do not independently authenticate their source. Consumers obtain them from the canonical release.

Source/build/run traceability and exact tested bytes remain mandatory. A source revision does not qualify rebuilt images. Source and image evidence are associated once in the release subject record, with a separate harness identity when needed.

Original project work is all rights reserved. Third-party licenses and notices remain applicable. PostgreSQL tools and their native libraries are the only runtime native exception. Production Go and its linked dependencies build with `CGO_ENABLED=0`. Compilers, test actors, scanners, Python, Barman, and pgBackRest are absent from final images. [Security packaging](security-packaging.md) describes inventories and corresponding-source obligations.

## Security requirements

Build inputs, Go modules/toolchain, container inputs, and Actions are pinned. Inventories distinguish full build dependencies from actual linked and shipped files. The binary and both actual images receive SBOMs, checksums, and current vulnerability scans.

`govulncheck` checks linked Go code. Trivy checks both final images, including native libraries and secrets, with recorded tool and database versions. Reachable high/critical defects, leaked credentials, exploitable extraction, and unresolved backup/restore/data-loss defects block release. Specific false-positive or absent-code findings can have evidence-backed dispositions with revisit conditions; blanket ignores cannot hide missing coverage.

Test workflows default to read permissions. Publication grants `contents: write` and `packages: write` only where needed. Signing-only token permissions are unnecessary. Untrusted PR code never executes with `pull_request_target` publication privileges. No production or model credentials belong in tests.

## Qualification requirements

[testing.md](testing.md) defines mandatory recovery and failure behavior. A release needs independent review, applicable source/unit/integration checks, fixed production-module simulation regressions, 20 minutes total Go fuzzing, exact-image recovery scenarios, prior-release compatibility where applicable, and actual security/resource evidence.

Resource measurements include:

- Manager idle Go RSS below 128 MiB, with default 50m CPU/64 MiB requests and 500m/256 MiB limits.
- Data Go RSS below 128 MiB idle and 256 MiB during transfer, measured separately from native children.
- Whole sidecar cgroup peak within the configured limit, at most 3 GiB, with OOM outcomes recorded. Default CPU limit is 2.
- One concurrent native operation, explicit finite per-filesystem capacity, raw/spooled/reconstructed bytes, workspace peaks, and restore timings.
- A durable WAL callback during sustained backup transfer larger than application buffers, with measured latency and backlog.

Requests and example workspace sizes are defaults, not measurements. Samples do not prove unlimited capacity or every transient peak. Recovery campaigns budget 120 minutes with a 150-minute hosted hard timeout. Mandatory work that does not finish remains incomplete, regardless of elapsed time.

Release assets retain concise qualification results, first failures and their dispositions, and minimized synthetic fixtures. Expiring runner logs are supplemental. A reviewed causal supplement must identify the exact failed obligation and distinguishing same-image replay without rewriting raw outcomes as a full pass. The [v0.1.0 record](releases/v0.1.0/qualification.md) documents such a decision.
