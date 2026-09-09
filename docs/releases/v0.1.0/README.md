# CNPG backup v0.1.0 — unsigned Linux amd64 release

Canonical distribution: <https://github.com/djosh34/cnpg_backup/releases/tag/v0.1.0>.
Original project work is **all rights reserved**; publication grants no general
project license. Third-party notices and corresponding native sources accompany
this release. No production rollout is performed.

Read [qualification and limitations of the evidence](qualification.md) and the
[operations runbook](../../operations.md) before installation or recovery.
The source-image association is [recorded once](../../../.github/release-subjects/v0.1.0.json).
Version tags promote those existing tested manifest bytes, without rebuilding.

## Compatibility and boundaries

| Component | Qualified version |
|---|---|
| Platform | Linux amd64; PostgreSQL 8 KiB blocks / 1 GiB relation segments |
| PostgreSQL server / native tools | 18.6 |
| Kubernetes / CNPG / cert-manager | 1.35.8 / 1.30.0 / 1.21.1 |
| Go / gRPC / MinIO SDK | 1.27.1, CGO_ENABLED=0 / 1.83.2 / minio-go v7.3.0 |
| MinIO | RELEASE.2025-09-07T16-13-09Z; SigV2/SigV4 and private CA tested |
| Repository format | v1; first release, no supported predecessor |

Full and direct-full-parent differential capture on the primary, fresh-cluster
latest/PITR restore, separate WAL and CNPG-managed tablespaces are supported in
this matrix. Invalid differential prerequisites fail; **no full fallback**.
Unknown formats fail closed; no automatic destructive migration. Not qualified:
other architectures/PG majors/operator versions, standby capture, unmanaged
layouts, NFS/shared-writer targets, replica-cluster bootstrap or arbitrary S3
providers. Other S3 endpoints must satisfy the documented conditional-write,
strong-read/list and TLS contract. The single-node kind campaign does not prove
multi-node hardware resilience, physical power-loss tolerance, an SLA, fixed
fault RPO or an unlimited database-size/RTO. NetworkPolicy examples require an
enforcing consumer CNI and adapted addresses; kind is not isolation evidence.

## Distribution and installation

Download release assets and verify `sha256sum -c SHA256SUMS` in their directory
(all named assets must be present). Checksums detect changed downloads; these
artifacts are intentionally **unsigned**. Obtain them from the canonical release.
`cnpg-backup-linux-amd64` is the exact shipped static executable. The two
`*.oci.tar` files contain the exact tested manager/PG18 manifests and blobs.

Extract `install-and-runbooks.tar.gz` in an empty directory. It contains:

- `install.json`: rendered manager, RBAC, mTLS/discovery and metrics resources,
  pinned to both qualified plugin digests, managing namespace `database`.
- `config/`: Repository CRD, digest-pinned database Cluster and full/scheduled
  backup examples, recovery renderer, retention/monitoring/network examples.
- `recovery-example.json`: rendered fresh `recovered` Cluster, source `source`,
  destination `recovered-destination`; no reused target PVC identities.
- `prerequisites/pinned-{cert-manager,cnpg-1.30.0}.yaml`: upstream manifests
  verified against `build/kubernetes-inputs.lock.json`, with every selected
  operator image replaced by that lock's immutable digest, as in the harness.
- `docs/`: install, retention/protection, fresh recovery, rotation, update and
  monitoring runbooks, support/security details; `build/*.lock.json` and module
  pins provide exact tool/native/dependency inventory references.

Review the runbook and customize endpoints, lineage UUIDs, Secret/CA selectors,
finite-workspace storage classes and capacities before applying examples.
The rendered Secret allowlist matches the runbook's `database`/`recovered`
example names. Prerequisites require an existing compatible Kubernetes cluster;
apply cert-manager and wait for it, then CNPG and wait for it. Create the consumer
namespace/Secrets/CA, install the Repository CRD and `install.json`, then apply
adapted Repository/Cluster examples in runbook order. No credentials are shipped.
Do not apply the network example unchanged or use ambient production contexts.

GHCR may require consumer authentication with **read:packages** even though
source/releases are public. Configure manager Pod `imagePullSecrets` and CNPG
Cluster `imagePullSecrets` as needed. Public portable release OCI archives are
the registry-independent alternative: import/mirror with an OCI-capable tool
(e.g. `skopeo copy --preserve-digests oci-archive:<archive> docker://<mirror>:v0.1.0`).
Verify the destination manifest digest equals the release record; refuse tools
that recompress or wrap the single manifest in a different index. Change registry
names in install configuration only while preserving tested manifest digests.
Consumer credentials and deployment are not release-publisher/owner steps.

## Durable assets

- `binary.spdx.json`, `manager.spdx.json`, `pg18.spdx.json`: actual SBOMs.
- `third-party-notices.tar.gz`, `native-sources.tar`, `native-sources.json`:
  notices/licenses and exact corresponding native source obligations.
- `source-and-regressions.tar.gz`: product source at da4 plus the merged J
  harness source and its checked-in synthetic regressions; no release rebuild.
- `recovery-evidence.tar.gz`, `build-test-evidence.tar.gz`,
  `security-evidence.tar.gz`, `fuzz-evidence.tar.gz`, `fuzz-corpus.tar.gz`:
  original evidence, raw failed/selected-only outcomes and accepted causal
  decision, current scans/dispositions and actual fuzz workload.
- `initial-v1-backup-wal.tar.gz`, `initial-v1-fixture.json`: retained synthetic
  initial-format fixture; producer is pre-release I, not a previous release.
- `release-subject.json`, `qualification.md`, `SHA256SUMS`: subject, explicit
  release decision and integrity inventory. The raw subject's false flag is
  intentionally preserved; read qualification.md rather than relabeling a run.

For future updates retain these fixtures and verify old full/differential/WAL SQL
recovery with the new exact images. Never clear uncertain markers/holders by time
or Pod deletion; retry poisoned targets with a new Cluster and all-new PVCs.
