# CNPG backup v0.1.0

[v0.1.0](https://github.com/djosh34/cnpg_backup/releases/tag/v0.1.0) was published on September 9, 2026 for Linux amd64. It is an unsigned release, not a draft or prerelease. The [release subject record](../../../.github/release-subjects/v0.1.0.json) identifies the exact binary revision and tested manager/data manifest digests. Version tags promote those existing bytes without rebuilding.

Read the [qualification evidence and limitations](qualification.md) and [operations runbook](../../operations.md) before installation. Original project work is all rights reserved. Publication grants no general project license. Third-party notices and corresponding native sources accompany the release.

## Compatibility

| Component | Qualified version |
|---|---|
| Platform | Linux amd64, PostgreSQL 8 KiB blocks and 1 GiB relation segments |
| PostgreSQL server and native tools | 18.6 |
| Kubernetes, CNPG, cert-manager | 1.35.8, 1.30.0, 1.21.1 |
| Go, gRPC, MinIO SDK | 1.27.1 with CGO disabled, 1.83.2, minio-go v7.3.0 |
| MinIO | RELEASE.2025-09-07T16-13-09Z, SigV2/SigV4 and private CA tested |
| Repository format | v1, no supported predecessor |

The matrix supports primary full and direct-full-parent differential capture, fresh-cluster latest/PITR recovery, separate WAL, and CNPG-managed tablespaces. Invalid differential prerequisites fail without a full fallback. Unknown formats fail closed without automatic migration.

Other architectures, PG majors, operator versions, standby capture, replica-cluster bootstrap, unmanaged layouts, NFS/shared-writer targets, and arbitrary S3 providers are not qualified. Other endpoints must meet the [conditional-write, consistency, and TLS requirements](../../repository.md#storage-requirements). Single-node kind does not prove multi-node hardware resilience, physical power-loss tolerance, CNI enforcement, an SLA, or a fixed fault RPO.

## Download and install

Download assets from the canonical release and run this command with every listed asset present:

```sh
sha256sum -c SHA256SUMS
```

Checksums detect changed downloads. These artifacts are unsigned, so obtain both the assets and checksum file from the canonical release. `cnpg-backup-linux-amd64` is the shipped static executable. The two `*.oci.tar` files contain the exact tested image manifests and blobs.

Extract `install-and-runbooks.tar.gz` into an empty directory. It contains digest-pinned `install.json`, configuration examples/renderers, `recovery-example.json`, runbooks, dependency locks, and pinned upstream cert-manager/CNPG prerequisite manifests.

Follow the [installation order](../../operations.md#install-the-plugin). Customize endpoint, lineage UUIDs, Secret and CA selectors, finite-workspace StorageClass, and all volume capacities before applying examples. The rendered manager manages namespace `database` and allowlists Secrets for the `database` and `recovered` examples. No credentials are shipped. Do not apply the network-policy example unchanged.

If GHCR requires authentication, use consumer `read:packages` credentials and configure manager and CNPG Cluster `imagePullSecrets`. Alternatively, mirror the public OCI archives with an OCI-capable tool:

```sh
skopeo copy --preserve-digests oci-archive:<archive> docker://<mirror>:v0.1.0
```

Verify the destination manifest digest against the release record. Do not use a tool that recompresses layers or wraps the single manifest in a different index. Change registry names in installation configuration only while preserving tested digests.

## Asset reference

| Assets | Contents |
|---|---|
| `binary.spdx.json`, `manager.spdx.json`, `pg18.spdx.json` | Actual binary and image SBOMs |
| `third-party-notices.tar.gz`, `native-sources.tar`, `native-sources.json` | Licenses, notices, and exact corresponding native sources |
| `source-and-regressions.tar.gz` | Product and test-tool source with synthetic regression fixtures |
| `recovery-evidence.tar.gz` | Original failed full-run evidence, distinguishing same-image replay, and accepted qualification decision |
| `build-test-evidence.tar.gz`, `security-evidence.tar.gz` | Build, integration, scan, and disposition records |
| `fuzz-evidence.tar.gz`, `fuzz-corpus.tar.gz` | Actual fuzz workload and corpus |
| `initial-v1-backup-wal.tar.gz`, `initial-v1-fixture.json` | Retained synthetic initial-format fixture with a pre-release producer |
| `release-subject.json`, `qualification.md`, `release-guide.md`, `SHA256SUMS` | Identity, qualification, installation, and integrity records |

The original run's `release_qualified=false` remains unchanged. [qualification.md](qualification.md) explains the reviewed release decision rather than presenting that run as a full pass. Published asset bytes and checksums remain fixed even when checkout documentation improves.

Retain the fixtures for future old-to-new full/differential/WAL recovery tests. Never clear uncertain markers or holders by time or Pod deletion. Retry poisoned targets with a new Cluster and all-new PVCs.
