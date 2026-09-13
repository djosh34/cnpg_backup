# Build and test prerequisites

The build produces a static Go executable and two minimal container roots. [Testing](testing.md) lists profiles, and [recovery campaigns](recovery-campaign.md) explains testing already-published images without rebuilding them.

## Prepare the host

Use Linux amd64 and a non-root account. PostgreSQL refuses root. Basic harness tests need Bash, Python, and curl. Native builds need Python 3.12 or later with `dpkg-deb`, or Python 3.14 for rootless zstd package extraction. Host PostgreSQL execution also needs the corresponding ordinary runtime libraries. These host libraries are not inputs to the shipped images.

Use a short physical checkout path and disk-backed temporary space. PostgreSQL Unix sockets have a path-length limit. Allow roughly 3 GiB for build inputs plus space for outputs and the selected fixture. Docker with working daemon/socket access is required for image, guard, and kind profiles. Race tests need a C compiler. The harness does not install host packages or change services.

Check Docker networking, namespaces, cgroups, available memory, disk space, and inodes before provisioning. Run one local kind campaign at a time on a shared host. Do not disable host security or prune unrelated Docker resources to make a fixture pass.

## Run

```sh
./hack/test harness
./hack/test harness test_lifecycle_harness
./hack/test unit
./hack/test fast
./hack/test integration --seed 1806
./hack/test integration --seed 1806 --images
./hack/test guard
./hack/test cnpg-smoke
```

`harness` accepts unittest module, class, or method names. Its unfiltered run also checks metrics fixtures and CRD generation. `unit` adds Go and Promtool but does not build native roots. `fast` builds and checks roots without Docker. Full profiles run harness tests before expensive setup. Failures stop the profile.

If only native PostgreSQL diagnostics are possible, prepare tools with `fast`, then run:

```sh
python3 hack/recovery.py --seed 1806 --native-only
```

This diagnostic does not establish MinIO, image, or CNPG coverage.

## Use verified caches

`CNPG_BUILD_CACHE=/absolute/disk/path` relocates tool inputs from the default `.work/tools`. Downloads are checksum-verified even on cache hits. A mismatch fails rather than changing a pin. Share only trusted caches, never writable inputs from untrusted users.

`CNPG_TEST_TMP` selects a short disk-backed temporary directory. Otherwise the entry point respects `TMPDIR` and defaults to `.work/tmp`. Go also uses this directory through `GOTMPDIR`. Avoid tmpfs for database-sized work. Each build replaces `build/out`; do not run concurrent builds in one checkout.

The harness selects the pinned Go toolchain with `GOTOOLCHAIN=local`, `CGO_ENABLED=0`, Linux amd64, and bounded build parallelism. Race tests override CGO only for test execution.

## Build inputs and outputs

[build/inputs.lock.json](../build/inputs.lock.json) pins Go, PGDG packages, MinIO, and native library/CA packages by version, URL, and SHA256. [build/kubernetes-inputs.lock.json](../build/kubernetes-inputs.lock.json) pins Kubernetes tooling and upstream operator/database images. Module versions live in `go.mod` and `go.sum`. Other tool and source locks remain under `build/`.

`hack/build.py` builds the production executable twice with fixed revision, `-trimpath`, `-buildvcs=false`, and an empty build ID. Byte inequality fails. CGO dependencies, ELF `PT_INTERP`, and ELF `PT_DYNAMIC` fail the static Go check. OCI filesystem metadata can still vary by builder, so image qualification uses actual digests rather than assuming reproducible containers.

| Output | Contents |
|---|---|
| `go-linked.json` | Production executable package closure |
| `go-modules.json`, `go-version.txt` | Selected module graph and actual binary build information |
| `go-dependency-scopes.json` | Executable, production-package, and test dependency scopes |
| `native-files.json` | Six native tools, recursive ELF loader/library closure, hashes, and package attribution |
| `native-packages.json` | Packages represented by selected runtime files, not a claim that whole packages ship |
| `manager-files.json`, `pg18-files.json` | Complete prepared filesystem inventories |
| Image inspect/export records | Actual immutable image IDs and exported file/hash/mode comparison |

Native closure resolution uses only pinned package contents, not host libraries. Package maintainer scripts are never executed. Docker builds use scratch, prepared roots, and disabled build networking. No floating builder image or package resolver is involved.

Both images use UID/GID 26 and include CA data, identity files, inventories, and notices. The manager contains the static executable. The data image adds only six PostgreSQL client tools and their loader/library closure. Neither contains a shell, Python, database server, MinIO, compiler, scanner, or test actor. Run with read-only root, dropped capabilities, and no privilege escalation.

[Security packaging](security-packaging.md) covers actual-image SBOMs, corresponding native sources, scans, and redistribution obligations.

## Native integration oracle

`hack/recovery.py` starts a disposable PG18 primary on a private Unix socket and checksum-pinned MinIO on a random loopback port. Ephemeral credentials stay in private configuration and child environments, not command arguments or artifacts. Its test-only HTTP fixture does not relax production TLS requirements; production SDK tests separately exercise both signers and a private CA.

The seeded workload captures F, D1 from F, and D2 from the same F. It deletes D1 and capture copies, retrieves original inputs, verifies their hashes, reconstructs only F+D2, and starts PostgreSQL. Independent SQL checks cover updates, deletes, truncate, drop/recreate, whole-table digest, and absence of post-D2 writes.

A test-only Go verifier directly invokes `pg_verifybackup --no-parse-wal` and `pg_waldump` for every accepted range. With `--images`, it runs inside the actual shell-free data image without becoming a shipped tool. Negative controls reject corrupted originals, missing or damaged bundled WAL, and an absent later range. A separate control demonstrates that a new synthetic manifest can validate already-corrupted input, which is why original verification is mandatory.

Artifacts under `artifacts/` contain result manifests, versions, seed, replay commands, journals, command exits, hashes, assertions, and redacted logs. Success and failure evidence are collected. Raw fixture databases and credentials are removed only after owned PostgreSQL processes stop. A killed runner cannot guarantee final collection and never counts as success.
