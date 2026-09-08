# Foundation build and native recovery harness (PR A)

This page describes the historical PR A build/test foundation, **not release
qualification**. Runtime features were added in subsequent PRs; its original
unsupported-mode state is not a description of current runtime capabilities.
For PR G's actual full/PITR acceptance, see [recovery-campaign.md](recovery-campaign.md).
That entry point consumes exact existing subject images and deliberately bypasses
the source-build pipeline documented below.

## Run

Start with the [local-first feedback ladder](testing.md#local-first-feedback). `harness` needs bash, Python and curl (disposable loopback HTTP fixtures), and runs fixture/oracle tests without tool downloads; `unit` adds the pinned Go toolchain and Promtool, not native inputs. Full profiles need Linux amd64, non-root, disk-backed checkout with roughly 3 GiB free, plus space for the chosen campaign. Needs bash,
curl, Python >=3.12 with dpkg-deb (Ubuntu hosted runner), or Python >=3.14 for
rootless zstd package extraction. Host PostgreSQL test execution also requires
ordinary distro libraries (recorded by ldd); these host dependencies are not
runtime-image inputs. Docker is needed for actual image/guard checks and the kind/CNPG profiles.
Production credentials/endpoints are never accepted. The harness itself does not
provision the host. Separately, the explicitly owner-authorized `local-runtime-4`
setup may install/configure supported runtime/native/compiler prerequisites;
this is not general permission to change unrelated services or bypass platform
security. Check sudo, namespaces/cgroups, netlink and shared resources first;
retain the first failure and stop at genuine privilege/kernel boundaries.

```sh
./hack/test harness
./hack/test harness test_lifecycle_harness  # optional unittest module/class/method
./hack/test unit
./hack/test fast
./hack/test integration --seed 1806             # real PG18.6 + real MinIO
./hack/test integration --seed 1806 --images    # CI acceptance profile
# Diagnostic only when local MinIO cannot execute; NOT MinIO acceptance:
# After ./hack/test fast has prepared tools/roots:
python3 hack/recovery.py --seed 1806 --native-only
# Exact-artifact CNPG full/PITR (requires integrated G images, no subject build):
./hack/test recovery-campaign --subject-sha <sha> \
  --manager-image ghcr.io/djosh34/cnpg-backup-manager@sha256:<digest> \
  --data-image ghcr.io/djosh34/cnpg-backup-pg18@sha256:<digest> \
  --profile recovery --seed 1806 --duration-minutes 120
```

`harness` also runs first inside every full profile/CI job, before tool downloads and builds, so fixture failures stop cheaply. Targeted `harness` arguments select unittest names; an unfiltered run includes metrics self-tests and CRD generation checks. `unit` runs the same Go tests/vet as `fast`; it does not claim native/image coverage. Every profile failure remains a failure.

`CNPG_BUILD_CACHE=/absolute/disk/path` optionally relocates verified tool inputs;
default `.work/tools`. Fixtures and Go temporary files stay under `.work`, **not
/tmp** (which may be tmpfs). Avoid very long checkout paths: PostgreSQL Unix
sockets have a length limit. Downloads are checksum-checked even on cache hits;
a mismatch fails, never silently replaces the pin. Do not share a writable cache
between untrusted users. `build/out` is replaced by each build. No system package
installation/maintainer scripts are executed by the harness. Authorized host
prerequisite installation is separate and must record versions, origins,
checksums/signature verification, services/resources and actual test results.
The only Docker builds use scratch and already prepared roots, with build networking disabled: no floating builder
image, apt resolver or Dockerfile frontend download.

## Authorized local host setup

On a disposable Fedora host with explicit provisioning authorization:

```sh
sudo -n id
sudo -n dnf install -y moby-engine gcc glibc-devel
sudo -n systemctl start docker
sudo -n docker info
```

Keep the harness non-root (PostgreSQL refuses root). If the current user lacks
Docker socket access, a private CLI wrapper uses existing passwordless sudo
without making the root-equivalent socket world-writable or requiring a login:

```sh
mkdir -p .work/runtime/bin
printf '#!/bin/sh\nexec sudo -n /usr/sbin/runuser -u %s -g %s -G docker -- /usr/bin/docker "$@"\n' "$(id -un)" "$(id -gn)" > .work/runtime/bin/docker
chmod 700 .work/runtime/bin/docker
export PATH="$PWD/.work/runtime/bin:$PATH"
export DOCKER_HOST=unix:///var/run/docker.sock
export GOMAXPROCS=2 GOFLAGS=-p=2
./hack/test integration --seed 1806 --images
# With the shared host's kind slot assigned to this run:
./hack/test cnpg-smoke
```

Use a short **physical** checkout path (for example `$HOME/cb`), not merely a
symlink; Python resolves checkout paths before creating PostgreSQL sockets.
Keep Git metadata: image/guard harnesses require the tested commit. The existing
CNPG harness downloads and verifies kind/kubectl from the lock file itself.
`CNPG_BUILD_CACHE` may reuse trusted verified inputs; do not delete caches used
by active runs. Installed GCC enables `CGO_ENABLED=1 CC=/usr/bin/gcc go test
-race ./...` with the pinned Go toolchain; production remains CGO-free.

Before a campaign, prove current daemon/network and MinIO readiness rather than
reuse an old privilege failure. Bound shared workloads and assign exclusive kind
ownership. Clean only owned resources; preserve failure artifacts before removing
a merged, inactive worktree. Do not disable seccomp, SELinux or other protections,
reset unrelated services, or prune a shared daemon indiscriminately.

The wrapper keeps CLI-created build/save files owned by the non-root user while
using the existing Docker socket group. Running the CLI itself as root can leave
kind unable to read its mode-0600 image archive. Preserve the user's primary
group as well as Docker access; buildx may restore ordinary directory ownership.
Earlier restricted-context sudo/netlink failures remain historical evidence,
not a reason to bypass protections or disregard current successful checks.

## Pins and dependency policy

`build/inputs.lock.json` locks the verified Go **1.27.1** archive, MinIO binary,
PGDG **18.6-3.pgdg24.04+1** packages and Ubuntu library/CA packages by URL/version/
SHA256. Ubuntu hashes were resolved from noble main/security/updates package
indexes and checked against every downloaded package. PG and MinIO pins match
the frozen research; Go was checked against the official download JSON. The
upstream CNPG database image digest is retained for later CNPG integration; this
native harness starts the pinned PGDG server directly instead of claiming that
upstream image was exercised. Builder-host Python/dpkg/kernel versions are
recorded, not represented as immutable hosted runner inputs.

`go.mod` pins the language/toolchain minimum; `GOTOOLCHAIN=local` and the exact
version check prevent silent toolchain selection. There are no external Go
modules yet, so no artificial empty go.sum. New runtime modules must be pinned,
CGO-free, justified by an actual caller and licensed/inventoried. No RDMA tag.
The production executable is built twice with `-trimpath`, `-buildvcs=false`, a
fixed revision and empty build ID; byte inequality fails. Separate scratch
filesystem metadata/OCI digests may vary with the builder; qualify the exact
resulting image, not an imagined reproducible digest. Race detection is a
separate test-only CGO exception, never a release compiler setting.

- `go-linked.json`: complete `go list -deps -json` production-package closure.
- `go-modules.json` and `go-version.txt`: selected modules and actual binary build
  info. CGO package presence fails; ELF PT_INTERP/PT_DYNAMIC also fail.
- `native-files.json`: six PG tools plus recursive ELF interpreter/DT_NEEDED
  closure, hashes and source-package attribution. Resolution never consults
  host libraries. Native library version execution is also checked locally.
- `native-packages.json` and narrowed `/var/lib/dpkg/status`: exact packages
  represented by selected runtime files (not claims that whole packages ship).
  `inputs.lock.json` separately inventories all build/test inputs.
- `manager-files.json`, `pg18-files.json`: complete prepared filesystem inventory.
  `--images` exports actual containers, compares every regular file/hash/mode to
  those roots, rejects unexpected links/special files, and saves actual image
  inspect/export inventories and immutable image IDs.

Both final images use UID/GID 26. Manager is scratch + static binary, CA,
identity, inventory and notices. The data image adds only `pg_basebackup`,
`pg_verifybackup`, `pg_combinebackup`, `pg_waldump`, `pg_controldata`, `psql` and
the pinned loader/library closure. No shell, Python, MinIO, database server,
compiler, test verifier, Barman or pgBackRest is copied. Use read-only root,
dropped capabilities and no-new-privileges when running them. Original work is
all rights reserved; see `LICENSE` and `THIRD_PARTY_NOTICES.md`. Exact upstream
notices/common-license texts are copied into both roots. Release source bundles,
SBOM formats, vulnerability scanning, provenance and publication remain PR J/K
obligations, not purportedly passed PR A qualification.

## Recovery oracle and evidence

One Python driver starts a disposable PG18.6 primary on a private Unix socket
(no PG TCP listener) and a checksum-pinned MinIO on a random **loopback-only**
port. Random ephemeral credentials stay in a mode-0600 curl configuration and
child environment, never command arguments/artifacts. Test-only curl SigV4
PUT/GET stores and retrieves each original F/D2 archive/manifest; downloaded
hashes must match. PR B adds the production SDK, SigV2 and TLS/private CA tests;
this loopback HTTP fixture is not a production TLS exception.

The seeded workload has 20,000 identifiable rows, updates/deletes, truncate and
drop/recreate transactions. It captures F, D1 from F, D2 from the **same F**;
deletes D1 and capture copies; verifies MinIO-downloaded originals; combines
only F+D2; verifies the synthetic output; starts a new PostgreSQL instance and
checks row count plus an independently computed whole-table digest, D1-era and
D2-era effects, truncate/recreate contents, and absence of post-D2 writes.

Every tar/original/synthetic verification calls the **test-only Go helper**,
which directly executes `pg_verifybackup --no-parse-wal` then `pg_waldump` for
**every** validated manifest range. With `--images`, the helper is mounted
read-only into the actual shell-free data image, never shipped. Negative controls:

1. Corrupt an original incremental payload: original verifier must reject it.
2. Combine those bad bytes anyway: new synthetic manifest can accept them;
   that output is never started. This distinguishes original authentication
   from synthetic consistency.
3. Remove required bundled WAL, then separately damage its header: direct WAL
   parser must reject each even though file verification succeeds.
4. Add a test-only checksummed second manifest range whose WAL is absent: the
   later direct parser must fail (no silent first-range-only loop).

The synthetic multi-range fixture tests iteration, not production primary-only
capture policy. Trusted fixture extraction/native orchestration here is not the
production hostile-input extractor or native process/workspace policy. The
research same-segment fixture was read but is not repackaged as qualification.
PITR beyond bundled WAL, tablespaces, CNPG, source-loss recovery, storage faults,
DST/fuzz campaigns, guard ownership and product helpers remain feature-specific
acceptance in the existing delivery plan.

`artifacts/recovery-*/` contains schema-1 result/version/pin/seed/replay data,
incremental JSONL events and acknowledged-transaction journal, command exits,
original manifest copies, artifact hashes/sizes, SQL assertions and redacted
PG/MinIO logs. Expected negative exits count as passing only with distinguishing
error assertions. Real scheduling and native backup bytes are not deterministic;
the seed fixes workload semantics, not a physical interleaving. Both success and
first failure evidence are retained. Raw databases/credentials are removed only
after owned PostgreSQL processes stop. CI uploads evidence with `always()` and
14-day retention. Cancellation cannot guarantee collection; job timeouts fail.

The `foundation` workflow runs fast and actual-image integration automatically,
with pinned Actions and read-only token permissions, no pushes/deployments/model
credentials. A green native diagnostic explicitly leaves MinIO and image gates
unexecuted; no profile is release-qualified.
