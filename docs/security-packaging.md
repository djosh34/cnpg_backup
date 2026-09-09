# Candidate security packaging (J)

This is the implemented scanner/build contract, **not a claim that J has passed
its final scans or that any release is qualified**. K owns v0.1.0 publication.
`release_qualified` remains false in candidate/security/recovery records.

## Subjects, inventory and corresponding source

The existing `pr-g-candidate.yml` publisher accepts only exact repository-owned
pushes to the enumerated delivery branches, including `implementation/pr-j`.
The security author's branch is not a publisher. Both immutable `sha-<commit>`
tags must be positively absent before building/publishing. Auth/network errors
never authorize overwriting a tag. Partial publication is preserved and blocks
rebuilding under that tag.

J optionally reads `.github/pr-j-subject.json` for **explicit** reuse. Missing
record means a new build; there is no implicit I-subject fallback. Ancestry,
canonical digests and unchanged known build inputs remain mandatory. Changes to
`hack/build.py`, native attribution, notices or locks require fresh images, even
if application Go is unchanged. A source SHA never qualifies rebuilt bytes.

`hack/build.py` retains the existing static Go check, six native tools, recursive
ELF closure and actual exported-image comparison. It now copies the locked
Ubuntu `os-release` metadata and writes Debian **Source name/version from the
authenticated binary package control record**, not a guess based on binary
package names. This lets Trivy select the Ubuntu noble vulnerability feed and
match source-package advisories. `Architecture: all` CA metadata is preserved.
These are metadata additions, not an OS userspace, scanner, shell or Python in
an image. Production remains CGO-free.

Inventories have distinct scopes:

- `go-modules.json`: full build/module graph (includes tooling/test dependencies).
- `go-linked.json`, executable package inventory, binary `go version -m`, and
  `go-dependency-scopes.json`: executable versus production/tests. A module in
  the full graph is not automatically shipped.
- Actual-image `*-files.json`, `native-files.json`, `native-packages.json`:
  selected native files/hashes and containing packages, **not whole packages**.
- Syft SPDX JSON for the extracted binary and **both actual registry digests**:
  machine-readable binary/final-image SBOMs, not a source-tree SBOM presented as
  runtime evidence. The binary extracted from both images must be identical.

`build/native-sources.lock.json` locks the exact `.dsc`, upstream archives,
Debian/Ubuntu patches and signatures listed by each corresponding source
package. Its identities were cross-checked against checksum-authenticated
`.deb` control records. `hack/security.py` downloads these files **on the hosted
scanner job**, verifies hashes and sizes, and archives the selected closure as
`native-sources.tar` with a source manifest. It reads the subject revision's
locks, not the harness revision's dependency versions. Missing source records
or mismatched package versions fail closed. No package scripts are installed
or executed. K must retain this bundle with release assets; expiring CI
artifacts and links to a package mirror alone do not satisfy source obligations.

Original project work remains **all rights reserved**. Unmodified third-party
notices and common-license texts remain in both roots. The source bundle
contains the distro packaging/build instructions as well as corresponding
library/tool source. Native LGPL libraries remain dynamically linked; recipients
can rebuild the container with their replacement libraries using the included
packaging inputs and existing Dockerfile. The runtime read-only-root setting is
not a prohibition on recipients rebuilding third-party components. Do not apply
the original-work reservation to upstream source or licenses.

## Scanning and signing

New combined-J publication calls `.github/workflows/security.yml`. Its manual
and reusable interfaces consume canonical existing digests; neither silently
rebuilds a subject. A diagnostic scan of I is possible but old I images lack J's
scanner-origin metadata: absent OS/package coverage fails, not a zero-findings
pass. Run from trusted main/J/I only; repository, event, ref, source ancestry,
actual registry digest, platform, UID and image source/revision labels are checked.

Pinned hosted tools (`build/security-tools.lock.json`):

| Tool | Pin | Role |
| --- | --- | --- |
| govulncheck | v1.8.0, module checksum verified via Go checksum database | Actual linked binary/symbol analysis |
| Trivy | 0.74.0, release tar SHA256 | Both final images, native/Go vulnerabilities and secrets |
| Syft | 1.51.1, release tar SHA256 | Binary and both final-image SPDX SBOMs |
| actions/attest | v4.2.1, full commit SHA | Ephemeral GitHub Actions OIDC/Sigstore provenance and SBOM signatures |

No scanner/compiler is copied into runtime roots. Tools, DB metadata, govulncheck
JSON (including its configuration/version/DB timestamp), full vulnerability
results, redacted secret findings and gate results are retained. Each hosted run
uses a fresh Trivy DB directory and downloads updates; DB `UpdatedAt` older than
48 hours fails. The scanner must positively identify Ubuntu 24.04, every selected
native/CA package version and the linked Go binary. No `--ignore-unfixed`, hidden
ignore file, skip-DB flag or zero-findings assertion hides coverage gaps.

HIGH/CRITICAL/UNKNOWN image vulnerabilities, any secret finding and linked Go
findings block the automated security gate. Lower severities remain reviewable
in complete reports. Secret code snippets/matches are removed before artifact
upload; raw matches, registry authentication, exports and tool caches remain in
private `.work`, not evidence. A failed security gate does not upload its binary
for signing. The publisher does not receive production/model/API credentials;
its token is only ephemeral repository package/attestation authority.

A scanner finding is not automatically an exploitable defect. Parent independent
review can disposition a specific false positive/unreachable finding with:

1. Exact subject digest and report hash, scanner and DB version/timestamp.
2. Vulnerability ID, package/version, shipped file/symbol/call path.
3. Concrete vendor fix/backport, absent shipped code, or unreachable-path evidence;
   why the reported exploit prerequisites cannot hold in the supported runtime.
4. Review resolution link and revisit conditions (new bytes, versions, call paths,
   scanner knowledge or supported configuration).

There is intentionally no blanket ignore list or automatic severity waiver.
If actual scans justify an automated exception, implement/test only that narrow
reviewed disposition against the exact subject/component, retaining the original
finding; do not switch off a scanner or fabricate a clean result. Actual reachable
high/critical defects, leaked credentials, unsafe extraction and unresolved
data-loss defects remain blockers. Checkmarx is currently **unavailable, not
passed** (no configured license/credentials/pipeline); no purchase gate is added.

Build provenance is signed **in the actual producing job**, for the exact binary
checksum and both published image digests. Reuse never creates false new build
provenance; retain the original producer's bundles. The separate successful scan
job supplies SBOMs to an OIDC-only attestation job. SBOM signing is a claim about
these scanned bytes, not a claim that the scanner job built them or that recovery
qualification passed. Portable Sigstore bundles are archived in both workflows.
Only producing jobs have packages-write or id-token/attestations-write; tests and
scans have read permissions. No `pull_request_target` or privileged fork checkout.

Verify downloaded bytes and signed identities, not merely an unsigned SBOM:

```sh
sha256sum --check SHA256SUMS
gh attestation verify cnpg-backup --repo djosh34/cnpg_backup \
  --signer-workflow djosh34/cnpg_backup/.github/workflows/pr-g-candidate.yml \
  --bundle binary-provenance.sigstore.json
gh attestation verify oci://ghcr.io/djosh34/cnpg-backup-manager@sha256:<digest> \
  --repo djosh34/cnpg_backup \
  --signer-workflow djosh34/cnpg_backup/.github/workflows/pr-g-candidate.yml
# SBOM bundles use --predicate-type https://spdx.dev/Document and signer
# djosh34/cnpg_backup/.github/workflows/security.yml (a reusable workflow).
```

Also compare the verified certificate source ref/revision to the recorded trusted
producer. A valid signature alone is not authorization for a different branch,
digest or release. GHCR verification/pulls may need read:packages login; K supplies
portable archives/bundles for registry-independent distribution.

## Run, retain and remediate

```sh
./hack/test harness test_security test_premerge_candidate
# After this workflow exists on the selected trusted branch:
gh workflow run security.yml --ref implementation/pr-j \
  -f subject_sha=<actual-product-sha> -f trusted_ref=implementation/pr-j \
  -f manager_image=ghcr.io/djosh34/cnpg-backup-manager@sha256:<digest> \
  -f data_image=ghcr.io/djosh34/cnpg-backup-pg18@sha256:<digest>
```

The publisher normally invokes that reusable scan automatically. Hosted scan
hard timeout is 60 minutes, signing 15; downloads, source bundle (512MiB),
exports (512MiB each) and artifact collection (768MiB) are bounded. Artifacts
retain 14 days, with failure collection independent of credential cleanup.
Local scanner DB downloads/builds compete with the ops author's 8GiB runtime
slot and require admission; offline negative controls need neither.

Dependabot proposes weekly Go module and pinned Action updates with a finite PR
limit. JSON tool/native/Go input locks require deliberate primary-release checks:
update versions/checksums, inspect corresponding source/control metadata,
regenerate native source pins and notices where necessary, then run the existing
harness/unit/build/image tests. Scan the **new actual candidate digests**, preserve
all findings/first failures, review reachable code and native advisories, and run
the applicable exact-image recovery/resource matrix before promotion. A new
security database finding on unchanged bytes still needs triage; a package update
never inherits old byte qualification. No auto-merge or production rollout is
configured by dependency updates.

The existing recovery workflow additionally accepts optional `prior_release`.
It must be a version tag, `trusted_ref=main`, and match the exact revision/images
in `origin/main:.github/release-subjects/<tag>.json`. The subject must be an ancestor
of that tag and the tag an ancestor of trusted main. No record/tag means failure,
not a fallback build. This interface creates no tag or release. v0.1.0 has no
predecessor; K records that N/A and establishes initial fixtures. Later releases
record their qualified subject once for this same digest-only recovery path.
