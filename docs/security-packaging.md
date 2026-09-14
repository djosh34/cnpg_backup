# Security packaging reference

[Release policy](release-policy.md) defines publication requirements. `hack/build.py` inventories prepared roots, and `hack/security.py` scans existing product image digests. Neither a source-tree scan nor a successful build substitutes for final-image coverage. Release artifacts are unsigned.

## Subject identity and inventories

Security jobs consume an existing trusted source/image association. They verify repository, source ancestry, canonical registry digest, platform, UID, and source/revision labels. The executable extracted from both images must be identical. Input changes, including native attribution, notices, source locks, or build logic, require fresh candidate bytes and qualification.

| Record | Scope |
|---|---|
| `go-modules.json` | Full selected module graph, including test/tool dependencies |
| `go-linked.json`, `go version -m`, `go-dependency-scopes.json` | Actual executable linkage, separated from production-package and test scopes |
| Actual image file inventories | Every shipped file, hash, and mode |
| `native-files.json`, `native-packages.json` | Selected native closure and containing package versions, not whole installed packages |
| `binary.spdx.json`, `manager.spdx.json`, `pg18.spdx.json` | Syft SPDX SBOMs for the extracted binary and both actual images |

The build includes pinned Ubuntu `os-release` metadata and Debian Source name/version from authenticated binary-package control records. Trivy uses these to select the Ubuntu 24.04 feed and source-package advisories. Missing OS or package coverage fails rather than appearing as zero findings. Metadata does not add a shell or general OS userspace to the images.

## Corresponding native source

[build/native-sources.lock.json](../build/native-sources.lock.json) pins the exact `.dsc`, upstream archives, distribution patches, and associated signatures for the selected native packages. Security collection reads the subject revision's locks, verifies sizes/hashes and package identities, and produces `native-sources.tar` and its manifest. A different harness revision must not substitute its own dependency versions.

Release assets retain this source bundle, including distribution packaging/build instructions, alongside notices and license texts. Expiring CI artifacts or a package-mirror URL alone are insufficient. The build never executes package maintainer scripts.

Original project work is all rights reserved; upstream source and licenses are not subject to that reservation. Native LGPL libraries remain dynamically linked. Recipients can rebuild the container with replacement libraries using the corresponding sources, packaging inputs, and Dockerfile. Read-only-root runtime settings do not prevent rebuilding third-party components.

## Scanners and gates

[build/security-tools.lock.json](../build/security-tools.lock.json) pins tool inputs. The scanning tools are test/build dependencies, not runtime files.

| Tool | Function |
|---|---|
| govulncheck | Linked binary and symbol vulnerability analysis |
| Trivy | Both images, native and Go vulnerabilities, and secrets |
| Syft | Binary and final-image SPDX SBOMs |

Each scan downloads a fresh Trivy database. An `UpdatedAt` older than 48 hours fails. The scanner must positively identify the selected native and CA packages, their versions, and the linked Go binary. It retains tool/database metadata, complete vulnerability results, redacted secret findings, and gate results.

HIGH, CRITICAL, and UNKNOWN image vulnerabilities, secret findings, and linked Go findings block the automated gate unless a specific implemented disposition applies. Lower-severity findings remain in reports. No `--ignore-unfixed`, hidden ignore file, or disabled database update hides coverage gaps. [Current narrow dispositions](security-dispositions.md) require proof of absent executable packages, not a module-version waiver.

A disposition identifies the vulnerability, affected component/version, exact subject and report, scanner/database version, shipped file or package path, and concrete absence/backport/unreachability evidence. Changed bytes, call paths, supported configuration, or advisory applicability require reconsideration. Reachable defects cannot be excused merely by authentication or request-size limits.

Raw secret matches, registry credentials, image exports, and tool caches stay in private `.work` storage. Uploaded secret findings omit matching snippets. Collection is bounded, including 512 MiB for each export and the native source bundle. Failure collection remains separate from credential cleanup. Ordinary artifacts expire after 14 days; release assets retain the required evidence.

## Build a release candidate

From `main`, start the candidate workflow:

```sh
gh workflow run candidate.yml --ref main
```

The workflow builds and audits both images, publishes immutable candidate tags, and runs security and recovery checks against their digests. It refuses existing tags rather than replacing previously built bytes. Candidate images are not releases. Promote them only after the checks in [release policy](release-policy.md) pass.

## Run scans

Run the offline scanner regressions before a hosted scan:

```sh
./hack/test harness test_security
gh workflow run security.yml --ref main \
	-f subject_sha="$SUBJECT_SHA" -f trusted_ref=main \
	-f manager_image="$MANAGER_IMAGE" -f data_image="$DATA_IMAGE"
```

Use actual existing immutable references from a trusted publication. The workflow checks the subject and does not silently rebuild it. Test and scan jobs need read permissions; publication permissions belong only to publishing jobs. No privileged fork checkout or production credentials are involved.

## Update dependencies

Update versions and checksums from primary release sources. Inspect native package control/source identities, regenerate corresponding-source locks and notices when needed, and run the existing unit, build, image, and integration checks. Dependabot covers Go modules and Actions, while JSON input locks need explicit maintenance.

Scan the new actual image digests and review all findings. Run applicable recovery and resource qualification before promotion. New package bytes never inherit an old image's qualification. New database findings on unchanged bytes still require triage. Dependency updates do not deploy the product or automatically merge themselves.
