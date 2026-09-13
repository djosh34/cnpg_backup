# Security dispositions

`hack/security.py` applies only the following absent-code dispositions for `golang.org/x/crypto v0.55.0` in `usr/local/bin/cnpg-backup`.

| Finding | Package tree that must be absent | Primary advisory |
|---|---|---|
| CVE-2026-56855 | `golang.org/x/crypto/ssh` | [GO-2026-6355](https://vuln.go.dev/ID/GO-2026-6355.json) |
| CVE-2026-78662 | `golang.org/x/crypto/ssh` | [GO-2026-6354](https://vuln.go.dev/ID/GO-2026-6354.json) |
| GO-2026-5932 | `golang.org/x/crypto/openpgp` | [GO-2026-5932](https://vuln.go.dev/ID/GO-2026-5932.json) |

Every image must reconfirm absence using its producing executable package closure in `go-dependency-scopes.json`. Missing or empty closure, test-only inventory, an affected package or subpackage, a changed component/version/target, or another finding does not qualify. Raw findings remain in the report. The security `gate.json` records each applied disposition and actual package paths. Revisit these dispositions when advisory applicability broadens or at the next release security review.

There is no gRPC exception. The pinned gRPC v1.83.2 incorporates the [receive-buffer compaction fix](https://github.com/grpc/grpc-go/security/advisories/GHSA-vp52-pcj8-j9qc) and the [xDS version-match fix](https://github.com/grpc/grpc-go/security/advisories/GHSA-2v4p-qf9q-27wj). CVE-2026-84304 affects ordinary gRPC transport, including admitted mTLS and Unix peers. Authentication and receive-size limits do not remove that vulnerability. The upstream receive-buffer compaction default remains enabled.

[Security packaging](security-packaging.md) defines scanner coverage and release-blocking findings. These dispositions are not a general ignore list or a claim of zero findings.
