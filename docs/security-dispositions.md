# J security remediation and narrow absence dispositions

Actual b408 scan [34332345048](https://github.com/djosh34/cnpg_backup/actions/runs/34332345048) is retained as failed. Independent adjudication found **CVE-2026-84304 reachable in ordinary gRPC transport**, including admitted malicious mTLS/Unix peers. Authentication and receive-size limits do not fix it. b408 is not releasable.

The authorized minimum correction selects **gRPC v1.83.2** (v1.83.1 fixes receive-buffer compaction; v1.83.2 also fixes the xDS version match), with only its required transitive changes. Leave the upstream default receive-buffer compaction **enabled**: do not set `GRPC_GO_EXPERIMENTAL_ENABLE_RECEIVE_BUFFER_COMPACTION=false`. New immutable images must pass actual security/recovery evidence; old image qualification does not transfer.

The scanner implements only these independently adjudicated absent-code identities for `golang.org/x/crypto v0.55.0` in `usr/local/bin/cnpg-backup`:

| Finding | Required absent package tree | Primary advisory |
| --- | --- | --- |
| CVE-2026-56855 | `golang.org/x/crypto/ssh` | [GO-2026-6355](https://vuln.go.dev/ID/GO-2026-6355.json) |
| CVE-2026-78662 | `golang.org/x/crypto/ssh` | [GO-2026-6354](https://vuln.go.dev/ID/GO-2026-6354.json) |
| GO-2026-5932 | `golang.org/x/crypto/openpgp` | [GO-2026-5932](https://vuln.go.dev/ID/GO-2026-5932.json) |

Every new image must reconfirm absence from its producing executable package closure, retained in its existing `go-dependency-scopes.json`. Missing/empty closure, test-only inventory, affected package/subpackage, changed component/version/target or other finding does not qualify. Raw findings remain; `gate.json` records each applied disposition and the actual package paths. This is not a module-version-only waiver or general ignore/VEX system. Revisit if advisory applicability broadens or at the next release security review.

Primary gRPC fixes: [receive-buffer advisory](https://github.com/grpc/grpc-go/security/advisories/GHSA-vp52-pcj8-j9qc), [xDS panic advisory](https://github.com/grpc/grpc-go/security/advisories/GHSA-2v4p-qf9q-27wj). No gRPC exception is installed. Signing remains removed under the owner’s J/K override; scans, exact digests/checksums and third-party obligations remain mandatory.
