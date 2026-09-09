# Third-party components

Original project work is all rights reserved; this is not a project license.

- Go 1.27.1 standard library/runtime: Go authors, BSD-style license. Builds copy
  the toolchain's LICENSE and PATENTS into both image roots. Generated
  `go-linked.json` and `go-version.txt` describe the actual executable, not merely
  go.mod. `hack/godeps.py` inventories executable, production and test scopes
  separately and copies unmodified production module notices, including nested
  package licenses, into `notices/go-modules` in both image roots.
- CNPG-I v0.6.0 (Apache-2.0), gRPC-Go (Apache-2.0), protobuf-go and Go extended
  libraries (BSD-style), and Google generated RPC bindings (Apache-2.0) supply
  the CNPG wire and local control services. The generated runtime inventory is
  authoritative for the transitive linked subset; dependency test libraries
  are not shipped.
- Kubernetes API/apimachinery/client-go v0.35.8 (Apache-2.0) supply typed Pod/Job
  objects, resource quantities and the uncached API client. Their actually linked
  dependency closure and notices are included by the same generated inventory;
  fake clients, Ginkgo/Gomega and JSON-patch regression tooling are test-only.
- minio-go/v7 v7.3.0 (ce0e323c55c64964e6ad820ef0c6f5b286446aae):
  Copyright MinIO, Inc., Apache-2.0. Its unmodified LICENSE and NOTICE, and
  transitive production-package module license/notice texts, are copied to
  `go-notices/` and both image notice directories by `hack/godeps.py`.
  `go-dependency-scopes.json` records each exact version and whether it is used
  by production packages, tests, and/or the executable. The SDK is not linked
  artificially before repository/WAL callers land. The full module graph is
  separately recorded in `go-modules.json`; test-only dependencies are not
  automatically runtime dependencies. Source and checksum pins are in go.sum.
- PostgreSQL 18.6: PostgreSQL Global Development Group / Regents of the
  University of California, PostgreSQL License. The data image includes exactly
  six tools, not the database server. PGDG package copyright notices accompany
  the tools.
- The data image's dynamically linked native libraries have their own licenses
  (including LGPL, GPL with exceptions, OpenSSL/Apache, MIT and BSD variants).
  Generated `native-files.json` identifies the actual ELF closure; package
  versions, download URLs and hashes are in `inputs.lock.json`. Unmodified
  package copyright notices and referenced common-license texts are copied
  into `/usr/share/cnpg-backup/notices`. Notices for extra build-input packages
  do not mean their executable code is shipped.
- CA certificates come from the checksum-pinned Ubuntu ca-certificates package
  (Mozilla trust data); its notices accompany the generated bundle.
- MinIO is AGPLv3 test-only tooling, downloaded separately and never copied to
  either runtime image. Its pinned source is
  https://github.com/minio/minio/tree/07c3a429bfed433e49018cb0f78a52145d4bedeb.

These images are not release-qualified distributions. J's
`build/native-sources.lock.json` attributes binary packages to their exact
corresponding distro/PGDG sources, including packaging patches/build instructions.
The hosted security job verifies and bundles the actual selected closure's source
archives and .dsc files as `native-sources.tar`; K must preserve that bundle with
the release notices/assets, including LGPL source and replacement-library rights.
See [security packaging](docs/security-packaging.md). A package URL, expiring CI
artifact or this summary alone does not satisfy release redistribution obligations.
Ubuntu source packages originate at https://archive.ubuntu.com/ubuntu/ and PGDG
sources at https://apt.postgresql.org/pub/repos/apt/pool/main/p/postgresql-18/.
