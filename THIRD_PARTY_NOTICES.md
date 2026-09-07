# Third-party components

Original project work is all rights reserved; this is not a project license.

- Go 1.27.1 standard library/runtime: Go authors, BSD-style license. Builds copy
  the toolchain's LICENSE and PATENTS into both image roots. Generated
  `go-linked.json` and `go-version.txt` describe the actual executable, not merely
  go.mod. External linked modules' license/notice texts are copied into
  `notices/go`; `go-runtime-modules.json` records exact versions and module sums.
- CNPG-I v0.6.0 (Apache-2.0), gRPC-Go (Apache-2.0), protobuf-go and Go extended
  libraries (BSD-style), and Google generated RPC bindings (Apache-2.0) supply
  the CNPG wire and local control services. The generated runtime inventory is
  authoritative for the transitive linked subset; dependency test libraries
  are not shipped.
- Kubernetes API/apimachinery/client-go v0.35.8 (Apache-2.0) supply typed Pod/Job
  objects, resource quantities and the uncached API client. Their actually linked
  dependency closure and notices are included by the same generated inventory;
  fake clients, Ginkgo/Gomega and JSON-patch regression tooling are test-only.
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

These foundation images are not release-qualified distributions. Before release,
PR J/K must bundle or otherwise satisfy each component's exact corresponding
source/redistribution obligations, including LGPL libraries, with the release
notices and source artifacts. Do not assume a package URL or this summary alone
satisfies those obligations. Ubuntu source packages are available via
https://archive.ubuntu.com/ubuntu/ and PGDG source packages via
https://apt.postgresql.org/pub/repos/apt/pool/main/p/postgresql-18/.
