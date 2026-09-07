# Third-party components

Original project work is all rights reserved; this is not a project license.

- Go 1.27.1 standard library/runtime: Go authors, BSD-style license. Builds copy
  the toolchain's LICENSE and PATENTS into both image roots. No external Go
  modules are currently linked into the executable; the production S3 package
  and its tests now use the modules below. Generated `go-linked.json` and
  `go-version.txt` describe the actual executable, not merely go.mod.
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

These foundation images are not release-qualified distributions. Before release,
PR J/K must bundle or otherwise satisfy each component's exact corresponding
source/redistribution obligations, including LGPL libraries, with the release
notices and source artifacts. Do not assume a package URL or this summary alone
satisfies those obligations. Ubuntu source packages are available via
https://archive.ubuntu.com/ubuntu/ and PGDG source packages via
https://apt.postgresql.org/pub/repos/apt/pool/main/p/postgresql-18/.
