#!/usr/bin/env bash
# Research-only, rootless PGDG Ubuntu binaries on x86_64 Linux. No installation.
# Usage: local-tools.sh /absolute/new/tools-directory
set -euo pipefail
umask 077
root=${1:?absolute new tools directory required}
[[ $root = /* && ! -e $root ]] || { echo 'need absolute nonexistent directory' >&2; exit 1; }
[[ $(uname -m) = x86_64 ]] || exit 1
mkdir -p "$root/downloads" "$root/root"
fetch() {
  curl --fail --silent --show-error --location "$1" -o "$root/downloads/$2"
  printf '%s  %s\n' "$3" "$root/downloads/$2" | sha256sum --check
}
pg=https://apt.postgresql.org/pub/repos/apt/pool/main/p/postgresql-18
fetch "$pg/postgresql-18_18.6-3.pgdg24.04%2B1_amd64.deb" server.deb 611dc088b63d5bef3d63917832a00874b6f938e6942425abde818e90d5da3957
fetch "$pg/postgresql-client-18_18.6-3.pgdg24.04%2B1_amd64.deb" client.deb 635c8cbad024be8d1208433ec902cf721ae77028e7d6b7e0231a2768ba4dacfa
fetch "$pg/libpq5_18.6-3.pgdg24.04%2B1_amd64.deb" libpq.deb 29452c26315aeefeb900895b20b3e8adbd33c32cb9b602a83c30ac5cea1c2055
fetch https://archive.ubuntu.com/ubuntu/pool/main/i/icu/libicu74_74.2-1ubuntu3.1_amd64.deb icu.deb c9a70989678660eed9a1e904c74fa043da8bec8e2036856fc16e31ced79b04f8
fetch https://archive.ubuntu.com/ubuntu/pool/main/libu/liburing/liburing2_2.5-1build1_amd64.deb uring.deb c2aef62accee92a06263c3ad4ef46132c13e409b54296c44792a20491830a7b0
# Extraction only, never installation or package-script execution. Ubuntu's
# dpkg-deb handles zstd data.tar members; Python gained that support in 3.14.
if command -v dpkg-deb >/dev/null 2>&1; then
  for package in "$root/downloads/"*.deb; do
    dpkg-deb --extract "$package" "$root/root"
  done
else
# Fedora fallback requires Python >=3.14. These pinned package archives are
# research tooling, not the product's hostile-backup extraction implementation.
python3 - "$root" <<'PY'
import io, pathlib, sys, tarfile
if sys.version_info < (3, 14):
    sys.exit('package extraction requires dpkg-deb or Python >=3.14 (zstd support)')
root = pathlib.Path(sys.argv[1])
for package in sorted((root / 'downloads').glob('*.deb')):
    data = package.read_bytes()
    assert data[:8] == b'!<arch>\n'
    pos = 8
    while pos < len(data):
        header = data[pos:pos + 60]
        size = int(header[48:58])
        name = header[:16].decode().strip().rstrip('/')
        body = data[pos + 60:pos + 60 + size]
        pos += 60 + size + size % 2
        if name.startswith('data.tar'):
            with tarfile.open(fileobj=io.BytesIO(body)) as archive:
                archive.extractall(root / 'root', filter='data')
PY
fi
printf 'export PGBIN=%q\nexport LD_LIBRARY_PATH=%q\n' "$root/root/usr/lib/postgresql/18/bin" "$root/root/usr/lib/x86_64-linux-gnu" > "$root/env.sh"
source "$root/env.sh"
tools=(postgres initdb pg_basebackup pg_combinebackup pg_verifybackup pg_waldump pg_controldata pg_checksums psql pg_ctl)
missing=0
for tool in "${tools[@]}"; do
  if absent=$(ldd "$PGBIN/$tool" | grep 'not found'); then
    printf '%s missing runtime dependencies:\n%s\n' "$tool" "$absent" >&2
    missing=1
  fi
done
[[ $missing = 0 ]] || exit 1
for tool in "${tools[@]}"; do "$PGBIN/$tool" --version; done
printf 'Source %s/env.sh, then run native-workflow.sh. No services were installed.\n' "$root"
