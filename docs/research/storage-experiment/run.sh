#!/usr/bin/env bash
# Disposable loopback-only research harness. No container/system service required.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
command -v go >/dev/null
command -v openssl >/dev/null
scratch=$(mktemp -d)
pid=
cleanup() { if [[ -n "$pid" ]]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi; rm -rf "$scratch"; }
trap cleanup EXIT
mkdir -p "$scratch/certs" "$scratch/data"
if [[ -n "${EXPERIMENT_MINIO_BINARY:-}" ]]; then
 cp "$EXPERIMENT_MINIO_BINARY" "$scratch/minio"
else
 curl -fsSL --retry 2 https://dl.min.io/server/minio/release/linux-amd64/archive/minio.RELEASE.2025-09-07T16-13-09Z -o "$scratch/minio"
fi
printf '%s  %s\n' 7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f "$scratch/minio" | sha256sum -c -
chmod 700 "$scratch/minio"
"$scratch/minio" --version
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$scratch/ca.key" -out "$scratch/ca.crt" -days 1 -subj /CN=storage-experiment-ca >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -keyout "$scratch/certs/private.key" -out "$scratch/server.csr" -subj /CN=localhost >/dev/null 2>&1
printf 'subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth\n' > "$scratch/ext"
openssl x509 -req -in "$scratch/server.csr" -CA "$scratch/ca.crt" -CAkey "$scratch/ca.key" -CAcreateserial -out "$scratch/certs/public.crt" -days 1 -extfile "$scratch/ext" >/dev/null 2>&1
export MINIO_ROOT_USER=storage-experiment
export MINIO_ROOT_PASSWORD
MINIO_ROOT_PASSWORD=$(openssl rand -hex 24)
export EXPERIMENT_CA="$scratch/ca.crt"
export EXPERIMENT_ENDPOINT=127.0.0.1:19443
export MINIO_BROWSER=off
"$scratch/minio" server --address "$EXPERIMENT_ENDPOINT" --console-address 127.0.0.1:19444 --certs-dir "$scratch/certs" "$scratch/data" > "$scratch/minio.log" 2>&1 &
pid=$!
ready=false
for _ in $(seq 1 100); do
 if curl -fsS --cacert "$EXPERIMENT_CA" "https://$EXPERIMENT_ENDPOINT/minio/health/ready" >/dev/null 2>&1; then ready=true; break; fi
 if ! kill -0 "$pid" 2>/dev/null; then printf 'MinIO exited before readiness\n'; grep -v -E 'RootUser|RootPass|PASSWORD' "$scratch/minio.log"; exit 1; fi
 sleep .2
done
[[ "$ready" == true ]] || { printf 'MinIO readiness timed out\n'; exit 1; }
export CGO_ENABLED=0
go version
go mod download
go test -v -count=1 -timeout=180s ./...
go test -c -o "$scratch/experiment.test" ./...
go list -deps -f '{{if .CgoFiles}}{{.ImportPath}}: {{.CgoFiles}}{{end}}' ./... > "$scratch/cgo-packages"
[[ ! -s "$scratch/cgo-packages" ]] || { printf 'Unexpected CGO packages\n'; exit 1; }
go list -deps -test -json ./... > "$scratch/packages.json"
go list -deps -test -f '{{if .Module}}{{.Module.Path}} {{.Module.Version}}{{end}}' ./... | sort -u
printf 'PASS CGO_ENABLED=0 compile and package inspection\n'
