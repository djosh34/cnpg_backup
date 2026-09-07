#!/usr/bin/env bash
# Disposable research, not product code/qualification. Requires PG18.6 tools,
# Python 3.12+, bash, GNU coreutils. No TCP listener, root, containers or secrets.
# PGBIN=/absolute/path/bin bash native-workflow.sh /absolute/new/work-directory
set -euo pipefail
umask 077
: "${PGBIN:?set PGBIN to matching PG18.6 binaries}"
export PATH="$PGBIN:$PATH" LC_ALL=C
work=${1:?absolute new experiment directory required}
[[ $work = /* && ! -e $work && $work != *[\ \']* ]] || exit 1
[[ $(id -u) != 0 ]] || { echo 'run as non-root' >&2; exit 1; }
[[ $(postgres --version) = *' 18.6 '* || $(postgres --version) = *' 18.6' ]] || exit 1
mkdir -p "$work"/{socket,archive,source-ts}
exec > >(tee "$work/results.log") 2>&1
source_data=$work/source
restore_data=$work/restored
cleanup() {
  for data in "$restore_data" "$source_data"; do
    if [[ -f $data/postmaster.pid ]]; then pg_ctl -D "$data" -m immediate -w stop || true; fi
  done
}
trap cleanup EXIT
export PGHOST="$work/socket" PGPORT=55486 PGUSER="$(id -un)" PGDATABASE=postgres
sql() { psql -X -v ON_ERROR_STOP=1 -At "$@"; }
expect_fail() {
  local log=$1 pattern=$2; shift 2
  if "$@" > "$log" 2>&1; then echo "UNEXPECTED SUCCESS: $*"; exit 1; fi
  grep -E "$pattern" "$log"
}
initdb -D "$source_data" --waldir="$work/source-wal" --no-locale --encoding=UTF8 --data-checksums > "$work/initdb.log"
printf "\nlisten_addresses = ''\nunix_socket_directories = '%s'\nport = 55486\nwal_level = replica\nsummarize_wal = on\nwal_summary_keep_time = '30d'\nmax_wal_senders = 5\nmax_replication_slots = 5\narchive_mode = on\narchive_command = 'test ! -f %s/%%f && cp %%p %s/%%f'\n" "$PGHOST" "$work/archive" "$work/archive" >> "$source_data/postgresql.conf"
pg_ctl -D "$source_data" -l "$work/source.log" -w start
sql -c 'SELECT version(); SHOW data_checksums; SHOW wal_segment_size;'
sql -c 'CREATE ROLE backup LOGIN REPLICATION;'
sql -c "CREATE TABLESPACE extra LOCATION '$work/source-ts';"
sql -c "CREATE TABLE t (id integer PRIMARY KEY, v text); INSERT INTO t SELECT i, repeat(md5(i::text), 32) FROM generate_series(1,20000) i; CREATE TABLE ts (id integer PRIMARY KEY, v text) TABLESPACE extra; INSERT INTO ts VALUES (1,'full'); CHECKPOINT;"
tsoid=$(sql -c "SELECT oid FROM pg_tablespace WHERE spcname='extra'")
backup() {
  local name=$1; shift
  pg_basebackup -h "$PGHOST" -p "$PGPORT" -U backup --no-password -D "$work/$name-tar" -F tar -X stream --checkpoint=fast --manifest-checksums=SHA256 "$@"
  pg_verifybackup --exit-on-error --no-parse-wal "$work/$name-tar"
}
backup F
sql -c "UPDATE t SET v='d1' WHERE id=1; UPDATE ts SET v='d1'; CHECKPOINT;"
backup D1 --incremental="$work/F-tar/backup_manifest"
sql -c "UPDATE t SET v='d2' WHERE id=2; UPDATE ts SET v='d2'; CHECKPOINT;"
backup D2 --incremental="$work/F-tar/backup_manifest"
expect_fail "$work/tar-wal-rejected.log" 'WAL parsing is not supported|no-parse-wal' pg_verifybackup "$work/D2-tar"
# Extract only self-generated local regular/directory archives. Explicitly
# reject links: tablespace links are manufactured from our own trusted paths.
extract() {
  python3 - "$work" "$1" "$tsoid" <<'PY'
import pathlib, shutil, sys, tarfile
w, name, oid = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
p = w / name
p.mkdir()
def unpack(src, dst):
    dst.mkdir(parents=True, exist_ok=True)
    with tarfile.open(src) as t:
        for m in t:
            assert m.isfile() or m.isdir(), (m.name, m.type)
            t.extract(m, dst, filter='data')
unpack(w / (name + '-tar') / 'base.tar', p)
unpack(w / (name + '-tar') / 'pg_wal.tar', p / 'pg_wal')
ts = w / (name + '-ts')
unpack(w / (name + '-tar') / (oid + '.tar'), ts)
(p / 'pg_tblspc' / oid).symlink_to(ts)
shutil.copyfile(w / (name + '-tar') / 'backup_manifest', p / 'backup_manifest')
PY
  pg_verifybackup --exit-on-error "$work/$1"
}
extract F
extract D2
# Capture can verify only the extracted WAL, without extracting all data.
python3 - "$work" <<'PY'
import json, pathlib, subprocess, sys
w = pathlib.Path(sys.argv[1])
for name in ('F', 'D2'):
    m = json.loads((w / name / 'backup_manifest').read_bytes())
    for r in m['WAL-Ranges']:
        subprocess.run(['pg_waldump', '--quiet', '--path=' + str(w / name / 'pg_wal'),
                        '--timeline=' + str(r['Timeline']), '--start=' + r['Start-LSN'],
                        '--end=' + r['End-LSN']], check=True)
print('STANDALONE_WAL_RANGE_VERIFY_PASS')
PY
# Prove the original manifest verifies native INCREMENTAL.* bytes, not only
# their synthetic reconstruction. Alter one byte without changing file size.
python3 - "$work/D2" "$work/corrupt-path" <<'PY'
import pathlib, sys
p = next(p for p in sorted(pathlib.Path(sys.argv[1]).glob('base/*/INCREMENTAL.*')) if p.stat().st_size > 8192)
pathlib.Path(sys.argv[2]).write_text(str(p))
b = p.read_bytes(); p.write_bytes(b[:-1] + bytes([b[-1] ^ 1]))
PY
expect_fail "$work/corruption-rejected.log" 'checksum mismatch' pg_verifybackup --exit-on-error "$work/D2"
# Negative control: combine is not input integrity verification. A freshly
# computed synthetic manifest can bless corrupt incremental payload bytes.
pg_combinebackup --copy --manifest-checksums=SHA256 -T "$work/D2-ts=$work/corrupt-output-ts" -o "$work/corrupt-output" "$work/F" "$work/D2"
pg_verifybackup --exit-on-error "$work/corrupt-output"
echo 'NEGATIVE_CONTROL: combine + synthetic verify accepted corrupt input; original input verification is mandatory'
python3 - "$work/corrupt-path" <<'PY'
import pathlib, sys
p = pathlib.Path(pathlib.Path(sys.argv[1]).read_text())
b = p.read_bytes(); p.write_bytes(b[:-1] + bytes([b[-1] ^ 1]))
PY
pg_verifybackup --exit-on-error "$work/D2"
walfile=$(find "$work/D2/pg_wal" -maxdepth 1 -type f -name '00000001*' | head -1)
mv "$walfile" "$work/withheld-wal"
expect_fail "$work/missing-bundled-wal.log" 'could not find|could not open|WAL parsing failed' pg_verifybackup --exit-on-error "$work/D2"
mv "$work/withheld-wal" "$walfile"
# D1 is not supplied, extracted or otherwise used by reconstruction.
pg_combinebackup --copy --manifest-checksums=SHA256 -T "$work/D2-ts=$work/restored-ts" -o "$restore_data" "$work/F" "$work/D2"
pg_verifybackup --exit-on-error "$restore_data"
# pg_combinebackup copies the original tablespace_map. Verify it first, then
# remove it: startup must keep our trusted mapped pg_tblspc symlink, not recreate
# the source tablespace path. Move WAL only after combine, which skips WAL links.
rm "$restore_data/tablespace_map"
mv "$restore_data/pg_wal" "$work/restored-wal"
ln -s "$work/restored-wal" "$restore_data/pg_wal"
# Establish a post-backup archive-only PITR boundary. The target WAL is not
# bundled in D2, and SQL after the named point must not be visible on restore.
sql -c "SELECT pg_switch_wal(); INSERT INTO ts VALUES (2,'before-target');"
target=$(sql -c "SELECT pg_create_restore_point('native_target')")
target_file=$(sql -c "SELECT pg_walfile_name('$target'::pg_lsn)")
sql -c "INSERT INTO ts VALUES (3,'after-target'); SELECT pg_switch_wal();"
for i in $(seq 1 100); do [[ -f $work/archive/$target_file ]] && break; sleep 0.1; done
[[ -f $work/archive/$target_file && ! -f $work/D2/pg_wal/$target_file ]]
printf 'POST_BACKUP_TARGET lsn=%s segment=%s\n' "$target" "$target_file"
pg_ctl -D "$source_data" -m fast -w stop
: > "$restore_data/recovery.signal"
printf "\narchive_mode = off\nrestore_command = 'cp %s/%%f %%p'\nrecovery_target_name = 'native_target'\nrecovery_target_action = 'promote'\n" "$work/archive" >> "$restore_data/postgresql.conf"
pg_ctl -D "$restore_data" -l "$work/restore.log" -w start
for i in $(seq 1 100); do [[ $(sql -c 'SELECT pg_is_in_recovery()') = f ]] && break; sleep 0.1; done
[[ $(sql -c 'SELECT pg_is_in_recovery()') = f ]]
[[ $(sql -c "SELECT string_agg(id::text || ':' || v, ',' ORDER BY id) FROM ts") = '1:d2,2:before-target' ]]
[[ $(sql -c "SELECT string_agg(v, ',' ORDER BY id) FROM t WHERE id IN (1,2)") = 'd1,d2' ]]
[[ $(sql -c "SELECT pg_tablespace_location($tsoid)") = "$work/restored-ts" ]]
[[ $(readlink "$restore_data/pg_wal") = "$work/restored-wal" ]]
sql -c "SELECT 'RESTORE_SQL_PASS', pg_is_in_recovery(); SELECT * FROM ts ORDER BY id;"
pg_ctl -D "$restore_data" -m fast -w stop
# Missing local summaries must fail a new native differential, never fallback.
# Start first, let the summarizer catch up, then remove an already summarized
# interval. It does not rewind to repair past coverage for a new backup.
pg_ctl -D "$source_data" -l "$work/source.log" -w start
sql -c 'CHECKPOINT; SELECT pg_switch_wal();'
for i in $(seq 1 100); do
  [[ $(sql -c 'SELECT summarized_lsn >= pg_current_wal_insert_lsn() FROM pg_get_wal_summarizer_state()') = t ]] && break
  sleep 0.1
done
find "$work/source-wal/summaries" -type f -name '*.summary' -delete
expect_fail "$work/missing-summaries.log" 'WAL summaries are required.*(no summaries|incomplete)' pg_basebackup -h "$PGHOST" -p "$PGPORT" -U backup --no-password -D "$work/missing-summary-tar" -F tar -X stream --checkpoint=fast --incremental="$work/F-tar/backup_manifest"
pg_ctl -D "$source_data" -m fast -w stop
# Checksum transition is policy-rejected using control metadata; native tools
# alone are not a fail-closed policy for all checksum transition directions.
pg_controldata "$source_data" | grep 'Data page checksum version'
pg_checksums --disable -D "$source_data"
pg_controldata "$source_data" | grep 'Data page checksum version'
python3 - "$work" <<'PY'
import json, pathlib, sys
w = pathlib.Path(sys.argv[1])
for name in ('F', 'D1', 'D2'):
    m = json.loads((w / (name + '-tar') / 'backup_manifest').read_bytes())
    sizes = {p.name: p.stat().st_size for p in sorted((w / (name + '-tar')).iterdir())}
    print(name, 'WAL-Ranges=', m['WAL-Ranges'], 'artifact_bytes=', sizes,
          'incremental_files=', sum('/INCREMENTAL.' in f.get('Path', '') for f in m['Files']))
PY
printf 'PASS: full, two full-reference differentials, input corruption detection, F+D2-only SQL PITR, mapped tablespace/separate WAL, missing-summary failure; checksum transition observed.\n'
printf 'Evidence retained in %s; all disposable servers stopped by trap.\n' "$work"
