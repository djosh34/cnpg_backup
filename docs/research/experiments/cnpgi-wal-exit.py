#!/usr/bin/env python3
"""Disposable PG18 negative control; no CNPG, gRPC or S3 runtime is simulated.

PG_BIN=/absolute/pg18/bin [PG_SHARE=/absolute/share/postgresql/18]
[LD_LIBRARY_PATH=...] python3 docs/research/experiments/cnpgi-wal-exit.py
Uses only fresh private temporary directories and Unix sockets; never production.
Artifacts are retained at the printed path. All started servers are stopped.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

BIN = Path(os.environ["PG_BIN"]).resolve()
ROOT = Path(tempfile.mkdtemp(prefix="cnpgi-exit-"))
SOCKET = ROOT / "socket"
SOCKET.mkdir()
PORT = "65431"  # Private socket directory; TCP is disabled.
servers = []


def run(tool, *args, check=True):
    return subprocess.run([str(BIN / tool), *map(str, args)], check=check,
                          text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def sql(query):
    return run("psql", "-X", "-A", "-t", "-h", SOCKET, "-p", PORT,
               "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c", query).stdout.strip()


def stop(data):
    run("pg_ctl", "-D", data, "-m", "immediate", "-w", "stop", check=False)


def start(data, logfile, check=True):
    servers.append(data)
    return run("pg_ctl", "-D", data, "-l", logfile, "-t", "20", "-w", "start", check=check)


print("artifacts:", ROOT, flush=True)
try:
    version = run("postgres", "--version").stdout.strip()
    if " 18." not in version:
        raise RuntimeError("requires PostgreSQL 18: " + version)
    source = ROOT / "source"
    init_args = ["-D", source, "-A", "trust", "--no-locale"]
    if os.environ.get("PG_SHARE"):
        init_args += ["-L", os.environ["PG_SHARE"]]
    print(run("initdb", *init_args).stdout, flush=True)
    with (source / "postgresql.conf").open("a") as f:
        f.write(f"\nlisten_addresses=''\nport={PORT}\nunix_socket_directories='{SOCKET}'\n"
                "wal_level=replica\nmax_wal_senders=5\nmax_replication_slots=5\n"
                "archive_mode=off\n")
    start(source, ROOT / "source.log")
    sql("CREATE TABLE sentinel(id integer PRIMARY KEY); INSERT INTO sentinel VALUES (1)")
    seed = ROOT / "seed"
    run("pg_basebackup", "-h", SOCKET, "-p", PORT, "-D", seed,
        "-X", "stream", "-c", "fast")
    # Complete the base segment and preserve its full source version, not a
    # potentially zero-padded bundled copy. The NEXT segment is the outage.
    sql("SELECT pg_switch_wal()")
    archive = ROOT / "archive"
    archive.mkdir()
    base_wals = [f.name for f in (seed / "pg_wal").iterdir()
                 if len(f.name) == 24 and all(c in "0123456789ABCDEF" for c in f.name)]
    assert base_wals
    for name in base_wals:
        shutil.copyfile(source / "pg_wal" / name, archive / name)
    sql("INSERT INTO sentinel VALUES (2)")
    sql("SELECT pg_switch_wal()")
    assert sql("SELECT string_agg(id::text, ',' ORDER BY id) FROM sentinel") == "1,2"
    stop(source)

    results = {"version": version, "base_wals": base_wals, "source_rows": "1,2"}
    for code in (1, 255):
        data = ROOT / f"restore-{code}"
        shutil.copytree(seed, data)
        helper = ROOT / f"fetch-{code}.sh"
        # Controlled names/paths only. A verified absent history file returns
        # 1 in BOTH cases. Only WAL transport failure differs between cases.
        helper.write_text(f'''#!/bin/sh
printf '%s\\n' "$1" >> '{ROOT}/requests-{code}.log'
case "$1" in *.history) exit 1;; esac
if test -f '{archive}/'"$1"; then
  cp '{archive}/'"$1" "$2" || exit 255
  exit 0
fi
printf '%s\\n' 'injected WAL transport outage' >&2
exit {code}
''')
        helper.chmod(0o700)
        (data / "postgresql.auto.conf").write_text("")
        (data / "recovery.signal").touch()
        # PostgreSQL config literal quoting, separate from shell quoting.
        command = f"exec {helper} \"%f\" \"%p\"".replace("'", "''")
        with (data / "postgresql.conf").open("a") as f:
            f.write(f"\nrestore_command='{command}'\nrecovery_target_timeline='1'\n"
                    "recovery_target_action='promote'\n")
        logfile = ROOT / f"restore-{code}.log"
        started = start(data, logfile, check=False)
        log = logfile.read_text()
        if code == 1:
            assert started.returncode == 0, log
            assert sql("SELECT pg_is_in_recovery()") == "f"
            assert sql("SELECT string_agg(id::text, ',' ORDER BY id) FROM sentinel") == "1"
            assert "injected WAL transport outage" in log, log
            results["ordinary_exit_1"] = "PROMOTED; row 2 lost despite existing on source"
        else:
            assert started.returncode != 0, log
            assert "FATAL" in log and "255" in log, log
            assert "selected new timeline" not in log, log
            results["fatal_exit_255"] = "startup FAILED; no promotion; FATAL exit 255"
        stop(data)
    (ROOT / "result.json").write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps(results, indent=2), flush=True)
finally:
    for data in servers:
        stop(data)
