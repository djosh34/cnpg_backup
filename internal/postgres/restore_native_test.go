// Copyright 2026 cnpg_backup contributors. All rights reserved.
package postgres

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
)

// This is a real native component regression, not public CNPG/MinIO recovery or
// physical cross-PVC qualification. Source fixture tools are test-only; the
// materializer itself never starts PostgreSQL or invokes a shell.
func TestActualNativeFullMaterialization(t *testing.T) {
	bin := os.Getenv("CNPG_TEST_PG_BIN")
	if bin == "" {
		t.Skip("set CNPG_TEST_PG_BIN/SHARE/LIBS for disposable PG18 native restore regression")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	env := []string{"LANG=C", "LC_ALL=C", "HOME=/nonexistent", "LD_LIBRARY_PATH=" + os.Getenv("CNPG_TEST_PG_LIBS")}
	root, e := os.MkdirTemp(os.TempDir(), "restore-pg-")
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if t.Failed() {
			t.Log("retained native first-failure fixture", root)
			return
		}
		os.RemoveAll(root)
	}()
	socket, e := os.MkdirTemp(os.TempDir(), "s-")
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(socket)
	fixture := func(tool string, args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, filepath.Join(bin, tool), args...)
		cmd.Env = env
		b, e := cmd.CombinedOutput()
		if e != nil {
			t.Fatalf("fixture %s %v: %v\n%s", tool, args, e, b)
		}
		return strings.TrimSpace(string(b))
	}
	run := func(ctx context.Context, tool string, args ...string) ([]byte, error) {
		return runNative(ctx, env, filepath.Join(bin, tool), args...)
	}
	for _, tool := range []string{"pg_verifybackup", "pg_waldump", "pg_controldata"} {
		b, e := run(ctx, tool, "--version")
		if e != nil || !nativeVersion(b, tool) {
			t.Fatal("tool pin", tool, string(b), e)
		}
	}
	source := root + "/source"
	initArgs := []string{"-D", source, "--no-locale", "--encoding=UTF8", "--auth=trust", "--wal-segsize=1"}
	if share := os.Getenv("CNPG_TEST_PG_SHARE"); share != "" {
		initArgs = append(initArgs, "-L", share)
	}
	fixture("initdb", initArgs...)
	if e = os.WriteFile(source+"/postgresql.auto.conf", []byte("listen_addresses=''\nunix_socket_directories='"+socket+"'\nport=55483\nmax_wal_senders=4\nmax_replication_slots=4\n"), 0600); e != nil {
		t.Fatal(e)
	}
	fixture("pg_ctl", "-D", source, "-l", root+"/source.log", "-w", "start")
	sourceRunning := true
	defer func() {
		if sourceRunning {
			cmd := exec.Command(filepath.Join(bin, "pg_ctl"), "-D", source, "-m", "immediate", "-w", "stop")
			cmd.Env = env
			b, e := cmd.CombinedOutput()
			if e != nil {
				t.Errorf("fixture shutdown: %v %s", e, b)
			}
		}
	}()
	sql := func(query string) string {
		return fixture("psql", "-XAtq", "-h", socket, "-p", "55483", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c", query)
	}
	sql("CREATE TABLE sentinel(id integer primary key, value text); INSERT INTO sentinel VALUES(1,'before backup');")
	capture := func(name string) (*fullInput, repository.Commit) {
		t.Helper()
		dir := root + "/" + name
		if e := os.Mkdir(dir, 0700); e != nil {
			t.Fatal(e)
		}
		fixture("pg_basebackup", "--no-password", "-h", socket, "-p", "55483", "--pgdata="+dir+"/tar", "--format=tar", "--wal-method=stream", "--checkpoint=fast", "--manifest-checksums=SHA256")
		return nativeRestoreFixture(t, ctx, dir+"/tar")
	}
	in, c := capture("full")
	sql("INSERT INTO sentinel VALUES (2,'after selected full');")
	native := configuration.Native{MaxBackupBytes: 256 << 20, MaxBootstrapWALBytes: 32 << 20, MaxRestoredBytes: 256 << 20}
	if e = in.scan(ctx, c, 1<<20, native); e != nil {
		t.Fatal("actual original scan", e)
	}
	if e = in.verify(ctx, c, 1<<20, run); e != nil {
		t.Fatal("actual original tar/WAL verification", e)
	}
	t.Run("missing-required-original-WAL", func(t *testing.T) {
		entries, _ := os.ReadDir(in.walCheck)
		var p string
		for _, d := range entries {
			if len(d.Name()) == 24 {
				p = filepath.Join(in.walCheck, d.Name())
				break
			}
		}
		if p == "" {
			t.Fatal("no bundle")
		}
		if e := os.Rename(p, p+".withheld"); e != nil {
			t.Fatal(e)
		}
		defer os.Rename(p+".withheld", p)
		if e := in.verify(ctx, c, 1<<20, run); e == nil {
			t.Fatal("missing required bundle passed actual native original verification")
		}
	})
	t.Run("corrupt-original-manifest-checksum", func(t *testing.T) {
		p := in.directory + "/backup_manifest"
		original, e := os.ReadFile(p)
		if e != nil {
			t.Fatal(e)
		}
		corrupt := append([]byte{}, original...)
		at := strings.LastIndex(string(corrupt), `"Manifest-Checksum": "`)
		if at < 0 {
			t.Fatal("unexpected native manifest format")
		}
		at += len(`"Manifest-Checksum": "`)
		if corrupt[at] == '0' {
			corrupt[at] = '1'
		} else {
			corrupt[at] = '0'
		}
		if e = os.WriteFile(p, corrupt, 0600); e != nil {
			t.Fatal(e)
		}
		defer os.WriteFile(p, original, 0600)
		if e = in.verify(ctx, c, 1<<20, run); e == nil {
			t.Fatal("corrupted ORIGINAL self-checksum passed native verifier")
		}
	})
	t.Run("missing-second-range-is-not-skipped", func(t *testing.T) {
		ranges := append(append([]repository.WALRange{}, c.WALRanges...), repository.WALRange{Timeline: 1, StartLSN: "0/F000028", EndLSN: "0/F000120"})
		calls := 0
		if e := verifyRestoreWAL(ctx, in.walCheck, ranges, func(context.Context, string, ...string) ([]byte, error) { calls++; return nil, nil }); e == nil || calls != 0 {
			t.Fatal("unsupported multi-range input was partly accepted", e, calls)
		}
	})
	output := RestoreLayout{PGDATA: root + "/restored", WALDirectory: root + "/restored-wal", Tablespaces: map[string]string{}}
	hashes, e := in.materialize(ctx, c, output, run)
	if e != nil {
		t.Fatal("actual extracted original verification/materialization", e)
	}
	if len(hashes) == 0 {
		t.Fatal("no verified final bundle")
	}
	t.Log("actual full tar AND extracted original --no-parse-wal plus direct parser passed; local ordinary-copy WAL relocation passed")
	// A real tablespace capture has a local test path, intentionally outside the
	// supported CNPG namespace. Public scan must reject that map. Then exercise
	// the separately verified original native mapping/relocation components with
	// that trusted test fixture (not a production validation bypass).
	ts := root + "/source-ts"
	if e = os.Mkdir(ts, 0700); e != nil {
		t.Fatal(e)
	}
	sql("CREATE TABLESPACE fast LOCATION '" + ts + "';")
	sql("CREATE TABLE ts_sentinel(id int) TABLESPACE fast; INSERT INTO ts_sentinel VALUES (7);")
	tsInput, tsCommit := capture("tablespace-full")
	tsCommit.Tablespaces[0].Name = "fast"
	if e = tsInput.scan(ctx, tsCommit, 1<<20, native); e == nil {
		t.Fatal("unmanaged historical map accepted by public scan")
	}
	// scan performed strict raw/manifest/header checks and created metadata before
	// rejecting the historical map. Preserve those original bytes for native tools.
	tsInput.walCheck = root + "/tablespace-full/walcheck"
	if e = tsInput.extractArchive(ctx, tsCommit, "pg_wal.tar", tsInput.walCheck); e != nil {
		t.Fatal(e)
	}
	if e = tsInput.verify(ctx, tsCommit, 1<<20, run); e != nil {
		t.Fatal("actual tablespace original tar verification", e)
	}
	tsLayout := RestoreLayout{PGDATA: root + "/restored-ts", WALDirectory: root + "/restored-ts-wal", Tablespaces: map[string]string{"fast": root + "/target-ts"}}
	if _, e = tsInput.materialize(ctx, tsCommit, tsLayout, run); e != nil {
		t.Fatal("actual original tablespace extraction/mapping", e)
	}
	fixture("pg_ctl", "-D", source, "-m", "fast", "-w", "stop")
	sourceRunning = false
	for _, test := range []struct {
		name        string
		layout      RestoreLayout
		query, want string
	}{
		{"full", output, "SELECT string_agg(id::text||':'||value,',') FROM sentinel", "1:before backup"},
		{"tablespace-components", tsLayout, "SELECT (SELECT string_agg(id::text,',') FROM ts_sentinel)||':'||(SELECT pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname='fast')", "7:" + tsLayout.Tablespaces["fast"]},
	} {
		func() {
			// Test-only archive absence command exercises PostgreSQL's local
			// bundle replay. Public helper/remote WAL is the CNPG campaign gate.
			config, e := os.OpenFile(test.layout.PGDATA+"/postgresql.auto.conf", os.O_APPEND|os.O_WRONLY, 0)
			if e != nil {
				t.Fatal(e)
			}
			_, e = config.WriteString("\nrestore_command='/bin/false'\n")
			config.Close()
			if e != nil {
				t.Fatal(e)
			}
			if e := os.WriteFile(test.layout.PGDATA+"/recovery.signal", nil, 0600); e != nil {
				t.Fatal(e)
			}
			fixture("pg_ctl", "-D", test.layout.PGDATA, "-l", root+"/"+test.name+".log", "-w", "start")
			defer fixture("pg_ctl", "-D", test.layout.PGDATA, "-m", "fast", "-w", "stop")
			if got := sql(test.query); got != test.want {
				t.Fatal(got, "want", test.want)
			}
			if got := sql("SELECT pg_is_in_recovery()"); got != "f" {
				t.Fatal("not promoted", got)
			}
			t.Log(test.name + " actual local SQL verified")
		}()
	}
}

// Read exact real pg_basebackup artifacts into the commit fields needed by the
// native component boundary. No SQL/source identity input is needed to restore.
func nativeRestoreFixture(t *testing.T, ctx context.Context, dir string) (*fullInput, repository.Commit) {
	t.Helper()
	f, e := os.Open(dir + "/backup_manifest")
	if e != nil {
		t.Fatal(e)
	}
	m, e := ScanManifest(f)
	if e != nil {
		t.Fatal(e)
	}
	st, _ := f.Stat()
	hash, e := fileHash(ctx, f)
	f.Close()
	if e != nil {
		t.Fatal(e)
	}
	r := m.Ranges[0]
	c := repository.Commit{Kind: "full", SystemIdentifier: strconv.FormatUint(m.SystemIdentifier, 10), Timeline: r.Timeline, StartLSN: r.StartLSN, StopLSN: r.EndLSN, WALRanges: m.Ranges, ManifestBytes: st.Size(), ManifestSHA256: hash, Tablespaces: []repository.Tablespace{}}
	entries, e := os.ReadDir(dir)
	if e != nil {
		t.Fatal(e)
	}
	for _, d := range entries {
		if !strings.HasSuffix(d.Name(), ".tar") {
			continue
		}
		f, e := os.Open(filepath.Join(dir, d.Name()))
		if e != nil {
			t.Fatal(e)
		}
		st, _ := f.Stat()
		hash, e := fileHash(ctx, f)
		if e != nil {
			t.Fatal(e)
		}
		a := repository.Artifact{Index: len(c.Artifacts), RawBytes: st.Size(), StoredBytes: st.Size(), RawSHA256: hash, StoredSHA256: hash, Compression: "none"}
		switch d.Name() {
		case "base.tar":
			a.Role = "base"
		case "pg_wal.tar":
			a.Role = "wal"
		default:
			a.Role = "tablespace"
			oid, e := strconv.ParseUint(strings.TrimSuffix(d.Name(), ".tar"), 10, 32)
			if e != nil {
				t.Fatal(e)
			}
			v := uint32(oid)
			a.TablespaceOID = &v
			c.Tablespaces = append(c.Tablespaces, repository.Tablespace{OID: v, Name: "fast"})
		}
		if a.Role == "base" {
			_, e = scanArchive(ctx, f, st.Size(), func(p string, n int64, r io.Reader) error {
				if p == "backup_label" || p == "tablespace_map" {
					b, e := io.ReadAll(io.LimitReader(r, 64<<10))
					if e != nil {
						return e
					}
					if p == "backup_label" {
						c.BackupLabel = string(b)
					} else {
						c.TablespaceMap = string(b)
					}
				}
				return nil
			})
			if e != nil {
				t.Fatal(e)
			}
		}
		f.Close()
		c.Artifacts = append(c.Artifacts, a)
	}
	// Actual control is verified again after confined private extraction; initial
	// fixtures use initdb's default enabled page checksums on PG18.
	c.ChecksumVersion = 1
	return &fullInput{directory: dir}, c
}
