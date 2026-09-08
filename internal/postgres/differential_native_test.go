// Copyright 2026 cnpg_backup contributors. All rights reserved.
package postgres

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/repository"
)

// Real PG component feedback before image/CNPG validation. Uses production Go
// scanning, confined extraction, original verification, combine and transforms.
func TestActualNativeDifferentialMaterialization(t *testing.T) {
	bin := os.Getenv("CNPG_TEST_PG_BIN")
	if bin == "" {
		t.Skip("set CNPG_TEST_PG_BIN/SHARE/LIBS")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	root, e := os.MkdirTemp(os.TempDir(), "h-native-")
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if t.Failed() {
			t.Log("retained first failure", root)
		} else {
			os.RemoveAll(root)
		}
	}()
	socket, e := os.MkdirTemp(os.TempDir(), "hs-")
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(socket)
	env := []string{"LANG=C", "LC_ALL=C", "HOME=/nonexistent", "LD_LIBRARY_PATH=" + os.Getenv("CNPG_TEST_PG_LIBS")}
	fixture := func(tool string, args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, filepath.Join(bin, tool), args...)
		cmd.Env = env
		b, e := cmd.CombinedOutput()
		if e != nil {
			t.Fatalf("%s: %v %s", tool, e, b)
		}
		return strings.TrimSpace(string(b))
	}
	run := func(ctx context.Context, tool string, args ...string) ([]byte, error) {
		return runNative(ctx, env, filepath.Join(bin, tool), args...)
	}
	source := root + "/source"
	args := []string{"-D", source, "--no-locale", "--encoding=UTF8", "--auth=trust", "--wal-segsize=1"}
	if share := os.Getenv("CNPG_TEST_PG_SHARE"); share != "" {
		args = append(args, "-L", share)
	}
	fixture("initdb", args...)
	config := "listen_addresses=''\nunix_socket_directories='" + socket + "'\nport=55484\nmax_wal_senders=4\nmax_replication_slots=4\nsummarize_wal=on\n"
	if e = os.WriteFile(source+"/postgresql.auto.conf", []byte(config), 0600); e != nil {
		t.Fatal(e)
	}
	fixture("pg_ctl", "-D", source, "-l", root+"/source.log", "-w", "start")
	active := source
	defer func() {
		if active != "" {
			cmd := exec.Command(filepath.Join(bin, "pg_ctl"), "-D", active, "-m", "immediate", "-w", "stop")
			cmd.Env = env
			if b, e := cmd.CombinedOutput(); e != nil {
				t.Errorf("shutdown %v %s", e, b)
			}
		}
	}()
	sql := func(q string) string {
		return fixture("psql", "-XAtq", "-h", socket, "-p", "55484", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c", q)
	}
	sql("CREATE TABLE data(id int primary key, value text); INSERT INTO data SELECT i,repeat(md5(i::text),16) FROM generate_series(1,20000)i; CREATE TABLE truncated(id int); INSERT INTO truncated VALUES(1); CREATE TABLE recreated(id int); INSERT INTO recreated VALUES(1);")
	capture := func(name, reference string) (*fullInput, repository.Commit) {
		t.Helper()
		dir := root + "/" + name
		if e := os.Mkdir(dir, 0700); e != nil {
			t.Fatal(e)
		}
		args := []string{"--no-password", "-h", socket, "-p", "55484", "--pgdata=" + dir + "/tar", "--format=tar", "--wal-method=stream", "--checkpoint=fast", "--manifest-checksums=SHA256"}
		if reference != "" {
			args = append(args, "--incremental="+reference)
		}
		fixture("pg_basebackup", args...)
		in, c := nativeRestoreFixture(t, ctx, dir+"/tar")
		if reference != "" {
			c.Kind = "differential"
		}
		return in, c
	}
	full, fc := capture("F", "")
	sql("UPDATE data SET value='d1' WHERE id=1; DELETE FROM data WHERE id=2; TRUNCATE truncated; INSERT INTO truncated VALUES(2); DROP TABLE recreated; CREATE TABLE recreated(id int); INSERT INTO recreated VALUES(2);")
	_, d1 := capture("D1", full.directory+"/backup_manifest")
	sql("UPDATE data SET value='d2' WHERE id=3; DELETE FROM data WHERE id=4; TRUNCATE truncated; INSERT INTO truncated VALUES(3); DROP TABLE recreated; CREATE TABLE recreated(id int); INSERT INTO recreated VALUES(3);")
	differential, dc := capture("D2", full.directory+"/backup_manifest")
	size := func(c repository.Commit) int64 {
		var n int64
		for _, a := range c.Artifacts {
			n += a.RawBytes
		}
		return n
	}
	if size(dc) >= size(fc) {
		t.Fatal("largely unchanged fixture did not reduce native bytes", size(fc), size(dc))
	}
	t.Log("native raw transfer F/D1/D2", size(fc), size(d1), size(dc))
	expected := sql("SELECT count(*)||':'||md5(string_agg(id::text||':'||value,',' ORDER BY id)) FROM data")
	sql("INSERT INTO data VALUES(30000,'post-D2')")
	// D1 is physically absent: only F + D2 can participate in reconstruction.
	if e = os.RemoveAll(root + "/D1"); e != nil {
		t.Fatal(e)
	}
	n := configuration.Native{MaxBackupBytes: 256 << 20, MaxBootstrapWALBytes: 32 << 20, MaxRestoredBytes: 256 << 20}
	for i, in := range []*fullInput{full, differential} {
		c := []repository.Commit{fc, dc}[i]
		if e = in.scan(ctx, c, 1<<20, n); e != nil {
			t.Fatal("scan", i, e)
		}
		if e = in.verify(ctx, c, 1<<20, run); e != nil {
			t.Fatal("original", i, e)
		}
	}
	// Corrupt ORIGINAL data with transport metadata intentionally unchanged: the
	// native verifier must reject it before any combine or synthetic checksum.
	t.Run("original-differential-corruption-blocks-combine", func(t *testing.T) {
		f, e := os.OpenFile(differential.directory+"/base.tar", os.O_RDWR, 0)
		if e != nil {
			t.Fatal(e)
		}
		defer f.Close()
		// First regular payload is native base data; flipping a tar byte invalidates
		// either header or manifested input, never a synthetic output assumption.
		b := make([]byte, 1)
		if _, e = f.ReadAt(b, 512); e != nil {
			t.Fatal(e)
		}
		original := b[0]
		b[0] ^= 1
		if _, e = f.WriteAt(b, 512); e != nil {
			t.Fatal(e)
		}
		defer f.WriteAt([]byte{original}, 512)
		if e = differential.verify(ctx, dc, 1<<20, run); e == nil {
			t.Fatal("corrupt original accepted")
		}
	})
	output := RestoreLayout{PGDATA: root + "/output", WALDirectory: root + "/wal", Tablespaces: map[string]string{}}
	if _, e = combineOriginals(ctx, []*fullInput{full, differential}, []repository.Commit{fc, dc}, output, n, run); e != nil {
		t.Fatal("F+D2 reconstruction", e)
	}
	fixture("pg_ctl", "-D", source, "-m", "fast", "-w", "stop")
	active = ""
	if e = os.WriteFile(output.PGDATA+"/postgresql.auto.conf", []byte(config+"restore_command='/bin/false'\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(output.PGDATA+"/recovery.signal", nil, 0600); e != nil {
		t.Fatal(e)
	}
	fixture("pg_ctl", "-D", output.PGDATA, "-l", root+"/restore.log", "-w", "start")
	active = output.PGDATA
	if got := sql("SELECT count(*)||':'||md5(string_agg(id::text||':'||value,',' ORDER BY id)) FROM data"); got != expected {
		t.Fatal("data hash", got, expected)
	}
	if got := sql("SELECT (SELECT id FROM truncated)||':'||(SELECT id FROM recreated)||':'||pg_is_in_recovery()"); got != "3:3:false" {
		t.Fatal("DDL/recovery oracle", got)
	}
	t.Log("production F+D2 original/synthetic verification and ordinary separate-WAL copy; SQL update/delete/truncate/drop/recreate/datahash passed")
}
