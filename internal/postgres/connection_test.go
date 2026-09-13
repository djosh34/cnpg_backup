package postgres

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/configuration"
)

func TestNativeConnectionSnapshot(t *testing.T) {
	t.Setenv("PGPASSWORD", "ambient-password")
	t.Setenv("PGHOST", "foreign-host")
	snapshot := &configuration.CaptureSnapshot{Certificate: []byte("certificate"), Key: []byte("private-key"), ServerCA: []byte("trusted-ca")}
	for _, application := range []string{"cnpg-backup", "cnpg-backup-preflight"} {
		t.Run(application, func(t *testing.T) {
			dir := t.TempDir()
			env, err := prepareConnection(dir, "database-rw.test.svc", snapshot, application)
			if err != nil {
				t.Fatal(err)
			}
			for name, want := range map[string]string{"client.crt": "certificate", "client.key": "private-key", "server-ca.crt": "trusted-ca"} {
				path := filepath.Join(dir, name)
				got, err := os.ReadFile(path)
				if err != nil || string(got) != want {
					t.Fatal("credential snapshot differs", name, err)
				}
				info, err := os.Stat(path)
				if err != nil || info.Mode().Perm() != 0600 {
					t.Fatal("credential permissions", name, err)
				}
			}
			service, err := os.ReadFile(filepath.Join(dir, "service.conf"))
			if err != nil {
				t.Fatal(err)
			}
			want := "[local]\nhost=database-rw.test.svc\nhostaddr=127.0.0.1\nport=5432\nuser=streaming_replica\ndbname=postgres\nsslmode=verify-full\nconnect_timeout=10\nsslcert=" + dir + "/client.crt\nsslkey=" + dir + "/client.key\nsslrootcert=" + dir + "/server-ca.crt\n"
			if string(service) != want {
				t.Fatal("unexpected libpq service configuration")
			}
			for _, setting := range []string{"PGSERVICE=local", "PGSERVICEFILE=" + dir + "/service.conf", "PGPASSFILE=/nonexistent", "PGAPPNAME=" + application, "PGOPTIONS=-c search_path=pg_catalog -c statement_timeout=30000"} {
				if !slices.Contains(env, setting) {
					t.Fatal("missing native setting", setting)
				}
			}
			// Check the executed child's environment, not only the prepared slice.
			out, err := runNative(context.Background(), append(env, "NATIVE_PROCESS_TEST=environment"), os.Args[0], "-test.run=^TestNativeProcessHelper$")
			if err != nil || string(out) != "|" {
				t.Fatal("native child inherited ambient libpq configuration", err)
			}
		})
	}
}

func TestNativeConnectionSnapshotWriteFailure(t *testing.T) {
	for _, name := range []string{"client.crt", "client.key", "server-ca.crt", "service.conf"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Mkdir(filepath.Join(dir, name), 0700); err != nil {
				t.Fatal(err)
			}
			env, err := prepareConnection(dir, "database-rw.test.svc", &configuration.CaptureSnapshot{Key: []byte("private-key")}, "cnpg-backup")
			if err == nil || env != nil {
				t.Fatal("partial snapshot returned a runnable environment")
			}
			if strings.Contains(err.Error(), dir) || strings.Contains(err.Error(), "private-key") {
				t.Fatal("snapshot failure leaked private details")
			}
		})
	}
}
