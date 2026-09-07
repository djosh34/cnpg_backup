package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRanges(t *testing.T) {
	for _, body := range []string{`{}`, `{"PostgreSQL-Backup-Manifest-Version":2,"WAL-Ranges":[]}`, `{"PostgreSQL-Backup-Manifest-Version":2,"WAL-Ranges":[{"Timeline":0,"Start-LSN":"0/1","End-LSN":"0/2"}]}`, `{"PostgreSQL-Backup-Manifest-Version":2,"WAL-Ranges":[{"Timeline":1,"Start-LSN":"0/2","End-LSN":"0/1"}]}`} {
		if _, err := ranges(strings.NewReader(body)); err == nil {
			t.Fatal("accepted", body)
		}
	}
	for _, s := range []string{"-1/2", "1/100000000", "1/2;sh", "1/", "1/2/3"} {
		if _, err := lsn(s); err == nil {
			t.Fatal("accepted", s)
		}
	}
}

func TestEveryRangeAndLaterFailure(t *testing.T) {
	dir := t.TempDir()
	body := `{"PostgreSQL-Backup-Manifest-Version":2,"WAL-Ranges":[{"Timeline":1,"Start-LSN":"0/1","End-LSN":"0/2"},{"Timeline":2,"Start-LSN":"0/2","End-LSN":"0/3"}]}`
	if err := os.WriteFile(filepath.Join(dir, "backup_manifest"), []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("later WAL missing")
	calls := 0
	err := verify(context.Background(), "/tools", dir, "/wal", func(_ context.Context, tool string, args ...string) error {
		calls++
		if calls == 1 && (tool != "/tools/pg_verifybackup" || args[1] != "--no-parse-wal") {
			t.Fatal(tool, args)
		}
		if calls == 3 {
			if tool != "/tools/pg_waldump" || args[2] != "--timeline=2" {
				t.Fatal(tool, args)
			}
			return failure
		}
		return nil
	})
	if !errors.Is(err, failure) || calls != 3 {
		t.Fatal(calls, err)
	}
}
