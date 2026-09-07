package postgres

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/repository"
)

func TestActualNativeTarInput(t *testing.T) {
	directory := os.Getenv("CNPG_TEST_NATIVE_TAR")
	if directory == "" {
		t.Skip("native input fixture not selected; CNPG harness covers capture")
	}
	f, e := os.Open(filepath.Join(directory, "backup_manifest"))
	if e != nil {
		t.Fatal(e)
	}
	m, e := ScanManifest(f)
	f.Close()
	if e != nil {
		t.Fatal(e)
	}
	actual := map[string]int64{}
	label := ""
	entries, e := os.ReadDir(directory)
	if e != nil {
		t.Fatal(e)
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".tar") {
			continue
		}
		f, e := os.Open(filepath.Join(directory, entry.Name()))
		if e != nil {
			t.Fatal(e)
		}
		st, _ := f.Stat()
		_, e = scanArchive(context.Background(), f, st.Size(), func(p string, n int64, r io.Reader) error {
			switch entry.Name() {
			case "pg_wal.tar":
				return nil
			case "base.tar":
				if p == "backup_label" {
					b, e := io.ReadAll(r)
					if e != nil {
						return e
					}
					label = string(b)
				}
			default:
				p = "pg_tblspc/" + strings.TrimSuffix(entry.Name(), ".tar") + "/" + p
			}
			actual[p] = n
			return nil
		})
		f.Close()
		if e != nil {
			t.Fatal(entry.Name(), e)
		}
	}
	for p, n := range m.Files {
		if actual[p] != n {
			t.Fatal("native manifest/tar disagree", p)
		}
	}
	if len(actual) != len(m.Files) {
		t.Fatal("unmanifested regular native data")
	}
	r := m.Ranges[0]
	c := repository.Commit{Timeline: r.Timeline, StartLSN: r.StartLSN, StopLSN: r.EndLSN, StoppedAt: time.Now().UTC().Format(time.RFC3339Nano), BackupLabel: label}
	if _, e = parseLabel(&c, 16<<20); e != nil {
		t.Fatal("actual backup label", e)
	}
}
