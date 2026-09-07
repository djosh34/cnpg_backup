package postgres

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/repository"
)

func testManifest() []byte {
	return []byte(`{"PostgreSQL-Backup-Manifest-Version":2,"System-Identifier":123,"Files":[{"Path":"global/pg_control","Size":8192,"Last-Modified":"2026-09-07 00:00:00 GMT","Checksum-Algorithm":"SHA256","Checksum":"` + strings.Repeat("a", 64) + `"}],"WAL-Ranges":[{"Timeline":1,"Start-LSN":"0/1000028","End-LSN":"0/1000120"}],"Manifest-Checksum":"` + strings.Repeat("b", 64) + `"}`)
}
func TestManifestBoundsAndRanges(t *testing.T) {
	good := testManifest()
	if _, e := ScanManifest(bytes.NewReader(good)); e != nil {
		t.Fatal(e)
	}
	cases := map[string][]byte{
		"duplicate":   bytes.Replace(good, []byte(`"Size":8192`), []byte(`"Size":8192,"Size":8192`), 1),
		"traversal":   bytes.Replace(good, []byte(`global/pg_control`), []byte(`../outside`), 1),
		"expansion":   bytes.Replace(good, []byte(`8192`), []byte(`999999999999999`), 1),
		"range":       bytes.Replace(good, []byte(`0/1000120`), []byte(`0/1000000`), 1),
		"multi-range": bytes.Replace(good, []byte(`"End-LSN":"0/1000120"}]`), []byte(`"End-LSN":"0/1000120"},{"Timeline":2,"Start-LSN":"0/2000028","End-LSN":"0/2000120"}]`), 1),
		"unknown":     bytes.Replace(good, []byte(`"System-Identifier":123`), []byte(`"System-Identifier":123,"foo":0`), 1),
		"trailing":    append(bytes.Clone(good), 'x'),
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			if _, e := ScanManifest(bytes.NewReader(b)); e == nil {
				t.Fatal("accepted unsupported input")
			}
		})
	}
	encoded := bytes.Replace(good, []byte(`"Path":"global/pg_control"`), []byte(`"Encoded-Path":"`+hex.EncodeToString([]byte("global/pg_control"))+`"`), 1)
	if _, e := ScanManifest(bytes.NewReader(encoded)); e != nil {
		t.Fatal(e)
	}
}
func makeTar(t *testing.T, headers ...*tar.Header) []byte {
	t.Helper()
	var out bytes.Buffer
	w := tar.NewWriter(&out)
	for _, h := range headers {
		if e := w.WriteHeader(h); e != nil {
			t.Fatal(e)
		}
		if h.Typeflag == tar.TypeReg {
			if _, e := w.Write(bytes.Repeat([]byte("x"), int(h.Size))); e != nil {
				t.Fatal(e)
			}
		}
	}
	if e := w.Close(); e != nil {
		t.Fatal(e)
	}
	return out.Bytes()
}
func TestArchiveConfinementAndLimits(t *testing.T) {
	cases := map[string][]*tar.Header{
		"escape":          {{Name: "../outside", Typeflag: tar.TypeReg, Size: 1}},
		"absolute":        {{Name: "/outside", Typeflag: tar.TypeReg, Size: 1}},
		"symlink":         {{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "../outside"}},
		"hardlink":        {{Name: "link", Typeflag: tar.TypeLink, Linkname: "../outside"}},
		"fifo":            {{Name: "fifo", Typeflag: tar.TypeFifo}},
		"duplicate":       {{Name: "same", Typeflag: tar.TypeReg, Size: 1}, {Name: "same", Typeflag: tar.TypeReg, Size: 1}},
		"directory-alias": {{Name: "same/", Typeflag: tar.TypeDir}, {Name: "same", Typeflag: tar.TypeDir}},
		"parent-file":     {{Name: "parent", Typeflag: tar.TypeReg, Size: 1}, {Name: "parent/child", Typeflag: tar.TypeReg, Size: 1}},
		"pax":             {{Name: "pax", Typeflag: tar.TypeReg, Size: 1, Format: tar.FormatPAX, PAXRecords: map[string]string{"comment": "hidden"}}},
	}
	for name, headers := range cases {
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			outside := filepath.Join(directory, "outside")
			os.WriteFile(outside, []byte("sentinel"), 0600)
			target := filepath.Join(directory, "target")
			os.Mkdir(target, 0700)
			root, e := os.OpenRoot(target)
			if e != nil {
				t.Fatal(e)
			}
			defer root.Close()
			data := makeTar(t, headers...)
			_, e = scanArchive(context.Background(), bytes.NewReader(data), int64(len(data)), func(p string, n int64, r io.Reader) error { return copyMember(root, p, n, r) })
			if e == nil {
				t.Fatal("unsafe archive accepted")
			}
			got, _ := os.ReadFile(outside)
			if string(got) != "sentinel" {
				t.Fatal("escape modified outside sentinel")
			}
		})
	}
	good := makeTar(t, &tar.Header{Name: "global/", Typeflag: tar.TypeDir}, &tar.Header{Name: "global/pg_control", Typeflag: tar.TypeReg, Size: 8})
	inv, e := scanArchive(context.Background(), bytes.NewReader(good), int64(len(good)), nil)
	if e != nil || inv.files["global/pg_control"] != 8 {
		t.Fatal(inv, e)
	}
	for _, bad := range [][]byte{good[:len(good)-1025], append(bytes.Clone(good), '!'), append(bytes.Clone(good), make([]byte, 10241)...)} {
		if _, e := scanArchive(context.Background(), bytes.NewReader(bad), int64(len(bad)), nil); e == nil {
			t.Fatal("truncated/trailing input accepted")
		}
	}
	if _, e := scanArchive(context.Background(), bytes.NewReader(good), 10, nil); e == nil {
		t.Fatal("raw cap not enforced")
	}
}
func TestSpoolExactAndBounded(t *testing.T) {
	for _, compression := range []string{"none", "gzip"} {
		t.Run(compression, func(t *testing.T) {
			raw, e := os.CreateTemp(t.TempDir(), "raw")
			if e != nil {
				t.Fatal(e)
			}
			defer raw.Close()
			body := bytes.Repeat([]byte("bounded-fixture"), 20000)
			raw.Write(body)
			raw.Seek(0, 0)
			f, a, e := spool(context.Background(), raw, t.TempDir(), compression, 1<<20)
			if e != nil {
				t.Fatal(e)
			}
			defer f.Close()
			f.Seek(0, 0)
			var input io.Reader = f
			if compression == "gzip" {
				z, e := gzip.NewReader(f)
				if e != nil {
					t.Fatal(e)
				}
				defer z.Close()
				input = z
			}
			b, e := io.ReadAll(input)
			if e != nil || !bytes.Equal(body, b) || a.RawBytes != int64(len(body)) {
				t.Fatal("spool mismatch", e)
			}
			raw.Seek(0, 0)
			if _, _, e = spool(context.Background(), raw, t.TempDir(), compression, 1); e == nil {
				t.Fatal("stored cap ignored")
			}
		})
	}
}
func TestLabelBoundaries(t *testing.T) {
	c := repository.Commit{Timeline: 1, StartLSN: "0/1000028", StopLSN: "0/1000120", StoppedAt: "2026-09-07T01:00:01Z", BackupLabel: "START WAL LOCATION: 0/1000028 (file 000000010000000000000001)\nCHECKPOINT LOCATION: 0/1000060\nBACKUP METHOD: streamed\nBACKUP FROM: primary\nSTART TIME: 2026-09-07 01:00:00 UTC\nLABEL: pg_basebackup base backup\nSTART TIMELINE: 1\n"}
	if e := parseLabel(&c, 16<<20); e != nil {
		t.Fatal(e)
	}
	c.BackupLabel = strings.Replace(c.BackupLabel, "primary", "standby", 1)
	if e := parseLabel(&c, 16<<20); e == nil {
		t.Fatal("standby accepted")
	}
}
func FuzzNativeManifest(f *testing.F) {
	f.Add(testManifest())
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1<<20 {
			t.Skip()
		}
		m, e := ScanManifest(bytes.NewReader(b))
		if e == nil {
			if len(m.Ranges) != 1 || len(m.Files) > maxEntries {
				t.Fatal("invalid accepted inventory")
			}
			_, _ = json.Marshal(m)
		}
	})
}
func FuzzNativeArchive(f *testing.F) {
	f.Add([]byte("invalid"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1<<20 {
			t.Skip()
		}
		_, _ = scanArchive(context.Background(), bytes.NewReader(b), int64(len(b)), nil)
	})
}
