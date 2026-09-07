// verify is test-only R0 tooling, mounted into (never shipped in) the data image.
// Production verification and hostile-manifest parsing arrive with PR F/H.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type walRange struct {
	Timeline uint32 `json:"Timeline"`
	Start    string `json:"Start-LSN"`
	End      string `json:"End-LSN"`
}

var lsnPattern = regexp.MustCompile(`^[0-9A-F]{1,8}/[0-9A-F]{1,8}$`)

func lsn(s string) (uint64, error) {
	if !lsnPattern.MatchString(s) {
		return 0, fmt.Errorf("invalid LSN %q", s)
	}
	p := strings.Split(s, "/")
	hi, _ := strconv.ParseUint(p[0], 16, 32)
	lo, _ := strconv.ParseUint(p[1], 16, 32)
	return hi<<32 | lo, nil
}

func ranges(r io.Reader) ([]walRange, error) {
	const max = 64 << 20
	b, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if len(b) > max {
		return nil, fmt.Errorf("manifest too large")
	}
	var m struct {
		Version int        `json:"PostgreSQL-Backup-Manifest-Version"`
		Ranges  []walRange `json:"WAL-Ranges"`
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if m.Version != 2 || len(m.Ranges) == 0 || len(m.Ranges) > 64 {
		return nil, fmt.Errorf("unsupported manifest/range count")
	}
	for _, r := range m.Ranges {
		s, se := lsn(r.Start)
		e, ee := lsn(r.End)
		if r.Timeline == 0 || se != nil || ee != nil || s >= e {
			return nil, fmt.Errorf("invalid WAL range: %+v", r)
		}
	}
	return m.Ranges, nil
}

func verify(ctx context.Context, bin, data, wal string, execTool func(context.Context, string, ...string) error) error {
	f, err := os.Open(filepath.Join(data, "backup_manifest"))
	if err != nil {
		return err
	}
	rr, err := ranges(f)
	f.Close()
	if err != nil {
		return err
	}
	if err := execTool(ctx, filepath.Join(bin, "pg_verifybackup"), "--exit-on-error", "--no-parse-wal", data); err != nil {
		return err
	}
	for _, r := range rr {
		if err := execTool(ctx, filepath.Join(bin, "pg_waldump"), "--quiet", "--path="+wal, fmt.Sprintf("--timeline=%d", r.Timeline), "--start="+r.Start, "--end="+r.End); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: verify TOOL_BIN BACKUP WAL_DIRECTORY")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	err := verify(ctx, os.Args[1], os.Args[2], os.Args[3], func(ctx context.Context, tool string, args ...string) error {
		fmt.Fprintln(os.Stderr, tool, args)
		cmd := exec.CommandContext(ctx, tool, args...)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		return cmd.Run()
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
