package repository

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

func TestProtectedResolveFrontier(t *testing.T) {
	r, _ := setup(t)
	publish(t, r, backupID, "full")
	fp := digest([]byte("bootstrap"))
	op, _ := RestoreOperationID(targetID, fp)
	h, e := r.AdmitRestore(ctx, targetID, op)
	if e != nil {
		t.Fatal(e)
	}
	catalog, e := h.Catalog(ctx, CatalogLimits{100, 4 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer catalog.Close()
	history := func(_ context.Context, name string) ([]byte, error) {
		switch name {
		case "00000002.history":
			return []byte("1\t0/3000000\tpromotion\n"), nil
		case "00000003.history":
			return []byte("1\t0/4000000\tother branch\n"), nil
		}
		return nil, &s3store.Error{Kind: s3store.NotFound}
	}
	cases := []struct {
		name   string
		target Target
		names  []string
		want   []WALRange
		fail   bool
	}{
		{name: "bundle only", target: Target{Kind: "latest"}, want: []WALRange{}},
		{name: "same bundled filename remote required", target: Target{Kind: "latest"}, names: []string{"000000010000000000000001"}, want: []WALRange{{1, "0/1000100", "0/2000000"}}},
		{name: "no skip to newest after a gap", target: Target{Kind: "latest"}, names: []string{"000000010000000000000003"}, fail: true},
		{name: "continuous remote", target: Target{Kind: "latest"}, names: []string{"000000010000000000000001", "000000010000000000000002"}, want: []WALRange{{1, "0/1000100", "0/3000000"}}},
		{name: "native LSN equality not eligible", target: Target{Kind: "lsn", Value: "0/1000100"}, fail: true},
		{name: "source completion equality not eligible", target: Target{Kind: "time", Value: "2026-09-07T01:01:00Z"}, fail: true},
		{name: "valid later time", target: Target{Kind: "time", Value: "2026-09-07T03:01:01+02:00"}, want: []WALRange{}},
		{name: "missing history", target: Target{Kind: "latest"}, names: []string{"000000020000000000000003"}, fail: true},
		{name: "ambiguous leaves", target: Target{Kind: "latest"}, names: []string{"00000002.history", "00000003.history"}, fail: true},
		{name: "cross fork", target: Target{Kind: "latest", Timeline: 2}, names: []string{"00000002.history", "000000010000000000000001", "000000010000000000000002", "000000020000000000000003"}, want: []WALRange{{1, "0/1000100", "0/3000000"}, {2, "0/3000000", "0/4000000"}}},
		{name: "immediate ignores post base frontier not archive errors", target: Target{Kind: "immediate", BackupUID: ptrID(backupID)}, names: []string{"000000010000000000000003"}, want: []WALRange{}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			p, e := h.Resolve(ctx, catalog, Plan{DestinationRepositoryID: destID, BootstrapSHA256: fp, Target: tt.target}, tt.names, func(Commit) error { return nil }, history)
			if tt.fail {
				if e == nil {
					t.Fatal("accepted invalid frontier/target")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			if !reflect.DeepEqual(p.RequiredArchive, tt.want) {
				t.Fatalf("got %+v want %+v", p.RequiredArchive, tt.want)
			}
			if len(tt.want) > 0 {
				b, remote, e := p.Coverage("000000010000000000000001")
				if e != nil || !b || !remote {
					t.Fatal("S1 lost same segment remote interval", b, remote, e)
				}
			}
		})
	}
	if e = h.Close(ctx); e != nil {
		t.Fatal(e)
	}
	called := false
	if e = h.ReadSource(ctx, func(context.Context) error { called = true; return nil }); !errors.Is(e, ErrClosed) || called {
		t.Fatal("closed holder admitted source callback", e)
	}
}
func ptrID(s string) *string { return &s }
func TestHistoryNumericAndMalformed(t *testing.T) {
	p, e := ParseHistory(12, []byte("# comment\n1\t0/2000000\treason\n10\t0/4000000\treason with spaces\n"))
	if e != nil || fmt.Sprint([]uint32{p[0].ID, p[1].ID, p[2].ID}) != "[1 10 12]" {
		t.Fatal(p, e)
	}
	for _, b := range []string{"", "1 0/2", "1 0/2 reason\n1 0/3 duplicate", "12 0/2 future", "10 0/3 reason\n11 0/2 backwards", "0 0/2 zero", "1 X bad", strings.Repeat("x", (1<<20)+1)} {
		if _, e = ParseHistory(12, []byte(b)); e == nil {
			t.Fatal("accepted history", len(b))
		}
	}
}
func TestArchiveInventoryRequiresLiveReaderAndCompleteList(t *testing.T) {
	r, s := setup(t)
	h, e := r.AdmitRestore(ctx, targetID, UUID())
	if e != nil {
		t.Fatal(e)
	}
	key, _ := r.WALKey("000000010000000000000001")
	s.objects[key] = object{b: []byte("payload"), info: s3store.Info{Key: key, ETag: "wal"}}
	names, e := h.ArchiveNames(ctx)
	if e != nil || len(names) != 1 {
		t.Fatal(names, e)
	}
	badKey := "v1/" + repoID + "/wal/00000002/000000010000000000000001"
	s.objects[badKey] = object{b: []byte("bad"), info: s3store.Info{Key: badKey}}
	if _, e = h.ArchiveNames(ctx); e == nil {
		t.Fatal("accepted mismatching timeline directory")
	}
}
