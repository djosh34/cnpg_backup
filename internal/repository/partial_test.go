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

func TestPromotionPartialNameArithmetic(t *testing.T) {
	for _, size := range []int64{1 << 20, 16 << 20, 64 << 20, 1 << 30} {
		limit := (int64(1) << 32) / size
		last := fmt.Sprintf("00000001FFFFFFFF%08X.partial", limit-1)
		if !ValidWALFilename(last) || ValidateWALName(last, size) != nil {
			t.Fatal("last valid segment rejected", size, last)
		}
		if ValidateWALName(fmt.Sprintf("0000000100000000%08X.partial", limit), size) == nil {
			t.Fatal("partial segment arithmetic overflow accepted", size)
		}
	}
	const name = "000000010000000000000003.partial"
	for _, bad := range []string{
		"000000000000000000000003.partial", "00000001000000000000000a.partial",
		strings.ToUpper(name), name + ".partial", name + ".gz", "../" + name,
		"/" + name, "wal/" + name, name + "\x00", name + "\n", name + " ",
	} {
		if ValidWALFilename(bad) || ValidateWALName(bad, 16<<20) == nil {
			t.Fatalf("unsafe partial accepted: %q", bad)
		}
	}
}

func TestPromotionPartialInventoryRetainedAndGCRejected(t *testing.T) {
	r, s := setup(t)
	const name = "000000990000000000000003.partial"
	key, err := r.WALKey(name)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("auxiliary bytes: inventory never claims payload verification")
	info, err := s.mutate(key, data, s3store.Condition{Create: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	h, err := r.AdmitRestore(ctx, targetID, UUID())
	if err != nil {
		t.Fatal(err)
	}
	names, err := h.ArchiveNames(ctx)
	if err != nil || !reflect.DeepEqual(names, []string{name}) {
		t.Fatal("auxiliary missing from bounded inventory", names, err)
	}
	if err = h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = r.ReleaseLifetimeAfterTermination(ctx, targetID, h.holder.OperationID); err != nil {
		t.Fatal(err)
	}
	g, err := r.AcquireGC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close(ctx)
	before := s.n
	err = g.Execute(ctx, gcplan(g, Victim{Kind: "retire-wal", WALName: ptrID(name), ExpectedETag: &info.ETag, SHA256: digest(data), RawBytes: r.id.WALSegmentBytes}))
	if err != ErrInvalid || s.n != before || string(s.objects[key].b) != string(data) {
		t.Fatal("GC gained auxiliary retirement authority", err)
	}
}

func TestPromotionPartialCannotAdvanceOrFillArchiveFrontier(t *testing.T) {
	r, _ := setup(t)
	publish(t, r, backupID, "full")
	fp := digest([]byte("bootstrap"))
	op, _ := RestoreOperationID(targetID, fp)
	h, err := r.AdmitRestore(ctx, targetID, op)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(ctx)
	catalog, err := h.Catalog(ctx, CatalogLimits{100, 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	const first = "000000010000000000000001"
	const second = "000000010000000000000002"
	const third = "000000010000000000000003"
	for _, tc := range []struct {
		name    string
		names   []string
		want    []WALRange
		blocked bool
	}{
		{"partial alone is not a frontier", []string{third + ".partial"}, []WALRange{}, false},
		{"partial is not latest timeline or history evidence", []string{"000000990000000000000003.partial"}, []WALRange{}, false},
		{"partial cannot extend complete coverage", []string{first, second + ".partial"}, []WALRange{{1, "0/1000100", "0/2000000"}}, false},
		{"partial cannot fill interior gap", []string{first, second + ".partial", third}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := h.Resolve(ctx, catalog, Plan{DestinationRepositoryID: destID, BootstrapSHA256: fp, Target: Target{Kind: "latest"}}, tc.names, func(Commit) error { return nil }, func(context.Context, string) ([]byte, error) {
				t.Fatal("timeline-one auxiliary must not require history")
				return nil, ErrInvalid
			})
			if tc.blocked {
				if !errors.Is(err, ErrBlocked) {
					t.Fatal("valid partial inventory must leave a required complete-WAL gap", err)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(p.RequiredArchive, tc.want) {
				t.Fatalf("partial changed admitted coverage: got=%+v want=%+v error=%v", p.RequiredArchive, tc.want, err)
			}
			bundle, remote, err := p.Coverage(first + ".partial")
			if err != nil || bundle || remote {
				t.Fatal("partial classified as complete bundle/remote coverage", bundle, remote, err)
			}
		})
	}
}
