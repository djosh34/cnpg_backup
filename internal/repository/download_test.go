package repository

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

type countingDownloadStore struct {
	Storage
	downloads map[string]int
}

func (s *countingDownloadStore) Download(c context.Context, key string, dst *os.File, in s3store.Integrity) (s3store.Info, error) {
	s.downloads[key]++
	return s.Storage.Download(c, key, dst, in)
}

func TestDownloadInputTransfersOnlyRequestedPayload(t *testing.T) {
	r, s := setup(t)
	_, full := publish(t, r, backupID, "full")
	h, diff := differential(t, r, full, "77777777-7777-4777-8777-777777777777")
	counter := &countingDownloadStore{Storage: s, downloads: map[string]int{}}
	r.store = counter
	dst := file(t, nil)
	want := map[string]bool{}
	for _, c := range []Commit{full.Commit, diff.Commit} {
		for index := -1; index < len(c.Artifacts); index++ {
			key := r.attempt(c.BackupUID, c.AttemptID) + "manifest.pg.json"
			if index >= 0 {
				key = r.artifact(c, c.Artifacts[index])
			}
			want[key] = true
			if e := h.DownloadInput(ctx, c.BackupUID, index, dst); e != nil {
				t.Fatal(e)
			}
		}
	}
	if len(counter.downloads) != len(want) {
		t.Fatalf("unexpected downloaded keys: %v", counter.downloads)
	}
	for key := range want {
		if counter.downloads[key] != 1 {
			t.Errorf("%s downloaded %d times, want once", key, counter.downloads[key])
		}
	}
}

func TestDownloadInputRejectsOriginalCorruption(t *testing.T) {
	for _, which := range []string{"full", "differential"} {
		for _, index := range []int{-1, 0, 1} {
			t.Run(which+"/"+strconv.Itoa(index), func(t *testing.T) {
				r, s := setup(t)
				_, full := publish(t, r, backupID, "full")
				h, diff := differential(t, r, full, "77777777-7777-4777-8777-777777777777")
				c := full.Commit
				if which == "differential" {
					c = diff.Commit
				}
				key := r.attempt(c.BackupUID, c.AttemptID) + "manifest.pg.json"
				if index >= 0 {
					key = r.artifact(c, c.Artifacts[index])
				}
				// Preserve length and all HEAD metadata: only original bytes change.
				o := s.objects[key]
				o.b[0] ^= 1
				s.objects[key] = o
				if e := h.DownloadInput(ctx, c.BackupUID, index, file(t, nil)); e == nil {
					t.Fatal("corrupt original input returned successfully")
				}
			})
		}
	}
}

func TestSelectedDifferentialChainRejectsOriginalFullCorruption(t *testing.T) {
	for _, index := range []int{-1, 0, 1} {
		t.Run(strconv.Itoa(index), func(t *testing.T) {
			r, s := setup(t)
			_, full := publish(t, r, backupID, "full")
			_, diff := differential(t, r, full, "77777777-7777-4777-8777-777777777777")
			fp := digest([]byte("bootstrap"))
			op, _ := RestoreOperationID(targetID, fp)
			h, e := r.AdmitRestore(ctx, targetID, op)
			if e != nil {
				t.Fatal(e)
			}
			cat, e := h.Catalog(ctx, CatalogLimits{10, 32 << 20})
			if e != nil {
				t.Fatal(e)
			}
			defer cat.Close()
			template := Plan{DestinationRepositoryID: destID, BootstrapSHA256: fp, Target: Target{Kind: "latest", Timeline: 1}, Path: []Timeline{{ID: 1}}, RequiredArchive: []WALRange{}}
			p, e := h.Select(ctx, cat, template)
			if e != nil {
				t.Fatal(e)
			}
			if len(p.Chain) != 2 || p.Chain[1].BackupUID != diff.Commit.BackupUID {
				t.Fatal("not F+D selected")
			}
			key := r.attempt(full.Commit.BackupUID, full.Commit.AttemptID) + "manifest.pg.json"
			if index >= 0 {
				key = r.artifact(full.Commit, full.Commit.Artifacts[index])
			}
			o := s.objects[key]
			o.b[0] ^= 1
			s.objects[key] = o
			if _, e = h.Select(ctx, cat, template); e == nil {
				t.Fatal("Select accepted corrupt full parent")
			}
			if _, e = h.ValidatePlan(ctx, p); e == nil {
				t.Fatal("ValidatePlan accepted corrupt original full")
			}
		})
	}
}

func TestDifferentialDownloadChecksParentIdentityAndLiveness(t *testing.T) {
	for _, damage := range []string{"missing", "retired", "identity", "request"} {
		t.Run(damage, func(t *testing.T) {
			r, s := setup(t)
			_, full := publish(t, r, backupID, "full")
			h, diff := differential(t, r, full, "77777777-7777-4777-8777-777777777777")
			key := r.backup(full.Commit.BackupUID) + "commit.json"
			switch damage {
			case "missing":
				delete(s.objects, key)
			case "retired":
				b, _ := json.Marshal(Retirement{1, repoID, full.Commit.BackupUID, digest(full.Bytes), UUID()})
				s.objects[r.backup(full.Commit.BackupUID)+"retired.json"] = object{b: b}
			case "identity":
				c := full.Commit
				c.CaptureInstanceUID = targetID
				o := s.objects[key]
				o.b, _ = json.Marshal(c)
				s.objects[key] = o
			case "request":
				delete(s.objects, r.backup(full.Commit.BackupUID)+"request.json")
			}
			if e := h.DownloadInput(ctx, diff.Commit.BackupUID, 0, file(t, nil)); e == nil {
				t.Fatal("invalid parent accepted")
			}
		})
	}
}
