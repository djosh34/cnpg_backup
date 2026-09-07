package repository

import (
	"testing"
)

func TestRequestRootIsFrozenByValue(t *testing.T) {
	r, _ := setup(t)
	_, full := publish(t, r, backupID, "full")
	uid := "77777777-7777-4777-8777-777777777777"
	h, e := r.AdmitBackup(ctx, writerID, uid)
	if e != nil {
		t.Fatal(e)
	}
	req := request(uid)
	req.RequestedKind = "differential"
	req.RootBackupUID = ptr(full.Commit.BackupUID)
	a, _, e := h.Begin(ctx, req)
	if e != nil {
		t.Fatal(e)
	}
	*req.RootBackupUID = targetID
	if *a.request.RootBackupUID != full.Commit.BackupUID {
		t.Fatal("caller mutation changed frozen root")
	}
	if a.RequestSHA256() != a.claim.RequestSHA256 {
		t.Fatal("wrong frozen digest")
	}
}
func TestCatalogHeadsAreNotRestoreIntegrityEvidence(t *testing.T) {
	r, s := setup(t)
	_, res := publish(t, r, backupID, "full")
	key := r.artifact(res.Commit, res.Commit.Artifacts[0])
	o := s.objects[key]
	o.b[0] ^= 1
	s.objects[key] = o
	fp := digest([]byte("bootstrap"))
	op, _ := RestoreOperationID(targetID, fp)
	h, e := r.AdmitRestore(ctx, targetID, op)
	if e != nil {
		t.Fatal(e)
	}
	catalog, e := h.Catalog(ctx, CatalogLimits{10, 32 << 20})
	if e != nil {
		t.Fatal("same-length data corruption is not a HEAD finding:", e)
	}
	defer catalog.Close()
	_, e = h.Select(ctx, catalog, Plan{DestinationRepositoryID: destID, BootstrapSHA256: fp, Target: Target{Kind: "latest", Timeline: 1}, Path: []Timeline{{ID: 1}}, RequiredArchive: []WALRange{}})
	if e == nil {
		t.Fatal("selected a backup using HEAD as checksum")
	}
}
