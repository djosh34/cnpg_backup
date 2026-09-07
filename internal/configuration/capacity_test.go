package configuration

import (
	"golang.org/x/sys/unix"
	"strings"
	"testing"
)

func TestFiniteBackingRejectsLocalPathEmptyDirAndAliases(t *testing.T) {
	text := `30 20 7:8 / /cnpg-backup/work rw,relatime - ext4 /dev/loop8 rw
31 20 8:1 /var/local/pvc /data rw - ext4 /dev/sda rw
32 20 0:80 / /empty rw - tmpfs tmpfs rw,size=1048576
33 20 0:90 / /nfs rw - nfs server:/data rw
34 20 7:9 / /readonly ro - ext4 /dev/loop9 rw
`
	inventory, err := mounts(strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := finiteMount(inventory, "/cnpg-backup/work"); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/data", "/empty", "/nfs", "/readonly", "/missing"} {
		if _, err := finiteMount(inventory, path); err == nil {
			t.Fatal("unbounded backing accepted", path)
		}
	}
	// Exercise the real Linux probe on an ordinary directory, not only a modeled
	// capacity record. Never fill the host root filesystem to simulate ENOSPC.
	if err := CheckCapacity([]FilesystemBudget{{Mount: t.TempDir(), LimitBytes: 8192 * GiB}}); err == nil {
		t.Fatal("ordinary directory accepted as finite workspace")
	}
}
func TestCapacityIsPerFilesystemNotAggregate(t *testing.T) {
	b := FilesystemBudget{LimitBytes: 4 * GiB, RequiredBytes: 2 * GiB}
	st := unix.Statfs_t{Bsize: 4096, Blocks: uint64(4 * GiB / 4096), Bavail: uint64(3 * GiB / 4096)}
	if err := checkSpace(b, st); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*unix.Statfs_t){func(s *unix.Statfs_t) { s.Blocks++ }, func(s *unix.Statfs_t) { s.Bavail = uint64(GiB / 4096) }} {
		other := st
		change(&other)
		if checkSpace(b, other) == nil {
			t.Fatal("filesystem hard limit/free reservation ignored")
		}
	}
	n := Defaults().Native
	required, err := CaptureCapacity(n)
	if err != nil {
		t.Fatal(err)
	}
	if required <= n.MaxBackupBytes*2+n.MaxBootstrapWALBytes+GiB {
		t.Fatal("capture omitted spool/metadata/spare reserve")
	}
	if _, err := CaptureCapacity(Native{MaxBackupBytes: 1 << 62}); err == nil {
		t.Fatal("overflow budget accepted")
	}
}
