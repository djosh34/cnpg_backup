package cnpgi

import (
	"io/fs"
	"os"
	"path/filepath"

	"github.com/djosh34/cnpg_backup/internal/repository"
	"golang.org/x/sys/unix"
)

const retentionWorkspacePath = "/cnpg-backup/retention"
const retentionMountBytes int64 = 512 << 20

// The sole serial retention worker reserves its entire peak before admission.
// Account for prior crashed local spools; never delete/adopt another owner's
// state. emptyDir eviction is NOT the write limiter: the repository caps the
// catalog and each sequential manifest/history/control file in Go.
func newRetentionWorkspace(root string) (string, error) {
	var stat unix.Statfs_t
	if e := unix.Statfs(root, &stat); e != nil {
		return "", e
	}
	if stat.Bsize <= 0 || stat.Bavail < uint64(repository.GCWorkspaceBytes)/uint64(stat.Bsize)+1 {
		return "", repository.ErrCapacity
	}
	used, entries := int64(0), 0
	e := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		entries++
		if entries > 1024 {
			return repository.ErrCapacity
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return repository.ErrInvalid
		}
		// 8KiB per file/directory covers allocation rounding and metadata.
		used += info.Size() + 8192
		if used > retentionMountBytes-repository.GCWorkspaceBytes {
			return repository.ErrCapacity
		}
		return nil
	})
	if e != nil {
		return "", e
	}
	return os.MkdirTemp(root, "batch-")
}
