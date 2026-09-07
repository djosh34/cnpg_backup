// Copyright 2026 cnpg_backup contributors. All rights reserved.
package configuration

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// FilesystemBudget is an internal operation plan, not user assertions about a
// StorageClass. LimitBytes comes from the target PVC/workspace declaration;
// RequiredBytes includes the phase's allocation and free-space margin. Native
// writers must pass this check immediately before launch while owning the target
// and workspace. Polling is supplemental: the dedicated filesystem is the stop.
// Quota-only subdirectories/NFS are not initially supported without a verifier.
type FilesystemBudget struct {
	Mount         string `json:"mount"`
	LimitBytes    int64  `json:"limitBytes"`
	RequiredBytes int64  `json:"requiredBytes"`
}

type mountRecord struct {
	device, root, path, fs string
	readOnly               bool
}

func mounts(reader io.Reader) ([]mountRecord, error) {
	scanner := bufio.NewScanner(io.LimitReader(reader, 1<<20+1))
	scanner.Buffer(make([]byte, 4096), 64<<10)
	result := []mountRecord{}
	total := 0
	for scanner.Scan() {
		total += len(scanner.Bytes()) + 1
		if total > 1<<20 || len(result) >= 4096 {
			return nil, errors.New("mount inventory exceeds limit")
		}
		fields := strings.Fields(scanner.Text())
		separator := -1
		for i, f := range fields {
			if f == "-" {
				separator = i
				break
			}
		}
		if len(fields) < 10 || separator < 6 || separator+3 >= len(fields) {
			return nil, errors.New("invalid mount inventory")
		}
		decode := func(v string) string {
			return strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`).Replace(v)
		}
		result = append(result, mountRecord{fields[2], decode(fields[3]), decode(fields[4]), fields[separator+1], strings.Contains(","+fields[5]+",", ",ro,") || strings.Contains(","+fields[separator+3]+",", ",ro,")})
	}
	return result, scanner.Err()
}

func finiteMount(inventory []mountRecord, path string) (mountRecord, error) {
	var found mountRecord
	count := 0
	for _, m := range inventory {
		if m.path == path {
			found = m
			count++
		}
	}
	if count != 1 || found.root != "/" || (found.fs != "ext4" && found.fs != "xfs") || found.readOnly || strings.HasPrefix(found.device, "0:") {
		return found, errors.New("require a dedicated writable finite ext4/xfs filesystem, not local-path/emptyDir/subpath/NFS")
	}
	return found, nil
}

func checkSpace(b FilesystemBudget, st unix.Statfs_t) error {
	if b.LimitBytes <= 0 || b.LimitBytes > 8192*GiB || b.RequiredBytes < 0 || b.RequiredBytes > b.LimitBytes || st.Bsize <= 0 || st.Blocks > uint64(b.LimitBytes)/uint64(st.Bsize) {
		return errors.New("actual filesystem exceeds declared hard limit or invalid phase budget")
	}
	if st.Bavail > st.Blocks || st.Bavail < uint64((b.RequiredBytes+st.Bsize-1)/st.Bsize) {
		return errors.New("insufficient per-filesystem free capacity")
	}
	return nil
}

// CheckCapacity verifies kernel mount/device identity and actual finite capacity;
// PVC requests and emptyDir.sizeLimit alone are never treated as quotas.
func CheckCapacity(budgets []FilesystemBudget) error {
	if len(budgets) < 1 || len(budgets) > 67 {
		return errors.New("invalid filesystem budget count")
	}
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return errors.New("mount inventory unavailable")
	}
	inventory, err := mounts(file)
	file.Close()
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, b := range budgets {
		if !filepath.IsAbs(b.Mount) || filepath.Clean(b.Mount) != b.Mount || b.Mount == "/" {
			return errors.New("invalid capacity mount")
		}
		resolved, err := filepath.EvalSymlinks(b.Mount)
		if err != nil || resolved != b.Mount {
			return errors.New("capacity mount aliases are unsupported")
		}
		mount, err := finiteMount(inventory, b.Mount)
		if err != nil {
			return fmt.Errorf("capacity %s: %w", b.Mount, err)
		}
		if seen[mount.device] {
			return errors.New("workspace and targets must have independent filesystem limits")
		}
		seen[mount.device] = true
		fd, err := unix.Open(b.Mount, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return errors.New("capacity mount unavailable")
		}
		var fs unix.Statfs_t
		var st unix.Stat_t
		err = unix.Fstatfs(fd, &fs)
		if err == nil {
			err = unix.Fstat(fd, &st)
		}
		unix.Close(fd)
		device := strconv.FormatUint(uint64(unix.Major(st.Dev)), 10) + ":" + strconv.FormatUint(uint64(unix.Minor(st.Dev)), 10)
		if err != nil || device != mount.device || (fs.Type != unix.EXT4_SUPER_MAGIC && fs.Type != unix.XFS_SUPER_MAGIC) {
			return errors.New("capacity mount changed or unsupported filesystem")
		}
		if err = checkSpace(b, fs); err != nil {
			return fmt.Errorf("capacity %s: %w", b.Mount, err)
		}
	}
	return nil
}

func CapacityMargin(bytes int64) int64 { return max(GiB, (bytes+9)/10) }

// CaptureCapacity includes raw archives, bounded compression spool, extracted
// WAL, worst-supported input entry metadata and the independent spare margin.
func CaptureCapacity(n Native) (int64, error) {
	if n.MaxBackupBytes < 1 || n.MaxBackupBytes > 1024*GiB || n.MaxBootstrapWALBytes < 1 || n.MaxBootstrapWALBytes > 256*GiB || n.MaxBootstrapWALBytes > n.MaxBackupBytes {
		return 0, errors.New("invalid native capacity budgets")
	}
	a := n.MaxBackupBytes
	spool := a + max(GiB, (a+99)/100)
	peak := a + spool + n.MaxBootstrapWALBytes + 8192*100000
	return peak + CapacityMargin(peak), nil
}

func LoadCapacity(directory string) ([]FilesystemBudget, error) {
	root, err := Projection(directory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	data, err := Read(root, "capacity.json", 64<<10)
	if err != nil {
		return nil, err
	}
	var budgets []FilesystemBudget
	if err = StrictJSON(data, &budgets); err != nil {
		return nil, err
	}
	return budgets, nil
}
