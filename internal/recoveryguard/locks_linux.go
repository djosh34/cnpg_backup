// Copyright 2026 cnpg_backup contributors. All rights reserved.
package recoveryguard

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"

	"golang.org/x/sys/unix"
)

var ErrBusy = errors.New("TargetOwnershipBusy")
var ErrUncertain = errors.New("TargetOwnershipUncertain: retry with a fresh Cluster and all fresh target PVCs")

type marker struct {
	Owner   Owner    `json:"owner"`
	Targets []Target `json:"targets"`
}
type volumeLock struct{ dir, lock *os.File }
type ownership struct {
	volumes []volumeLock
	marker  marker
}

func openAt(dir *os.File, name string, flags int, mode uint32) (*os.File, error) {
	fd, err := unix.Openat(int(dir.Fd()), name, flags|unix.O_NOFOLLOW|unix.O_CLOEXEC, mode)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}
func privateRegular(f *os.File) error {
	var s unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &s); err != nil {
		return err
	}
	if s.Mode&unix.S_IFMT != unix.S_IFREG || s.Nlink != 1 || int(s.Uid) != os.Geteuid() || s.Mode&0077 != 0 {
		return ErrUncertain
	}
	return nil
}
func openVolume(path string) (volumeLock, error) {
	fd, err := unix.Open(path, unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_RDONLY, 0)
	if err != nil {
		return volumeLock{}, err
	}
	root := os.NewFile(uintptr(fd), path)
	defer root.Close()
	if err = unix.Mkdirat(fd, ".cnpg-backup", 0700); err != nil && !errors.Is(err, unix.EEXIST) {
		return volumeLock{}, err
	}
	dir, err := openAt(root, ".cnpg-backup", unix.O_DIRECTORY|unix.O_RDONLY, 0)
	if err != nil {
		return volumeLock{}, err
	}
	ok := false
	defer func() {
		if !ok {
			dir.Close()
		}
	}()
	var stat unix.Stat_t
	if err = unix.Fstat(int(dir.Fd()), &stat); err != nil {
		return volumeLock{}, err
	}
	if int(stat.Uid) != os.Geteuid() || stat.Mode&0077 != 0 {
		return volumeLock{}, ErrUncertain
	}
	if err = root.Sync(); err != nil {
		return volumeLock{}, err
	}
	lock, err := openAt(dir, "lock", unix.O_CREAT|unix.O_RDWR|unix.O_NONBLOCK, 0600)
	if err != nil {
		return volumeLock{}, err
	}
	if err = privateRegular(lock); err != nil {
		lock.Close()
		return volumeLock{}, err
	}
	ok = true
	return volumeLock{dir, lock}, nil
}
func (v volumeLock) close() { v.lock.Close(); v.dir.Close() }

// acquire never removes markers, even on partial failure. Lock files are
// permanent; unlinking one would let two processes lock different inodes.
func acquire(c Config, owner Owner) (*ownership, error) {
	held := &ownership{marker: marker{owner, c.sortedTargets()}}
	ok := false
	defer func() {
		if !ok {
			held.close()
		}
	}()
	for _, t := range held.marker.Targets {
		v, err := openVolume(t.Mount)
		if err != nil {
			return nil, err
		}
		held.volumes = append(held.volumes, v)
		if err = unix.Flock(int(v.lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			if errors.Is(err, unix.EWOULDBLOCK) {
				return nil, ErrBusy
			}
			return nil, err
		}
	}
	// Check the whole set before creating any fresh marker.
	for _, v := range held.volumes {
		var s unix.Stat_t
		err := unix.Fstatat(int(v.dir.Fd()), "owner.json", &s, unix.AT_SYMLINK_NOFOLLOW)
		if err == nil {
			return nil, ErrUncertain
		}
		if !errors.Is(err, unix.ENOENT) {
			return nil, err
		}
	}
	data, err := json.Marshal(held.marker)
	if err != nil {
		return nil, err
	}
	for _, v := range held.volumes {
		f, err := openAt(v.dir, "owner.json", unix.O_CREAT|unix.O_EXCL|unix.O_WRONLY, 0600)
		if err != nil {
			return nil, ErrUncertain
		}
		_, werr := f.Write(data)
		serr := f.Sync()
		cerr := f.Close()
		if errors.Join(werr, serr, cerr, v.dir.Sync()) != nil {
			return nil, ErrUncertain
		}
	}
	ok = true
	return held, nil
}
func readMarker(v volumeLock) (marker, error) {
	var m marker
	f, err := openAt(v.dir, "owner.json", unix.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return m, ErrUncertain
	}
	defer f.Close()
	if err = privateRegular(f); err != nil {
		return m, err
	}
	b, err := io.ReadAll(io.LimitReader(f, maxMessage+1))
	if err != nil {
		return m, err
	}
	err = Decode(b, &m)
	return m, err
}

// verifyOwner is called by Begin, before admission. It verifies both the durable
// whole-set marker and live lock exclusion, not merely the recorded PID.
func verifyOwner(c Config, owner Owner) error {
	expected := marker{owner, c.sortedTargets()}
	for _, t := range expected.Targets {
		v, err := openVolume(t.Mount)
		if err != nil {
			return ErrUncertain
		}
		m, err := readMarker(v)
		if err == nil && !reflect.DeepEqual(m, expected) {
			err = ErrUncertain
		}
		if err == nil {
			lockErr := unix.Flock(int(v.lock.Fd()), unix.LOCK_EX|unix.LOCK_NB)
			if !errors.Is(lockErr, unix.EWOULDBLOCK) {
				err = ErrUncertain
			}
		}
		v.close()
		if err != nil {
			return err
		}
	}
	return nil
}

// release is intentionally private. Only PID1's completed descendant reap and
// the original sidecar's acknowledged terminal Drain authorize this call.
func (h *ownership) release() error {
	for _, v := range h.volumes {
		m, err := readMarker(v)
		if err != nil || !reflect.DeepEqual(m, h.marker) {
			return ErrUncertain
		}
	}
	for _, v := range h.volumes {
		if err := unix.Unlinkat(int(v.dir.Fd()), "owner.json", 0); err != nil {
			return fmt.Errorf("%w: marker removal", ErrUncertain)
		}
		if err := v.dir.Sync(); err != nil {
			return fmt.Errorf("%w: marker directory sync", ErrUncertain)
		}
	}
	return nil
}
func (h *ownership) close() {
	for _, v := range h.volumes {
		v.close()
	}
	h.volumes = nil
}
