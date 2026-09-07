// Copyright 2026 cnpg_backup contributors. All rights reserved.
package postgres

import (
	"errors"
	"os"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// NativeWorkspace is exclusively sidecar-owned scratch, not source/target data
// or remote admission state. The permanent lock/owner live outside this subtree.
const NativeWorkspace = workspace + "/native"

var nativeWorkspaceLock atomic.Pointer[os.File]

// AcquireWorkspace runs before callback admission, with the sidecar as PID1 in
// its private namespace. The Pod-bound workspace cannot transfer to a different
// Pod. The lock is inherited by every approved native command (and PG's forked
// WAL child), so even a detached survivor excludes a replacement until it exits.
// Container PID1 death additionally kills all namespace descendants, not merely
// the command's process group. Legacy/unmarked scratch is deliberately untouched.
func AcquireWorkspace(pod string) (*os.File, error) {
	if os.Getpid() != 1 {
		return nil, errors.New("native workspace requires private-namespace PID1")
	}
	f, err := acquireWorkspaceAt(workspace, pod)
	if err != nil {
		return nil, err
	}
	nativeWorkspaceLock.Store(f)
	return f, nil
}

func acquireWorkspaceAt(directory, pod string) (*os.File, error) {
	if pod == "" || len(pod) > 128 {
		return nil, ErrInput
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	f, err := root.OpenFile("native.lock", os.O_RDWR|os.O_CREATE|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			f.Close()
		}
	}()
	st, err := f.Stat()
	var stat unix.Stat_t
	if err != nil || unix.Fstat(int(f.Fd()), &stat) != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || stat.Nlink != 1 || int(stat.Uid) != os.Geteuid() {
		return nil, ErrInput
	}
	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, errors.New("native workspace still owned by a sidecar or native descendant")
	}
	owner, err := root.OpenFile("native.owner", os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if os.IsNotExist(err) {
		// Never adopt an existing unmarked directory, even if its name is reserved.
		if _, e := root.Lstat("native"); !os.IsNotExist(e) {
			return nil, ErrInput
		}
		owner, err = root.OpenFile("native.owner", os.O_WRONLY|os.O_CREATE|os.O_EXCL|unix.O_NOFOLLOW, 0600)
		if err != nil {
			return nil, err
		}
		_, err = owner.WriteString(pod + "\n")
		if err == nil {
			err = owner.Sync()
		}
		owner.Close()
		if err != nil {
			return nil, err
		}
	} else {
		if err != nil {
			return nil, err
		}
		var b [130]byte
		n, e := owner.Read(b[:])
		st, se := owner.Stat()
		owner.Close()
		if e != nil || se != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || string(b[:n]) != pod+"\n" {
			return nil, ErrInput
		}
	}
	if st, e := root.Lstat("native"); e == nil {
		if !st.IsDir() || st.Mode().Perm() != 0700 {
			return nil, ErrInput
		}
	} else if !os.IsNotExist(e) {
		return nil, e
	}
	if err = root.RemoveAll("native"); err != nil {
		return nil, err
	}
	if err = root.Mkdir("native", 0700); err != nil {
		return nil, err
	}
	d, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	err = d.Sync()
	d.Close()
	if err != nil {
		return nil, err
	}
	success = true
	return f, nil
}
