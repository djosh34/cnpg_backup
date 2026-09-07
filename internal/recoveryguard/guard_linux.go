// Copyright 2026 cnpg_backup contributors. All rights reserved.
package recoveryguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// Run MUST be main-container PID1 in its private PID namespace. It starts no
// arbitrary command and has one wait4 owner for the entire namespace, including
// setsid/double-fork descendants which escape process-group-only supervision.
// Returning an error retains all markers. Main must exit immediately afterwards.
func Run(ctx context.Context, c Config, pod string, argv []string) (int, error) {
	if os.Getpid() != 1 {
		return 2, errors.New("recovery-guard requires PID1 in a private PID namespace")
	}
	owner, err := c.owner(pod, NewUUID())
	if err != nil {
		return 2, err
	}
	if err = c.ValidateArgv(argv); err != nil {
		return 2, err
	}
	held, err := acquire(c, owner)
	if err != nil {
		return 2, err
	}
	defer held.close()

	// Begin is bounded independently of the lifetime context. Once established,
	// that same stream stays alive through replay and Drain (no expiring lease).
	sessionCtx, cancelSession := context.WithCancel(context.Background())
	defer cancelSession()
	beginTimer := time.AfterFunc(15*time.Second, cancelSession)
	stop := context.AfterFunc(ctx, cancelSession)
	session, err := Begin(sessionCtx, SocketPath, owner)
	if !beginTimer.Stop() || err != nil || ctx.Err() != nil {
		stop()
		if session != nil {
			session.Close()
		}
		return 2, ErrUncertain
	}
	stop()
	defer session.Close()

	child, err := os.StartProcess(argv[0], argv, &os.ProcAttr{
		Env: os.Environ(), Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
	})
	if err != nil {
		return 2, fmt.Errorf("%w: unable to start CNPG", ErrUncertain)
	}
	pid := child.Pid
	defer child.Release()
	type reap struct {
		pid    int
		status unix.WaitStatus
		err    error
	}
	reaped := make(chan reap)
	go func() {
		for {
			var status unix.WaitStatus
			n, err := unix.Wait4(-1, &status, 0, nil)
			if errors.Is(err, unix.EINTR) {
				continue
			}
			reaped <- reap{n, status, err}
			if err != nil {
				return
			}
		}
	}()
	exit := 2
	mainExited, stopping, uncertain := false, false, false
	var killAfter, reapDeadline <-chan time.Time
	var killTimer, deadlineTimer *time.Timer
	defer func() {
		if killTimer != nil {
			killTimer.Stop()
		}
		if deadlineTimer != nil {
			deadlineTimer.Stop()
		}
	}()
	terminate := func() {
		if stopping {
			return
		}
		stopping = true
		// kill(-1) excludes this PID1 and cannot reach the separate sidecar namespace.
		if err := unix.Kill(-1, unix.SIGTERM); err != nil && !errors.Is(err, unix.ESRCH) {
			uncertain = true
		}
		killTimer = time.NewTimer(5 * time.Second)
		killAfter = killTimer.C
		deadlineTimer = time.NewTimer(30 * time.Second)
		reapDeadline = deadlineTimer.C
	}
	ctxDone, sessionDone := ctx.Done(), (<-chan error)(session.done)
	for {
		select {
		case <-ctxDone:
			uncertain = true
			ctxDone = nil
			terminate()
		case <-sessionDone:
			// EOF, error OR an unsolicited Drain acknowledgment is uncertainty.
			uncertain = true
			sessionDone = nil
			terminate()
		case <-killAfter:
			if err := unix.Kill(-1, unix.SIGKILL); err != nil && !errors.Is(err, unix.ESRCH) {
				uncertain = true
			}
			killAfter = nil
		case <-reapDeadline:
			return 2, ErrUncertain // no timer ever authorizes unlocking/marker removal
		case result := <-reaped:
			if errors.Is(result.err, unix.ECHILD) {
				if !mainExited || uncertain || ctx.Err() != nil {
					return 2, ErrUncertain
				}
				drainCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				err := session.drain(drainCtx)
				cancel()
				if err != nil || ctx.Err() != nil {
					return 2, ErrUncertain
				}
				if err := held.release(); err != nil {
					return 2, err
				}
				return exit, nil
			}
			if result.err != nil {
				return 2, ErrUncertain
			}
			if result.pid == pid {
				mainExited = true
				if result.status.Exited() {
					exit = result.status.ExitStatus()
				} else {
					exit = 128 + int(result.status.Signal())
				}
				terminate()
			}
		}
	}
}
