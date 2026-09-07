// Test-only actor for the disposable CNPG native capture harness. Never shipped.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	wire "github.com/cloudnative-pg/cnpg-i/pkg/backup"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const reserve = "/cnpg-backup/work/full-fixture-reserve"

func fail() { fmt.Println("test actor failed"); os.Exit(2) }
func native() []int {
	entries, e := os.ReadDir("/proc")
	if e != nil {
		fail()
	}
	var pids []int
	for _, entry := range entries {
		pid, e := strconv.Atoi(entry.Name())
		if e != nil {
			continue
		}
		b, e := os.ReadFile(filepath.Join("/proc", entry.Name(), "comm"))
		if e != nil {
			continue
		}
		switch strings.TrimSpace(string(b)) {
		case "pg_basebackup", "pg_verifybackup", "pg_waldump":
			pids = append(pids, pid)
		}
	}
	return pids
}
func main() {
	if len(os.Args) < 2 {
		fail()
	}
	// Never let the process-group actor run against a developer host or the
	// PostgreSQL container. All commands require the selected sidecar PID NS.
	comm, err := os.ReadFile("/proc/1/comm")
	if err != nil || strings.TrimSpace(string(comm)) != "cnpg-backup" || os.Getenv("POD_UID") == "" {
		fail()
	}
	switch os.Args[1] {
	case "native":
		json.NewEncoder(os.Stdout).Encode(native())
	case "pause-native":
		pids := native()
		if len(pids) == 0 {
			fail()
		}
		for _, pid := range pids {
			pg, e := syscall.Getpgid(pid)
			if e != nil || pg <= 1 {
				fail()
			}
			if syscall.Kill(-pg, syscall.SIGSTOP) != nil {
				fail()
			}
		}
		json.NewEncoder(os.Stdout).Encode(pids)
	case "signal-sidecar":
		if len(os.Args) != 3 {
			fail()
		}
		// Namespace-local SIGKILL cannot terminate namespace init. The harness
		// uses an ancestor-namespace CRI PID for the independent death case.
		if os.Args[2] != "TERM" {
			fail()
		}
		if syscall.Kill(1, syscall.SIGTERM) != nil {
			fail()
		}
	case "oom":
		group, e := os.ReadFile("/sys/fs/cgroup/memory.oom.group")
		if e != nil || strings.TrimSpace(string(group)) != "1" {
			fail()
		}
		maximum, e := os.ReadFile("/sys/fs/cgroup/memory.max")
		if e != nil {
			fail()
		}
		limit, e := strconv.ParseInt(strings.TrimSpace(string(maximum)), 10, 64)
		if e != nil || limit < 256<<20 || limit > 3<<30 {
			fail()
		}
		json.NewEncoder(os.Stdout).Encode(map[string]any{"oom_group": 1, "memory_max": limit, "bounded_cgroup_precondition": true})
		var memory [][]byte
		var allocated int64
		for allocated <= limit+64<<20 {
			b := make([]byte, 16<<20)
			for i := 0; i < len(b); i += 4096 {
				b[i] = 1
			}
			memory = append(memory, b)
			allocated += int64(len(b))
		}
		fmt.Println(len(memory))
		fail() // A surviving actor is not an OOM pass.
	case "fill-workspace":
		var fs unix.Statfs_t
		if unix.Statfs("/cnpg-backup/work", &fs) != nil || fs.Type != unix.EXT4_SUPER_MAGIC || fs.Blocks*uint64(fs.Bsize) > 8<<30 {
			fail()
		}
		f, e := os.OpenFile(reserve, os.O_WRONLY|os.O_CREATE|os.O_EXCL|unix.O_NOFOLLOW, 0600)
		if e != nil {
			fail()
		}
		defer f.Close()
		var allocated int64
		// Allocate actual inner-filesystem extents, not a sparse truncate or root
		// disk fill. Decrease allocation units to reach a real ENOSPC boundary.
		for _, chunk := range []int64{64 << 20, 1 << 20, 4096} {
			for {
				e = unix.Fallocate(int(f.Fd()), 0, allocated, chunk)
				if e == syscall.ENOSPC {
					break
				}
				if e != nil {
					fail()
				}
				allocated += chunk
				if allocated > 8<<30 {
					fail()
				}
			}
		}
		if unix.Statfs("/cnpg-backup/work", &fs) != nil {
			fail()
		}
		json.NewEncoder(os.Stdout).Encode(map[string]any{"allocated": allocated, "free": fs.Bavail * uint64(fs.Bsize), "enospc": true})
	case "clear-workspace":
		if os.Remove(reserve) != nil {
			fail()
		}
	case "backup":
		if len(os.Args) != 4 {
			fail()
		}
		cluster, e := os.ReadFile(os.Args[2])
		if e != nil {
			fail()
		}
		backup, e := os.ReadFile(os.Args[3])
		if e != nil {
			fail()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		conn, e := grpc.NewClient("unix://"+recoveryguard.SocketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if e != nil {
			fail()
		}
		defer conn.Close()
		result, e := wire.NewBackupClient(conn).Backup(ctx, &wire.BackupRequest{ClusterDefinition: cluster, BackupDefinition: backup, Parameters: map[string]string{"backupType": "full"}})
		if e != nil {
			fail()
		}
		json.NewEncoder(os.Stdout).Encode(result)
	default:
		fail()
	}
}
