// Test-only executable. NEVER copied to a product image. It replaces only the
// CNPG command in namespace supervision tests, NOT real CNPG acceptance tests.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/djosh34/cnpg_backup/internal/cnpgi"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
)

func touch(path string) { must(os.WriteFile(path, []byte("observed\n"), 0600)) }
func must(err error) {
	if err != nil {
		panic(err)
	}
}
func wait(path string) {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	panic("test barrier timed out: " + path)
}
func main() {
	if len(os.Args) > 1 && os.Args[1] == "detached" {
		signal.Ignore(syscall.SIGTERM)
		touch("/test/detached-ready")
		for {
			time.Sleep(time.Second)
		}
	}
	if len(os.Args) > 1 && os.Args[1] == "sidecar-writes" {
		sidecarWrites()
		return
	}
	if len(os.Args) < 3 || os.Args[1] != "instance" || os.Args[2] != "restore" {
		panic("unexpected fixture command")
	}
	if os.Getpid() == 1 {
		panic("original argv was not wrapped")
	}
	data, err := json.Marshal(os.Args)
	must(err)
	must(os.WriteFile("/test/original-argv.json", data, 0600))
	config, err := recoveryguard.LoadConfig(recoveryguard.ConfigPath)
	must(err)
	for _, target := range config.Targets {
		// CNPG-like preflight mutation on EVERY target. Replacement tests compare
		// these durable files; a preflight that reached here cannot pass unnoticed.
		if _, err := os.Stat(filepath.Join(target.Mount, ".cnpg-backup/owner.json")); err != nil {
			panic("preflight preceded durable ownership")
		}
		f, err := os.OpenFile(filepath.Join(target.Mount, "preflight"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		must(err)
		_, err = f.WriteString(os.Getenv("POD_UID") + "\n")
		must(err)
		must(f.Sync())
		must(f.Close())
	}
	touch("/test/main-started")
	if os.Getenv("SCENARIO") == "detached" {
		cmd := exec.Command("/controller/manager", "detached")
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		must(cmd.Start())
		wait("/test/detached-ready")
		// No Wait: actual orphan adoption by namespace PID1 is required.
	}
	wait("/test/main-release")
	if os.Getenv("SCENARIO") == "exit7" {
		os.Exit(7)
	}
}
func sidecarWrites() {
	config, err := recoveryguard.LoadConfig(recoveryguard.ConfigPath)
	must(err)
	admission, err := recoveryguard.NewAdmission(config, os.Getenv("POD_UID"))
	must(err)
	if _, err := admission.Admit(recoveryguard.Tuple{}); err == nil {
		panic("writes admitted before Begin")
	}
	touch("/test/before-begin-rejected")
	must(recoveryguard.InstallHelper("/usr/local/bin/cnpg-backup", recoveryguard.HelperPath))
	listener, err := net.Listen("unix", recoveryguard.SocketPath)
	must(err)
	must(os.Chmod(recoveryguard.SocketPath, 0600))
	go func() {
		wait("/test/main-started")
		tuple, err := admission.ActiveTuple()
		must(err)
		task, err := admission.Admit(tuple)
		must(err)
		touch("/test/write-pending")
		<-task.Context.Done()
		if _, err := admission.Admit(tuple); err == nil {
			panic("delayed callback after Drain admitted")
		}
		touch("/test/draining")
		// Deliberately uncancelable outstanding local publication: acknowledgment
		// must wait for the actual write, not merely a canceled context.
		wait("/test/write-release")
		f, err := os.OpenFile(filepath.Join(config.Targets[0].Mount, "delayed-write"), os.O_CREATE|os.O_WRONLY, 0600)
		must(err)
		_, err = fmt.Fprintln(f, "completed after drain began")
		must(err)
		must(f.Sync())
		must(f.Close())
		task.Done()
		if _, err := admission.Admit(tuple); err == nil {
			panic("terminal callback admitted")
		}
		touch("/test/stale-rejected")
	}()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	must(cnpgi.Serve(ctx, listener, admission, "test-only"))
}
