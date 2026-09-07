// Copyright 2026 cnpg_backup contributors. All rights reserved.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/djosh34/cnpg_backup/internal/cnpgi"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
)

var revision = "development"

func run(args []string, out, errOut io.Writer) (exit int) {
	defer func() {
		if recover() != nil {
			fmt.Fprintln(errOut, "cnpg-backup: unexpected internal failure")
			exit = 2
			if len(args) > 0 && args[0] == "wal-fetch" {
				exit = 255
			}
		}
	}()
	if len(args) == 1 && (args[0] == "version" || args[0] == "--version") {
		fmt.Fprintf(out, "cnpg-backup development revision=%s go=%s %s/%s\n", revision, runtime.Version(), runtime.GOOS, runtime.GOARCH)
		return 0
	}
	if len(args) > 0 {
		switch args[0] {
		case "wal-fetch":
			// Ordinary exit 1 means allowed WAL absence to PostgreSQL. Until the
			// real helper is implemented, every invocation must be fatal.
			fmt.Fprintln(errOut, "wal-fetch: not implemented")
			return 255
		case "instance", "recovery-job":
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			var err error
			if len(args) == 2 && args[1] == "--probe" {
				err = cnpgi.Probe(ctx)
			} else if len(args) == 1 {
				err = cnpgi.RunSidecar(ctx, args[0] == "recovery-job", revision)
			} else {
				fmt.Fprintln(errOut, "invalid sidecar arguments")
				return 2
			}
			if err != nil {
				fmt.Fprintln(errOut, "sidecar: startup/probe failed")
				return 2
			}
			return 0
		case "recovery-guard":
			if len(args) < 3 || args[1] != "--" {
				fmt.Fprintln(errOut, "invalid recovery-guard arguments")
				return 2
			}
			config, err := recoveryguard.LoadConfig(recoveryguard.ConfigPath)
			if err != nil {
				fmt.Fprintln(errOut, "recovery-guard: invalid configuration")
				return 2
			}
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			code, err := recoveryguard.Run(ctx, config, os.Getenv("POD_UID"), args[2:])
			if err != nil {
				fmt.Fprintln(errOut, "recovery-guard:", err)
			}
			return code
		case "manager":
			fmt.Fprintln(errOut, "manager: not implemented")
			return 2
		}
	}
	fmt.Fprintln(errOut, "usage: cnpg-backup version|manager|instance|recovery-job|recovery-guard|wal-fetch (lifecycle and data services not implemented)")
	return 2
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
