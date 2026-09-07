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
	"github.com/djosh34/cnpg_backup/internal/postgres"
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
			code := cnpgi.WALFetch(context.Background(), args[1:])
			if code == 255 {
				fmt.Fprintln(errOut, "wal-fetch: recovery fetch failed")
			}
			return code
		case "instance", "recovery-job":
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			var err error
			if len(args) == 2 && args[1] == "--prepare-socket" && args[0] == "instance" {
				err = cnpgi.PrepareSocket()
			} else if len(args) == 2 && args[1] == "--check-native" && args[0] == "instance" {
				err = postgres.Check(ctx, "/cnpg-backup/projection")
			} else if len(args) == 2 && args[1] == "--check-capacity" {
				err = cnpgi.CheckMountedCapacity()
			} else if len(args) == 2 && args[1] == "--probe" {
				err = cnpgi.Probe(ctx)
			} else if len(args) == 1 {
				err = cnpgi.RunSidecar(ctx, args[0] == "recovery-job", revision)
			} else {
				fmt.Fprintln(errOut, "invalid sidecar arguments")
				return 2
			}
			if err != nil {
				fmt.Fprintln(errOut, "sidecar:", err)
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
			if len(args) != 1 {
				fmt.Fprintln(errOut, "invalid manager arguments")
				return 2
			}
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			if err := cnpgi.RunManager(ctx, revision); err != nil {
				fmt.Fprintln(errOut, "manager: configuration or service failure")
				return 2
			}
			return 0
		}
	}
	fmt.Fprintln(errOut, "usage: cnpg-backup version|manager|instance|recovery-job|recovery-guard|wal-fetch (PG18 full backup and protected recovery; differential not implemented)")
	return 2
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
