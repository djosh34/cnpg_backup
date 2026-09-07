// Copyright 2026 cnpg_backup contributors. All rights reserved.
package main

import (
	"fmt"
	"io"
	"os"
	"runtime"
)

var revision = "development"

func run(args []string, out, errOut io.Writer) int {
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
		case "manager", "instance", "recovery-job", "recovery-guard":
			fmt.Fprintln(errOut, args[0]+": not implemented")
			return 2
		}
	}
	fmt.Fprintln(errOut, "usage: cnpg-backup version|manager|instance|recovery-job|recovery-guard|wal-fetch (service modes not implemented)")
	return 2
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
