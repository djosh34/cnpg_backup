// Test-only driver, not shipped in either runtime image. Executes the production
// manifest scanner and direct Go WAL verifier inside the shell-free subject.
package main

import (
	"context"
	"fmt"
	"github.com/djosh34/cnpg_backup/internal/postgres"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 3 {
		os.Exit(2)
	}
	f, e := os.Open(os.Args[1])
	if e != nil {
		os.Exit(2)
	}
	m, e := postgres.ScanManifest(f)
	f.Close()
	if e != nil {
		fmt.Println("manifest rejected")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if e = postgres.VerifyWAL(ctx, os.Args[2], m.Ranges); e != nil {
		fmt.Println("WAL rejected")
		os.Exit(2)
	}
	fmt.Println("verified native manifest and every accepted WAL range")
}
