// Test-only Unix RPC driver. Never included in either product image.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	wire "github.com/cloudnative-pg/cnpg-i/pkg/wal"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() { os.Exit(run()) }
func run() int {
	if len(os.Args) < 4 || len(os.Args) > 5 {
		return 255
	}
	definition, e := os.ReadFile(os.Args[1])
	if e != nil || len(definition) > 1<<20 {
		return 255
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	connection, e := grpc.NewClient("unix://"+recoveryguard.SocketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		return 255
	}
	defer connection.Close()
	client := wire.NewWALClient(connection)
	switch os.Args[2] {
	case "archive":
		if len(os.Args) != 4 {
			return 255
		}
		check := false // New optional false flag may never disable writer/no-clobber.
		_, e = client.Archive(ctx, &wire.WALArchiveRequest{ClusterDefinition: definition, SourceFileName: os.Args[3], CheckEmptyWalArchive: &check})
	case "restore":
		if len(os.Args) != 5 {
			return 255
		}
		_, e = client.Restore(ctx, &wire.WALRestoreRequest{ClusterDefinition: definition, SourceWalName: os.Args[3], DestinationFileName: os.Args[4], Mode: wire.WALRestoreRequest_MODE_REWIND})
	default:
		return 255
	}
	code := status.Code(e)
	fmt.Println(code.String()) // bounded code only; no request/SDK text
	if code == codes.OK {
		return 0
	}
	if code == codes.NotFound {
		return 1
	}
	return 255
}
