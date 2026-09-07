package postgres

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Optional local SQL contract probe; the mandatory CNPG harness executes this
// same query with actual streaming_replica certificates in the shipped image.
func TestNativeSQLWireTypes(t *testing.T) {
	bin, socket := os.Getenv("CNPG_TEST_PG_BIN"), os.Getenv("CNPG_TEST_PG_SOCKET")
	if bin == "" || socket == "" {
		t.Skip("local native fixture not selected; real CNPG harness covers SQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	env := []string{"LANG=C", "LC_ALL=C", "LD_LIBRARY_PATH=" + os.Getenv("CNPG_TEST_PG_LIBS")}
	b, e := runNative(ctx, env, filepath.Join(bin, "psql"), "-XAt", "-h", socket, "-d", "postgres", "-c", preflightSQL)
	if e != nil {
		t.Fatal(e)
	}
	var s serverState
	if e = json.Unmarshal(b, &s); e != nil {
		t.Fatal("public metadata query wire type mismatch:", e)
	}
	if len(s.TablespaceOIDs) == 0 {
		t.Fatal("fixture lacks required tablespace OID regression")
	}
	if s.Version != 180006 || s.Postmaster == "" || s.Clock == "" {
		t.Fatal("native identity fields absent")
	}
}
