package postgres

import (
	"github.com/djosh34/cnpg_backup/internal/configuration"
	"strings"
	"testing"
)

func TestActualServerSettingsAndManagedLayoutValidation(t *testing.T) {
	good := serverState{Version: 180006, Role: "streaming_replica", Primary: true, BlockSize: 8192, SegmentSize: 1 << 30, FullPageWrites: "on", WALLevel: "replica", SummarizeWAL: "on", SummaryMinutes: 14 * 24 * 60, ArchiveSeconds: 60, Tablespaces: map[string]string{"fast": "/var/lib/postgresql/tablespaces/fast/data"}}
	c := Connection{Host: "database-rw.test.svc", Tablespaces: good.Tablespaces}
	for name, change := range map[string]func(*serverState){
		"valid": func(*serverState) {}, "standby": func(s *serverState) { s.Primary = false }, "major": func(s *serverState) { s.Version = 170006 }, "minor": func(s *serverState) { s.Version = 180005 }, "role": func(s *serverState) { s.Role = "postgres" }, "ident-map": func(s *serverState) { s.Role = "cnpg_streaming_replica" }, "summary-disabled": func(s *serverState) { s.SummarizeWAL = "off" }, "summary-short": func(s *serverState) { s.SummaryMinutes = 24 * 60 }, "page-writes": func(s *serverState) { s.FullPageWrites = "off" }, "wal-level": func(s *serverState) { s.WALLevel = "minimal" }, "archive-timeout": func(s *serverState) { s.ArchiveSeconds = 0 }, "blocks": func(s *serverState) { s.BlockSize = 16384 }, "tablespace": func(s *serverState) { s.Tablespaces = map[string]string{"fast": "/unsafe"} },
	} {
		t.Run(name, func(t *testing.T) {
			s := good
			change(&s)
			err := validateState(s, c, configuration.Defaults().Native)
			if (err == nil) != (name == "valid") {
				t.Fatal(err)
			}
		})
	}
	for _, host := range []string{"localhost", "database.svc\nsslmode=disable", "db.svc;evil", "db.svc/other"} {
		if validHost(host) {
			t.Fatal("unsafe service identity", host)
		}
	}
}
func TestControlMetadataPhysicalLimitsAndBoundedNativeOutput(t *testing.T) {
	good := "Database block size: 8192\nBlocks per segment of large relation: 131072\nWAL block size: 8192\nDatabase system identifier: 1234\nBytes per WAL segment: 16777216\nData page checksum version: 1\n"
	if err := validateControl(good); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{strings.ReplaceAll(good, "8192", "16384"), strings.ReplaceAll(good, "16777216", "16000000"), strings.ReplaceAll(good, "checksum version: 1", "checksum version: 2"), ""} {
		if validateControl(text) == nil {
			t.Fatal("unsupported control metadata accepted")
		}
	}
	var output boundedOutput
	for i := 0; i < 8; i++ {
		if n, err := output.Write(make([]byte, 256<<10)); err != nil || n != 256<<10 {
			t.Fatal(n, err)
		}
	}
	if len(output.data) != 1<<20 || !output.exceeded {
		t.Fatal("unbounded native stdout")
	}
}
