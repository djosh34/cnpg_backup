// Copyright 2026 cnpg_backup contributors. All rights reserved.
package postgres

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/repository"
)

func TestLabelTimeLimits(t *testing.T) {
	began, stopped := "2026-09-07T05:00:00.9Z", "2026-09-07T05:00:01Z"
	for _, output := range []string{
		`{"matches":0,"started":null}`, `{"matches":2,"started":"2026-09-07T05:00:00Z"}`,
		`{"matches":1,"started":"2026-09-07T04:59:59Z"}`, `{"matches":1,"started":"2026-09-07T05:00:02Z"}`, `invalid`,
	} {
		if _, err := normalizedLabelTime([]byte(output), began, stopped); err == nil {
			t.Fatal("accepted", output)
		}
	}
	if got, err := normalizedLabelTime([]byte(`{"matches":1,"started":"2026-09-07T05:00:00Z"}`), began, stopped); err != nil || got != "2026-09-07T05:00:00Z" {
		t.Fatal(got, err)
	}
	for _, end := range []string{"invalid", "2026-09-07T04:00:00Z", "2026-09-07T11:00:02Z"} {
		if _, err := labelTimeQuery("2026-09-07 01:00:00 EDT", began, end, "America/New_York", began); err == nil {
			t.Fatal("unbounded clock interval", end)
		}
	}
	query, err := labelTimeQuery("2026-09-07 01:00:00 X'; SELECT 1; --", began, stopped, "X'\\", began)
	if err != nil || strings.Contains(query, "SELECT 1") || strings.Contains(query, "X'") {
		t.Fatal("SQL syntax injection", err)
	}
}

// Runs the actual SQL with a non-superuser on a disposable PG18 fixture. Only
// fixture setup changes log_timezone; product code changes session TimeZone.
// Foundation integration selects this test, in addition to real CNPG captures.
func TestNativeLabelTimeZones(t *testing.T) {
	bin, socket := os.Getenv("CNPG_TEST_PG_BIN"), os.Getenv("CNPG_TEST_PG_SOCKET")
	if bin == "" || socket == "" {
		t.Skip("disposable native SQL fixture not selected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	env := []string{"LANG=C", "LC_ALL=C", "LD_LIBRARY_PATH=" + os.Getenv("CNPG_TEST_PG_LIBS"), "PGPORT=" + os.Getenv("PGPORT")}
	sql := func(t *testing.T, query string, reader bool) string {
		t.Helper()
		args := []string{"-XAtq", "-h", socket, "-d", "postgres", "-v", "ON_ERROR_STOP=1"}
		user := os.Getenv("CNPG_TEST_PG_USER")
		if reader {
			user = os.Getenv("CNPG_TEST_PG_ROLE")
			if user == "" {
				user = "streaming_replica"
			}
		}
		if user != "" {
			args = append(args, "-U", user)
		}
		b, err := runNative(ctx, env, filepath.Join(bin, "psql"), append(args, "-c", query)...)
		if err != nil {
			t.Fatal("fixture SQL", err)
		}
		return strings.TrimSpace(string(b))
	}
	original := sql(t, "SHOW log_timezone", false)
	defer func() {
		sql(t, "ALTER SYSTEM SET log_timezone = '"+strings.ReplaceAll(original, "'", "''")+"'", false)
		sql(t, "SELECT pg_reload_conf()", false)
	}()
	state := func(t *testing.T) (string, string) {
		var s struct {
			Zone   string `json:"zone"`
			Loaded string `json:"loaded"`
		}
		b := sql(t, `SELECT json_build_object('zone',current_setting('log_timezone'),'loaded',to_char(pg_conf_load_time() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))`, true)
		if json.Unmarshal([]byte(b), &s) != nil {
			t.Fatal(b)
		}
		return s.Zone, s.Loaded
	}
	for _, tc := range []struct{ name, zone, label, began, stopped, want string }{
		{"utc", "UTC", "2026-09-07 05:00:00 UTC", "2026-09-07T05:00:00.9Z", "2026-09-07T05:00:01Z", "2026-09-07T05:00:00Z"},
		{"summer", "America/New_York", "2026-09-07 01:00:00 EDT", "2026-09-07T05:00:00Z", "2026-09-07T05:00:01Z", "2026-09-07T05:00:00Z"},
		{"winter", "America/New_York", "2026-01-07 01:00:00 EST", "2026-01-07T06:00:00Z", "2026-01-07T06:00:01Z", "2026-01-07T06:00:00Z"},
		{"fold-daylight", "America/New_York", "2026-11-01 01:30:00 EDT", "2026-11-01T05:00:00Z", "2026-11-01T07:00:00Z", "2026-11-01T05:30:00Z"},
		{"fold-standard", "America/New_York", "2026-11-01 01:30:00 EST", "2026-11-01T05:00:00Z", "2026-11-01T07:00:00Z", "2026-11-01T06:30:00Z"},
		{"spring-gap", "America/New_York", "2026-03-08 02:30:00 EST", "2026-03-08T06:00:00Z", "2026-03-08T08:00:00Z", ""},
		{"wrong-abbreviation", "America/New_York", "2026-09-07 01:00:00 EST", "2026-09-07T05:00:00Z", "2026-09-07T05:00:01Z", ""},
		{"not-US-CST", "Asia/Shanghai", "2026-09-07 13:00:00 CST", "2026-09-07T05:00:00Z", "2026-09-07T05:00:01Z", "2026-09-07T05:00:00Z"},
		{"fractional-offset", "Asia/Kathmandu", "2026-09-07 10:45:00 +0545", "2026-09-07T05:00:00Z", "2026-09-07T05:00:01Z", "2026-09-07T05:00:00Z"},
		{"mixed-case", "Pacific/Guam", "2026-09-07 15:00:00 ChST", "2026-09-07T05:00:00Z", "2026-09-07T05:00:01Z", "2026-09-07T05:00:00Z"},
		{"ambiguous-same-abbreviation", "XXX4XXX,M3.2.0,M11.1.0", "2026-11-01 01:30:00 XXX", "2026-11-01T04:00:00Z", "2026-11-01T06:00:00Z", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, old := state(t)
			sql(t, "ALTER SYSTEM SET log_timezone = '"+tc.zone+"'", false)
			sql(t, "SELECT pg_reload_conf()", false)
			var zone, loaded string
			for i := 0; i < 100; i++ {
				zone, loaded = state(t)
				if zone == tc.zone && loaded != old {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if zone != tc.zone || loaded == old {
				t.Fatal("fixture reload not observed")
			}
			originalLabel := "START WAL LOCATION: 0/1000028 (file 000000010000000000000001)\nCHECKPOINT LOCATION: 0/1000060\nBACKUP METHOD: streamed\nBACKUP FROM: primary\nSTART TIME: " + tc.label + "\nLABEL: pg_basebackup base backup\nSTART TIMELINE: 1\n"
			commit := repository.Commit{Timeline: 1, StartLSN: "0/1000028", StopLSN: "0/1000120", StoppedAt: tc.stopped, BackupLabel: originalLabel}
			label, err := parseLabel(&commit, 16<<20)
			if err != nil || commit.BackupLabel != originalLabel {
				t.Fatal("native label bytes changed", err)
			}
			query, err := labelTimeQuery(label, tc.began, tc.stopped, zone, loaded)
			if err != nil {
				t.Fatal(err)
			}
			got, err := normalizedLabelTime([]byte(sql(t, query, true)), tc.began, tc.stopped)
			if (tc.want == "" && err == nil) || (tc.want != "" && (err != nil || got != tc.want)) {
				t.Fatal("normalization", got, err, "want", tc.want)
			}
			// Same timezone but different reload generation must not be accepted.
			stale, _ := labelTimeQuery(tc.label, tc.began, tc.stopped, zone, old)
			if _, err := normalizedLabelTime([]byte(sql(t, stale, true)), tc.began, tc.stopped); err == nil {
				t.Fatal("changed source configuration accepted")
			}
			wrong, _ := labelTimeQuery(tc.label, tc.began, tc.stopped, "wrong-zone", loaded)
			if _, err := normalizedLabelTime([]byte(sql(t, wrong, true)), tc.began, tc.stopped); err == nil {
				t.Fatal("changed source timezone accepted")
			}
		})
	}
}
