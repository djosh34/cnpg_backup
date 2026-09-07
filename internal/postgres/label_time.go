// Copyright 2026 cnpg_backup contributors. All rights reserved.
package postgres

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// labelTimeQuery uses the source's own timezone database, never abbreviation
// input parsing (CST, for example, is not a unique offset). The capture deadline
// bounds this scan to 21,601 seconds. Formatting actual instants also distinguishes
// DST folds; zero or multiple matching instants fail rather than guess. SET is
// session-local only: the source's log_timezone/configuration is never changed.
// A reload, including change-away-and-back, invalidates this attempt conservatively.
func labelTimeQuery(label, began, stopped, zone, loaded string) (string, error) {
	first, last, err := labelTimeBounds(began, stopped)
	if err != nil || len(label) < 21 || len(label) > 128 || zone == "" || len(zone) > 256 || loaded == "" {
		return "", ErrInput
	}
	// Hex literals keep even hostile label/settings bytes out of SQL syntax.
	text := func(s string) string {
		return "convert_from(decode('" + hex.EncodeToString([]byte(s)) + "','hex'),'UTF8')"
	}
	return fmt.Sprintf(`WITH settings AS MATERIALIZED (
 SELECT set_config('TimeZone', current_setting('log_timezone'), true) AS zone
 ), matches AS (
 SELECT t FROM settings, generate_series(to_timestamp(%d), to_timestamp(%d), interval '1 second') AS t
 WHERE to_char(t, CASE WHEN settings.zone = %s
 AND to_char(pg_conf_load_time() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') = %s
 THEN 'YYYY-MM-DD HH24:MI:SS TZ' ELSE NULL END) = %s
 ) SELECT json_build_object('matches', count(*), 'started',
 to_char(min(t) AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"')) FROM matches;`, first.Unix(), last.Unix(), text(zone), text(loaded), text(label)), nil
}

func labelTimeBounds(began, stopped string) (time.Time, time.Time, error) {
	first, e := time.Parse(time.RFC3339Nano, began)
	last, e2 := time.Parse(time.RFC3339Nano, stopped)
	if e != nil || e2 != nil || last.Before(first) || last.Sub(first) > 6*time.Hour {
		return time.Time{}, time.Time{}, ErrInput
	}
	// Native backup_label truncates its time_t to seconds, including when the
	// preflight occurred later within that same second.
	return first.Truncate(time.Second), last.Truncate(time.Second), nil
}

func normalizedLabelTime(output []byte, began, stopped string) (string, error) {
	var result struct {
		Matches int    `json:"matches"`
		Started string `json:"started"`
	}
	first, last, err := labelTimeBounds(began, stopped)
	if err != nil || json.Unmarshal(output, &result) != nil || result.Matches != 1 {
		return "", ErrInput
	}
	started, err := time.Parse(time.RFC3339, result.Started)
	if err != nil || started.Before(first) || started.After(last) {
		return "", ErrInput
	}
	return started.UTC().Format(time.RFC3339Nano), nil
}
