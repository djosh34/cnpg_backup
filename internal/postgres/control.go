package postgres

import (
	"context"
	"strconv"
	"strings"
)

type Control struct {
	SystemIdentifier string
	WALSegmentBytes  int64
}

// ReadControl also works for stopped PostgreSQL during ordinary pg_rewind. It
// does not confer source-recovery access or target ownership.
func ReadControl(ctx context.Context) (Control, error) {
	b, e := command(ctx, []string{"LANG=C", "LC_ALL=C"}, "pg_controldata", "/var/lib/postgresql/data/pgdata")
	if e != nil {
		return Control{}, e
	}
	if e = validateControl(string(b)); e != nil {
		return Control{}, e
	}
	var result Control
	for _, line := range strings.Split(string(b), "\n") {
		p := strings.SplitN(line, ":", 2)
		if len(p) != 2 {
			continue
		}
		switch strings.TrimSpace(p[0]) {
		case "Database system identifier":
			result.SystemIdentifier = strings.TrimSpace(p[1])
		case "Bytes per WAL segment":
			result.WALSegmentBytes, _ = strconv.ParseInt(strings.TrimSpace(p[1]), 10, 64)
		}
	}
	return result, nil
}
