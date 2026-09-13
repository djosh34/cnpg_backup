package postgres

import (
	"context"
	"strconv"
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
	fields := controlFields(string(b))
	if e = validateControl(fields); e != nil {
		return Control{}, e
	}
	result := Control{SystemIdentifier: fields["Database system identifier"]}
	result.WALSegmentBytes, _ = strconv.ParseInt(fields["Bytes per WAL segment"], 10, 64)
	return result, nil
}
