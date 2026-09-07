package s3store

import "testing"

func TestWALRetirementContentType(t *testing.T) {
	o, e := options(Condition{Match: "opaque-etag"}, map[string]string{"cnpg-format": "wal-retired-v1"})
	if e != nil || o.ContentType != "application/json" {
		t.Fatal("retirement must be JSON", e)
	}
	live, e := options(Condition{Create: true}, map[string]string{"cnpg-format": "wal-v1"})
	if e != nil || live.ContentType != "application/octet-stream" {
		t.Fatal("live WAL content type changed", e)
	}
}
