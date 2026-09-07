package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestFailClosed(t *testing.T) {
	for _, mode := range []string{"manager", "instance", "recovery-job", "recovery-guard", "wal-fetch", "unknown", ""} {
		t.Run(mode, func(t *testing.T) {
			var out, err bytes.Buffer
			want := 2
			if mode == "wal-fetch" {
				want = 255
			}
			if got := run([]string{mode}, &out, &err); got != want || out.Len() != 0 || err.Len() == 0 {
				t.Fatalf("exit=%d stdout=%q stderr=%q", got, &out, &err)
			}
		})
	}
}

func TestVersion(t *testing.T) {
	var out, err bytes.Buffer
	if run([]string{"version"}, &out, &err) != 0 || !strings.Contains(out.String(), "revision=") || err.Len() != 0 {
		t.Fatalf("stdout=%q stderr=%q", &out, &err)
	}
}
