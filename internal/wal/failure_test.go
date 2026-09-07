package wal

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/s3store"
)

func TestUntrustedTLSCannotAcknowledgeOrBecomeAbsence(t *testing.T) {
	w, b := setup(t, 1<<20)
	bad, e := s3store.New(s3store.Config{Endpoint: b.endpoint, Bucket: "bucket", Signature: "v4", Addressing: "path", Region: "us-east-1", AccessKey: "fixture", SecretKey: "fixture"})
	if e != nil {
		t.Fatal(e)
	}
	defer bad.Close()
	w.Store = bad
	name := "000000010000000000000001"
	e = w.Archive(context.Background(), name, source(t, bytes.Repeat([]byte{1}, 1<<20)))
	if !s3store.Is(e, s3store.TLS) {
		t.Fatal("untrusted upload", e)
	}
	root, _ := local(t)
	if e = w.Restore(context.Background(), name, root, name); !s3store.Is(e, s3store.TLS) {
		t.Fatal("untrusted restore became missing", e)
	}
}

func TestUnwritableRawOutputNeverSucceeds(t *testing.T) {
	w, _ := setup(t, 1<<20)
	name := "00000002.history"
	ctx := context.Background()
	if e := w.Archive(ctx, name, source(t, []byte("1\t0/100000\tfixture\n"))); e != nil {
		t.Fatal(e)
	}
	// /dev/full rejects truncation before raw writes. This proves a local output
	// failure cannot be acknowledged, NOT actual finite-filesystem exhaustion.
	// The hosted CNPG matrix owns that distinct ENOSPC acceptance case.
	full, e := os.OpenFile("/dev/full", os.O_WRONLY, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer full.Close()
	if _, e = w.retrieve(ctx, name, full); e != ErrLocal {
		t.Fatal("unwritable verified output", e)
	}
}
