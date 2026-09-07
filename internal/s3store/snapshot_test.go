package s3store

import (
	"context"
	"encoding/pem"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestSnapshotsShareHardWALCeiling(t *testing.T) {
	entered := make(chan struct{}, 3)
	var requests atomic.Int32
	first, server := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		requests.Add(1)
		entered <- struct{}{}
		<-r.Context().Done()
	}))
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	stores := []*Store{first}
	for range 2 {
		s, e := New(config(server.URL, ca))
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(s.Close)
		stores = append(stores, s)
	}
	f, expected := spool(t, []byte("wal"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 3)
	for _, s := range stores[:2] {
		go func() { _, e := s.PutFile(ctx, "key", f, expected, Condition{Create: true}, nil); done <- e }()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("WAL requests did not start")
		}
	}
	go func() { _, e := stores[2].PutFile(ctx, "key", f, expected, Condition{Create: true}, nil); done <- e }()
	select {
	case <-entered:
		t.Fatal("new snapshot bypassed process WAL ceiling")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	for range 3 {
		if e := <-done; e == nil {
			t.Fatal("canceled PUT succeeded")
		}
	}
	if requests.Load() != 2 {
		t.Fatal("unexpected dispatch count", requests.Load())
	}
	if len(processWAL) != 0 {
		t.Fatal("WAL slots leaked")
	}
}
