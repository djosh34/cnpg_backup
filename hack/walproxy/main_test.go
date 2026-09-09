package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPartialBodyBarrierReleaseAndCancellation(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		state := &faults{release: make(chan struct{})}
		done := make(chan struct{})
		body := &partialBody{ReadCloser: io.NopCloser(bytes.NewBufferString("0123456789")), remaining: 5, state: state, release: state.release, done: done}
		p := make([]byte, 10)
		n, e := body.Read(p)
		if n != 5 || e != nil || string(p[:n]) != "01234" {
			t.Fatal("did not forward first half", n, e)
		}
		result := make(chan error, 1)
		go func() { _, e := body.Read(p); result <- e }()
		end := time.After(time.Second)
		for {
			state.Lock()
			blocked := state.blocked
			state.Unlock()
			if blocked == 1 {
				break
			}
			select {
			case <-end:
				t.Fatal("barrier not observed")
			default:
				time.Sleep(time.Millisecond)
			}
		}
		select {
		case <-result:
			t.Fatal("upload escaped active barrier")
		default:
		}
		if cancel {
			close(done)
		} else {
			close(state.release)
		}
		select {
		case e := <-result:
			if cancel != (e != nil) {
				t.Fatal("wrong terminal outcome", e)
			}
		case <-time.After(time.Second):
			t.Fatal("body did not drain")
		}
	}
}

func TestWALFaultsHaveDistinguishingStatusAndEffectiveCount(t *testing.T) {
	for _, test := range []struct {
		mode string
		code int
		body string
	}{
		{"missing-wal-get", 404, "NoSuchKey"}, {"auth-wal-get", 403, "AccessDenied"},
	} {
		t.Run(test.mode, func(t *testing.T) {
			state := &faults{}
			rec := httptest.NewRecorder()
			if !injectWAL(rec, httptest.NewRequest("GET", "https://fixture/wal/name", nil), test.mode, state) {
				t.Fatal("fault did not fire")
			}
			if rec.Code != test.code || !strings.Contains(rec.Body.String(), test.body) || state.blocked != 1 {
				t.Fatalf("ineffective fault: status=%d count=%d", rec.Code, state.blocked)
			}
		})
	}
	state := &faults{}
	if injectWAL(httptest.NewRecorder(), httptest.NewRequest("GET", "https://fixture/wal/name", nil), "", state) || state.blocked != 0 {
		t.Fatal("healthy request changed")
	}
}

func TestCorruptionChangesBytesNotMetadataOrResponseLength(t *testing.T) {
	body := &corruptBody{ReadCloser: io.NopCloser(strings.NewReader("actual payload"))}
	got, e := io.ReadAll(body)
	if e != nil || len(got) != len("actual payload") || string(got[1:]) != "ctual payload" || got[0] == 'a' {
		t.Fatalf("corruption was ineffective or truncated bytes: %q %v", got, e)
	}
}

func TestResetIsConnectionAbortNotOrdinaryMissing(t *testing.T) {
	defer func() {
		if recover() != http.ErrAbortHandler {
			t.Fatal("transport injection was not an actual abort")
		}
	}()
	injectWAL(httptest.NewRecorder(), httptest.NewRequest("GET", "https://fixture/wal/name", nil), "reset-wal-get", &faults{})
}

func TestSlowBodyCountsActualBoundedReadsAndDrains(t *testing.T) {
	state := &faults{}
	release, done := make(chan struct{}), make(chan struct{})
	close(release)
	body := &slowBody{ReadCloser: io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("x"), 128<<10))), state: state, release: release, done: done}
	buffer := make([]byte, 256<<10)
	n, err := body.Read(buffer)
	if err != nil || n != 64<<10 || state.transferred != int64(n) {
		t.Fatal(n, err, state.transferred)
	}
	n, err = body.Read(buffer)
	if err != nil || n != 64<<10 || state.transferred != 128<<10 {
		t.Fatal(n, err, state.transferred)
	}
	cancelled := make(chan struct{})
	close(cancelled)
	body.done, body.release = cancelled, make(chan struct{})
	if _, err = body.Read(buffer); err != io.ErrClosedPipe {
		t.Fatal("canceled transfer did not fail", err)
	}
}

func TestControlRejectsArbitraryFaultCommands(t *testing.T) {
	for _, mode := range []string{"", "slow-artifact-put", "hold-wal-put", "hold-commit-response", "tls-wal-get", "hold-wal-get-response", "hold-artifact-get-response"} {
		if !validMode(mode) {
			t.Fatal(mode)
		}
	}
	for _, mode := range []string{"sh -c id", "missing", "sleep 30", "auth-wal-get;id"} {
		if validMode(mode) {
			t.Fatal(mode)
		}
	}
}
