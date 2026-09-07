package main

import (
	"bytes"
	"io"
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
