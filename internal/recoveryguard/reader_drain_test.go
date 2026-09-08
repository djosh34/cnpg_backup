package recoveryguard

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestOriginalReaderReleaseIsAfterTaskDrainBeforeAcknowledgment(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "clean", true: "release failed"}[fail], func(t *testing.T) {
			a, _, socket, owner := sessionFixture(t)
			entered, finish := make(chan struct{}), make(chan struct{})
			if e := a.AfterDrain(func(context.Context) error {
				close(entered)
				<-finish
				if fail {
					return errors.New("uncertain reader release")
				}
				return nil
			}); e != nil {
				t.Fatal(e)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s, e := Begin(ctx, socket, owner)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			if e = a.AfterDrain(func(context.Context) error { return nil }); e == nil {
				t.Fatal("rebound reader release after Begin")
			}
			task, e := a.Admit(s.tuple)
			if e != nil {
				t.Fatal(e)
			}
			done := make(chan error, 1)
			go func() { done <- s.drain(ctx) }()
			<-task.Context.Done()
			select {
			case <-entered:
				t.Fatal("released reader while task/native child active")
			default:
			}
			task.Done()
			<-entered
			if _, e = a.Admit(s.tuple); e == nil {
				t.Fatal("new callback during source release")
			}
			select {
			case e := <-done:
				t.Fatal("acknowledged before reader CAS returned", e)
			default:
			}
			close(finish)
			e = <-done
			if (e != nil) != fail {
				t.Fatal("reader release failure did not poison acknowledgment", e)
			}
		})
	}
}
func TestControlLossNeverCallsReaderRelease(t *testing.T) {
	a, _, socket, owner := sessionFixture(t)
	var calls atomic.Int32
	if e := a.AfterDrain(func(context.Context) error { calls.Add(1); return nil }); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, e := Begin(ctx, socket, owner)
	if e != nil {
		t.Fatal(e)
	}
	task, e := a.Admit(s.tuple)
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	<-task.Context.Done()
	task.Done()
	<-a.drained
	if calls.Load() != 0 {
		t.Fatal("control loss released uncertain original reader")
	}
}
