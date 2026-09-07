package recoveryguard

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func sessionFixture(t *testing.T) (*Admission, *ownership, string, Owner) {
	t.Helper()
	c, owner := testConfig(t)
	held, err := acquire(c, owner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(held.close)
	// Same production admission/marker/stream code with disposable mount roots;
	// path-policy validation is tested separately and cannot be bypassed by CLI.
	admission := newAdmission(c, owner.PodUID)
	dir, err := os.MkdirTemp("", "s")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "socket")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(maxMessage))
	RegisterControl(server, admission)
	go server.Serve(listener)
	t.Cleanup(func() { admission.Close(); server.Stop(); listener.Close() })
	return admission, held, socket, owner
}
func TestRealStreamDrainWaitsForWritesAndIsTerminal(t *testing.T) {
	a, held, socket, owner := sessionFixture(t)
	if _, err := a.Admit(Tuple{}); err == nil {
		t.Fatal("work before Begin")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := Begin(ctx, socket, owner)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tuple, err := a.ActiveTuple()
	if err != nil || tuple != s.tuple {
		t.Fatal("session binding", err)
	}
	task, err := a.Admit(tuple)
	if err != nil {
		t.Fatal(err)
	}
	stale := tuple
	stale.SidecarUID = NewUUID()
	if _, err := a.Admit(stale); err == nil {
		t.Fatal("stale incarnation admitted")
	}
	done := make(chan error, 1)
	go func() { done <- s.drain(ctx) }()
	select {
	case <-task.Context.Done():
	case <-ctx.Done():
		t.Fatal("Drain did not cancel admission")
	}
	select {
	case err := <-done:
		t.Fatal("acknowledged outstanding write", err)
	default:
	}
	if _, err := a.Admit(tuple); err == nil {
		t.Fatal("late callback reopened gate")
	}
	// Actual delayed target I/O after cancellation must finish before acknowledgment.
	if err := os.WriteFile(filepath.Join(a.config.Targets[0].Mount, "late-write"), []byte("complete"), 0600); err != nil {
		t.Fatal(err)
	}
	task.Done()
	task.Done() // idempotent completion, not a double decrement
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := Begin(ctx, socket, owner); err == nil {
		t.Fatal("terminal sidecar rebound")
	}
	if _, err := a.Admit(tuple); err == nil {
		t.Fatal("stale callback admitted after Drain")
	}
	if err := held.release(); err != nil {
		t.Fatal(err)
	}
}
func TestControlLossClosesButDoesNotDrainWritesOrClearMarkers(t *testing.T) {
	a, held, socket, owner := sessionFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := Begin(ctx, socket, owner)
	if err != nil {
		t.Fatal(err)
	}
	task, err := a.Admit(s.tuple)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	select {
	case <-task.Context.Done():
	case <-ctx.Done():
		t.Fatal("EOF did not close gate")
	}
	select {
	case <-a.drained:
		t.Fatal("pending task treated as drained")
	default:
	}
	if _, err := Begin(ctx, socket, owner); err == nil {
		t.Fatal("reconnected after loss")
	}
	task.Done()
	held.close()
	if _, err := acquire(a.config, owner); err != ErrUncertain {
		t.Fatalf("lost session erased marker: %v", err)
	}
}
func TestBeginRejectsMissingLocksWrongPodAndReincarnation(t *testing.T) {
	a, held, socket, owner := sessionFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wrong := owner
	wrong.PodUID = NewUUID()
	if _, err := Begin(ctx, socket, wrong); err == nil {
		t.Fatal("wrong Pod")
	}
	wrong = owner
	wrong.GuardUID = NewUUID()
	if _, err := Begin(ctx, socket, wrong); err == nil {
		t.Fatal("wrong guard")
	}
	held.close()
	if _, err := Begin(ctx, socket, owner); err == nil {
		t.Fatal("Begin without live flock")
	}
	if _, err := a.ActiveTuple(); err == nil {
		t.Fatal("invalid Begin opened gate")
	}
}
func TestAdmissionBoundedAndCanceledDrainStaysClosed(t *testing.T) {
	a, _, socket, owner := sessionFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := Begin(ctx, socket, owner)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tasks := []*Task{}
	for range 4 {
		task, err := a.Admit(s.tuple)
		if err != nil {
			t.Fatal(err)
		}
		tasks = append(tasks, task)
	}
	if _, err := a.Admit(s.tuple); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("unbounded admission", err)
	}
	drainCtx, stop := context.WithCancel(ctx)
	stop()
	if err := a.drain(drainCtx, s.tuple); err == nil {
		t.Fatal("canceled drain acknowledged")
	}
	if _, err := a.Admit(s.tuple); err == nil {
		t.Fatal("canceled drain reopened gate")
	}
	for _, task := range tasks {
		task.Done()
	}
}
func TestMalformedControlIsBounded(t *testing.T) {
	a, _, socket, _ := sessionFixture(t)
	conn, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := conn.NewStream(ctx, &controlDescription.Streams[0], controlMethod)
	if err != nil {
		t.Fatal(err)
	}
	if err := send(stream, controlMessage{Kind: "drain"}); err != nil {
		t.Fatal(err)
	}
	if _, err := receive(stream); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if _, err := a.ActiveTuple(); err == nil {
		t.Fatal("malformed stream admitted")
	}
}
