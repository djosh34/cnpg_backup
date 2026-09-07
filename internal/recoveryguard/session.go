// Copyright 2026 cnpg_backup contributors. All rights reserved.
package recoveryguard

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const controlMethod = "/cnpg.backup.local.v1.RecoveryControl/Session"

// The private bidi RPC uses protobuf BytesValue envelopes with bounded strict
// JSON bodies. It is versioned separately from CNPG-I, on the SAME Unix socket.
// There are exactly two client messages (Begin, Drain) and two acknowledgments.
// Closing/replacing the stream is never a way to resume an incarnation.
type controlMessage struct {
	Kind  string `json:"kind"`
	Tuple Tuple  `json:"tuple"`
}

// Admission is one process incarnation's terminal gate. All future recovery
// handlers must Admit before dispatching any target/source work, hold Task until
// writes AND native descendants have actually drained, and carry the tuple in
// helper plans. Cancellation alone is not task completion.
type Admission struct {
	mu              sync.Mutex
	config          Config
	pod, process    string
	tuple           Tuple
	started, closed bool
	active          int
	drained         chan struct{}
	ctx             context.Context
	cancel          context.CancelFunc
	afterDrain      func(context.Context) error
}

func NewAdmission(c Config, pod string) (*Admission, error) {
	if _, err := c.owner(pod, NewUUID()); err != nil {
		return nil, err
	}
	return newAdmission(c, pod), nil
}
func newAdmission(c Config, pod string) *Admission {
	c.Targets = c.sortedTargets()
	ctx, cancel := context.WithCancel(context.Background())
	return &Admission{config: c, pod: pod, process: NewUUID(), drained: make(chan struct{}), ctx: ctx, cancel: cancel}
}
func (a *Admission) begin(owner Owner) (Tuple, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	expected := Owner{a.config.ClusterUID, a.config.OperationUID, a.pod, owner.GuardUID}
	if !uuidPattern.MatchString(owner.GuardUID) || owner != expected {
		return Tuple{}, status.Error(codes.FailedPrecondition, "recovery identity mismatch")
	}
	if a.started || a.closed {
		return Tuple{}, status.Error(codes.FailedPrecondition, "recovery admission is terminal")
	}
	if err := verifyOwner(a.config, owner); err != nil {
		return Tuple{}, status.Error(codes.FailedPrecondition, "target owner not established")
	}
	a.started = true
	a.tuple = Tuple{owner, a.process}
	return a.tuple, nil
}

// Task completion must be deferred around the entire actual I/O operation, not
// around queueing it. Context cancellation asks work to stop; Done proves it did.
type Task struct {
	Context   context.Context
	Tuple     Tuple
	once      sync.Once
	admission *Admission
}

func (a *Admission) Admit(tuple Tuple) (*Task, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.started || a.closed || tuple != a.tuple {
		return nil, status.Error(codes.FailedPrecondition, "no active recovery session")
	}
	if a.active >= 4 {
		return nil, status.Error(codes.ResourceExhausted, "local recovery task limit")
	}
	a.active++
	return &Task{Context: a.ctx, Tuple: tuple, admission: a}, nil
}

// ActiveTuple binds old CNPG-I callbacks, which carry no tuple, to the only
// admitted Pod-local session. Admit must still succeed before work begins.
func (a *Admission) ActiveTuple() (Tuple, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.started || a.closed {
		return Tuple{}, status.Error(codes.FailedPrecondition, "no active recovery session")
	}
	return a.tuple, nil
}
func (t *Task) Done() {
	t.once.Do(func() {
		a := t.admission
		a.mu.Lock()
		defer a.mu.Unlock()
		a.active--
		if a.closed && a.active == 0 {
			close(a.drained)
		}
	})
}
func (a *Admission) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}
	a.closed = true
	a.cancel()
	if a.active == 0 {
		close(a.drained)
	}
}

// AfterDrain installs the one original-process source-reader cleanup before
// Begin. It runs only after irreversible admission close and actual task drain,
// never merely on control loss or server shutdown. Failure poisons the guard.
func (a *Admission) AfterDrain(fn func(context.Context) error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.started || a.closed || a.afterDrain != nil || fn == nil {
		return errors.New("drain callback already bound")
	}
	a.afterDrain = fn
	return nil
}
func (a *Admission) drain(ctx context.Context, tuple Tuple) error {
	a.mu.Lock()
	valid := a.started && !a.closed && tuple == a.tuple
	a.mu.Unlock()
	if !valid {
		return status.Error(codes.FailedPrecondition, "invalid drain identity or terminal session")
	}
	a.Close()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.drained:
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if a.afterDrain != nil {
			return a.afterDrain(ctx)
		}
		return nil
	}
}

type controlServer interface{ session(grpc.ServerStream) error }

var controlDescription = grpc.ServiceDesc{
	ServiceName: "cnpg.backup.local.v1.RecoveryControl",
	HandlerType: (*controlServer)(nil),
	Streams: []grpc.StreamDesc{{StreamName: "Session", ServerStreams: true, ClientStreams: true,
		Handler: func(srv any, stream grpc.ServerStream) error { return srv.(controlServer).session(stream) }}},
}

func RegisterControl(server *grpc.Server, a *Admission) {
	server.RegisterService(&controlDescription, a)
}
func receive(stream interface{ RecvMsg(any) error }) (controlMessage, error) {
	var frame wrapperspb.BytesValue
	var msg controlMessage
	if err := stream.RecvMsg(&frame); err != nil {
		return msg, err
	}
	if err := Decode(frame.Value, &msg); err != nil {
		return msg, status.Error(codes.InvalidArgument, "invalid recovery control message")
	}
	return msg, nil
}
func send(stream interface{ SendMsg(any) error }, msg controlMessage) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return stream.SendMsg(wrapperspb.Bytes(b))
}
func (a *Admission) session(stream grpc.ServerStream) error {
	msg, err := receive(stream)
	if err != nil {
		return err
	}
	if msg.Kind != "begin" || msg.Tuple.SidecarUID != "" {
		return status.Error(codes.InvalidArgument, "expected Begin")
	}
	tuple, err := a.begin(msg.Tuple.Owner)
	if err != nil {
		return err
	}
	// From this point ALL error/EOF/cancellation paths irreversibly close the gate.
	defer a.Close()
	if err = send(stream, controlMessage{"begun", tuple}); err != nil {
		return err
	}
	msg, err = receive(stream)
	if err != nil {
		return err
	}
	if msg.Kind != "drain" || msg.Tuple != tuple {
		return status.Error(codes.FailedPrecondition, "expected same-session Drain")
	}
	if err = a.drain(stream.Context(), tuple); err != nil {
		return err
	}
	return send(stream, controlMessage{"drained", tuple})
}

// Session is never reconnected. done is buffered so the receive owner always
// finishes even when PID1 chooses a failure/poison path.
type Session struct {
	tuple  Tuple
	conn   *grpc.ClientConn
	stream grpc.ClientStream
	cancel context.CancelFunc
	done   chan error
}

func Begin(ctx context.Context, socket string, owner Owner) (*Session, error) {
	conn, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessage), grpc.MaxCallSendMsgSize(maxMessage)))
	if err != nil {
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := conn.NewStream(streamCtx, &controlDescription.Streams[0], controlMethod)
	if err != nil {
		cancel()
		conn.Close()
		return nil, err
	}
	s := &Session{conn: conn, stream: stream, cancel: cancel, done: make(chan error, 1)}
	if err = send(stream, controlMessage{"begin", Tuple{Owner: owner}}); err != nil {
		s.Close()
		return nil, err
	}
	msg, err := receive(stream)
	if err != nil || msg.Kind != "begun" || msg.Tuple.Owner != owner || !uuidPattern.MatchString(msg.Tuple.SidecarUID) {
		s.Close()
		return nil, errors.New("Begin was not acknowledged by the configured sidecar")
	}
	s.tuple = msg.Tuple
	go func() {
		msg, err := receive(stream)
		if err == nil && (msg.Kind != "drained" || msg.Tuple != s.tuple) {
			err = errors.New("invalid Drain acknowledgment")
		}
		s.done <- err
	}()
	return s, nil
}
func (s *Session) Close() { s.cancel(); s.conn.Close() }
func (s *Session) drain(ctx context.Context) error {
	if err := send(s.stream, controlMessage{"drain", s.tuple}); err != nil {
		return err
	}
	select {
	case err := <-s.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
