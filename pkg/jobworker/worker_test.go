package jobworker_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	jobrunv1 "github.com/crowbett/jobrun/gen/jobrun/v1"
	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/internal/jobtest"
	"github.com/crowbett/jobrun/internal/obs"
	"github.com/crowbett/jobrun/internal/server"
	"github.com/crowbett/jobrun/pkg/jobworker"
)

const queue = "remote"

type harness struct {
	leases *jobtest.Leases
	srv    *server.WorkerServer
	conn   *grpc.ClientConn
	grpc   *grpc.Server
}

// newHarness serves a real server.WorkerServer over bufconn on top of the
// in-memory lease store.
func newHarness(t *testing.T, ttl time.Duration) *harness {
	t.Helper()
	leases := jobtest.NewLeases()
	ws := server.NewWorkerServer(leases, nil, server.WorkerConfig{LeaseTTL: ttl, PollFloor: 5 * time.Millisecond, BatchSize: 4})
	gs := grpc.NewServer()
	ws.Register(gs)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = gs.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		gs.Stop()
	})
	return &harness{leases: leases, srv: ws, conn: conn, grpc: gs}
}

func newJobs(n int, typ string) []job.Job {
	out := make([]job.Job, n)
	for i := range out {
		out[i] = job.Job{ID: job.NewID(), Queue: queue, Type: typ, Payload: []byte(`{}`)}
	}
	return out
}

// runWorker runs w until the test ends; stop cancels and waits for Run.
func runWorker(t *testing.T, w *jobworker.Worker) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	var once atomic.Bool
	stop = func() {
		if !once.CompareAndSwap(false, true) {
			return
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Run did not return after cancel")
		}
	}
	t.Cleanup(stop)
	return stop
}

func recvCall(t *testing.T, ch <-chan jobtest.Call) jobtest.Call {
	t.Helper()
	select {
	case c := <-ch:
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a Complete/Fail")
		panic("unreachable")
	}
}

func TestRemoteWorkerRunsJobsWithinConcurrency(t *testing.T) {
	t.Parallel()
	h := newHarness(t, time.Second)
	const n, conc = 40, 3
	jobs := newJobs(n, "work")
	h.leases.Add(jobs...)
	var running, peak atomic.Int32
	w := jobworker.New(h.conn, jobworker.Config{WorkerID: "w-a", Queues: []string{queue}, Concurrency: conc})
	if err := w.Handle("work", jobworker.HandlerFunc(func(ctx context.Context, j jobworker.Job) error {
		cur := running.Add(1)
		defer running.Add(-1)
		for {
			p := peak.Load()
			if cur <= p || peak.CompareAndSwap(p, cur) {
				break
			}
		}
		if j.LeaseToken != 1 || j.Attempt != 1 || j.Queue != queue {
			return fmt.Errorf("job %+v", j)
		}
		time.Sleep(2 * time.Millisecond)
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := w.Handle("work", nil); err == nil {
		t.Fatal("duplicate Handle must fail")
	}
	stop := runWorker(t, w)
	for range n {
		c := recvCall(t, h.leases.Calls)
		if c.Op != "complete" || c.Err != nil || c.Owner != server.RemoteOwnerPrefix+"w-a" || c.Lease.Token != 1 {
			t.Fatalf("call = %+v", c)
		}
	}
	stop()
	if p := peak.Load(); p > conc {
		t.Fatalf("peak concurrency %d > %d", p, conc)
	}
	if st := w.Stats(); st.Succeeded != n || st.Received != n || st.Fenced != 0 {
		t.Fatalf("stats = %+v", st)
	}
	if st := h.srv.Stats(); st.Delivered != n || st.Completed != n {
		t.Fatalf("server stats = %+v", st)
	}
}

func TestRemoteWorkerFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		err      error
		wantPerm bool
		wantSub  string
	}{
		{name: "transient", err: errors.New("flaky upstream"), wantSub: "flaky upstream"},
		{name: "permanent", err: fmt.Errorf("%w: bad payload", jobworker.ErrPermanent), wantPerm: true, wantSub: "bad payload"},
		{name: "panic", wantSub: "handler panicked"},
		{name: "unknown type", wantPerm: true, wantSub: "no handler registered"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, time.Second)
			typ := "t"
			if tc.name == "unknown type" {
				typ = "nobody"
			}
			j := newJobs(1, typ)[0]
			h.leases.Add(j)
			w := jobworker.New(h.conn, jobworker.Config{WorkerID: "w-f", Queues: []string{queue}})
			_ = w.Handle("t", jobworker.HandlerFunc(func(context.Context, jobworker.Job) error {
				if tc.name == "panic" {
					panic("boom")
				}
				return tc.err
			}))
			runWorker(t, w)
			c := recvCall(t, h.leases.Calls)
			if c.Op != "fail" || c.Perm != tc.wantPerm || !strings.Contains(c.Cause, tc.wantSub) || c.Err != nil {
				t.Fatalf("call = %+v", c)
			}
		})
	}
}

// TestRemoteLostLockDetection: a remote handler whose lease is lost - stolen
// by the reaper, or unrenewable because the server is gone - has its ctx
// cancelled promptly and is never completed or failed; a cancel request stops
// it but still reports Fail.
func TestRemoteLostLockDetection(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		act       func(h *harness, id job.ID)
		ignoreCtx bool
		wantFence bool
		maxCancel time.Duration
	}{
		{name: "stolen lease", act: func(h *harness, id job.ID) { h.leases.Steal(id) }, wantFence: true, maxCancel: time.Second},
		{name: "stolen lease, handler claims success", act: func(h *harness, id job.ID) { h.leases.Steal(id) }, ignoreCtx: true, wantFence: true, maxCancel: time.Second},
		{name: "heartbeats keep failing", act: func(h *harness, _ job.ID) { h.leases.SetHeartbeatError(jobtest.ErrDown) }, wantFence: true, maxCancel: 2 * time.Second},
		{name: "cancel requested", act: func(h *harness, id job.ID) { h.leases.RequestCancel(id) }, maxCancel: time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const ttl = 600 * time.Millisecond
			h := newHarness(t, ttl)
			j := newJobs(1, "block")[0]
			h.leases.Add(j)
			started := make(chan struct{})
			cancelled := make(chan time.Duration, 1)
			w := jobworker.New(h.conn, jobworker.Config{WorkerID: "w-l", Queues: []string{queue}, HeartbeatEvery: 100 * time.Millisecond})
			_ = w.Handle("block", jobworker.HandlerFunc(func(ctx context.Context, _ jobworker.Job) error {
				close(started)
				<-ctx.Done()
				if tc.ignoreCtx {
					return nil // pretends it finished: must still not be completed
				}
				return ctx.Err()
			}))
			stop := runWorker(t, w)
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("handler never started")
			}
			at := time.Now()
			tc.act(h, j.ID)
			go func() {
				deadline := time.Now().Add(5 * time.Second)
				for time.Now().Before(deadline) {
					if w.Stats().Fenced > 0 || w.Stats().Failed > 0 {
						cancelled <- time.Since(at)
						return
					}
					time.Sleep(2 * time.Millisecond)
				}
			}()
			select {
			case took := <-cancelled:
				if took > tc.maxCancel {
					t.Fatalf("detected after %v, want <= %v", took, tc.maxCancel)
				}
			case <-time.After(6 * time.Second):
				t.Fatal("lost lease never detected")
			}
			if tc.wantFence {
				select {
				case c := <-h.leases.Calls:
					t.Fatalf("a fenced job must never be reported, got %+v", c)
				case <-time.After(200 * time.Millisecond):
				}
			} else if c := recvCall(t, h.leases.Calls); c.Op != "fail" || !strings.Contains(c.Cause, "cancel requested") {
				t.Fatalf("call = %+v", c)
			}
			stop()
			st := w.Stats()
			if tc.wantFence && (st.Fenced != 1 || st.Succeeded+st.Failed != 0) {
				t.Fatalf("stats = %+v", st)
			}
			if !tc.wantFence && (st.Fenced != 0 || st.Failed != 1) {
				t.Fatalf("stats = %+v", st)
			}
		})
	}
}

// TestRemoteTracePropagation: the traceparent stored at enqueue reaches the
// remote worker, whose jobrun.run span is its child; the worker's run span
// context travels back on Complete and the server's jobrun.complete span is
// its child. Both sides export to one in-memory exporter.
func TestRemoteTracePropagation(t *testing.T) { //nolint:paralleltest // swaps the global tracer provider
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp) // the server side uses the global provider
	t.Cleanup(func() { otel.SetTracerProvider(prev); _ = tp.Shutdown(context.Background()) })

	h := newHarness(t, time.Second)
	// The producer side: a real enqueue span whose traceparent is stored.
	pctx, enqueueSpan := obs.StartEnqueue(context.Background(), job.Job{ID: job.NewID(), Queue: queue, Type: "traced"})
	j := newJobs(1, "traced")[0]
	j.TraceParent = obs.TraceParent(pctx)
	enqueueSpan.End()
	h.leases.Add(j)

	var seen string
	w := jobworker.New(h.conn, jobworker.Config{WorkerID: "w-t", Queues: []string{queue}, TracerProvider: tp})
	_ = w.Handle("traced", jobworker.HandlerFunc(func(_ context.Context, jj jobworker.Job) error {
		seen = jj.TraceParent
		return nil
	}))
	stop := runWorker(t, w)
	if c := recvCall(t, h.leases.Calls); c.Op != "complete" || c.Err != nil {
		t.Fatalf("call = %+v", c)
	}
	stop()
	if seen != j.TraceParent || seen == "" {
		t.Fatalf("handler saw traceparent %q, want %q", seen, j.TraceParent)
	}

	byName := map[string]tracetest.SpanStub{}
	for _, s := range exp.GetSpans() {
		byName[s.Name] = s
	}
	enq, run, done := byName[obs.SpanEnqueue], byName[obs.SpanRun], byName[obs.SpanComplete]
	if !enq.SpanContext.IsValid() || !run.SpanContext.IsValid() || !done.SpanContext.IsValid() {
		t.Fatalf("missing spans: %v", exp.GetSpans())
	}
	if run.SpanContext.TraceID() != enq.SpanContext.TraceID() || run.Parent.SpanID() != enq.SpanContext.SpanID() {
		t.Fatalf("run span is not a child of the enqueue span: run parent %s enqueue %s", run.Parent.SpanID(), enq.SpanContext.SpanID())
	}
	if done.SpanContext.TraceID() != run.SpanContext.TraceID() || done.Parent.SpanID() != run.SpanContext.SpanID() {
		t.Fatalf("server complete span is not a child of the remote run span")
	}
}

// TestClaimRespectsCredits drives the raw stream: the server pushes no more
// jobs than the credits granted, and more after a new grant.
func TestClaimRespectsCredits(t *testing.T) {
	t.Parallel()
	h := newHarness(t, time.Second)
	h.leases.Add(newJobs(5, "c")...)
	client := jobrunv1.NewWorkerServiceClient(h.conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&jobrunv1.ClaimRequest{WorkerId: "raw", Queues: []string{queue}, Credits: 2}); err != nil {
		t.Fatal(err)
	}
	got := make(chan *jobrunv1.LeasedJob, 8)
	streamErr := make(chan error, 1)
	go func() {
		for {
			r, err := stream.Recv()
			if err != nil {
				streamErr <- err
				return
			}
			got <- r.GetJob()
		}
	}()
	for range 2 {
		select {
		case lj := <-got:
			if lj.GetLeaseToken() != 1 || lj.GetLeaseTtl().AsDuration() != time.Second {
				t.Fatalf("leased job = %v", lj)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no job for granted credit")
		}
	}
	select {
	case lj := <-got:
		t.Fatalf("job beyond the granted credits: %v", lj)
	case <-time.After(150 * time.Millisecond):
	}
	if err := stream.Send(&jobrunv1.ClaimRequest{Credits: 1}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("no job after a new credit")
	}
	if err := stream.Send(&jobrunv1.ClaimRequest{Credits: -1}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-streamErr:
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("negative credits: want InvalidArgument got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream not ended after negative credits")
	}
}
