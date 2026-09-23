package server_test

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
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
)

func dial(t *testing.T, register func(grpc.ServiceRegistrar), srvOpts []grpc.ServerOption, cliOpts ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	gs := grpc.NewServer(srvOpts...)
	register(gs)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = gs.Serve(lis) }()
	opts := append([]grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, cliOpts...)
	conn, err := grpc.NewClient("passthrough:///bufconn", opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(); gs.Stop() })
	return conn
}

func newWorkerClient(t *testing.T, leases *jobtest.Leases) (jobrunv1.WorkerServiceClient, *server.WorkerServer) {
	t.Helper()
	ws := server.NewWorkerServer(leases, nil, server.WorkerConfig{LeaseTTL: time.Second, PollFloor: 5 * time.Millisecond})
	conn := dial(t, ws.Register, nil)
	return jobrunv1.NewWorkerServiceClient(conn), ws
}

// claimOne leases one queued job to owner "remote:w" through the store
// directly (the stream is tested in pkg/jobworker).
func claimOne(t *testing.T, leases *jobtest.Leases) job.Job {
	t.Helper()
	js, err := leases.Claim(context.Background(), job.ClaimQuery{Queue: "q", Owner: server.RemoteOwnerPrefix + "w", Limit: 1, LeaseTTL: time.Second})
	if err != nil || len(js) != 1 {
		t.Fatalf("claim: %v %v", js, err)
	}
	return js[0]
}

func pbLease(j job.Job) *jobrunv1.Lease {
	return &jobrunv1.Lease{JobId: j.ID.String(), Token: j.LeaseToken}
}

func TestWorkerServiceUnaryValidation(t *testing.T) {
	t.Parallel()
	leases := jobtest.NewLeases()
	c, _ := newWorkerClient(t, leases)
	ctx := context.Background()
	good := &jobrunv1.Lease{JobId: job.NewID().String(), Token: 1}
	many := make([]*jobrunv1.Lease, server.MaxLeasesPerHeartbeat+1)
	for i := range many {
		many[i] = good
	}
	tests := []struct {
		name string
		call func() error
		want codes.Code
	}{
		{"heartbeat empty worker id", func() error {
			_, err := c.Heartbeat(ctx, &jobrunv1.HeartbeatRequest{Leases: []*jobrunv1.Lease{good}})
			return err
		}, codes.InvalidArgument},
		{"heartbeat worker id with space", func() error {
			_, err := c.Heartbeat(ctx, &jobrunv1.HeartbeatRequest{WorkerId: "a b"})
			return err
		}, codes.InvalidArgument},
		{"heartbeat worker id too long", func() error {
			_, err := c.Heartbeat(ctx, &jobrunv1.HeartbeatRequest{WorkerId: strings.Repeat("w", server.MaxWorkerIDLen+1)})
			return err
		}, codes.InvalidArgument},
		{"heartbeat bad job id", func() error {
			_, err := c.Heartbeat(ctx, &jobrunv1.HeartbeatRequest{WorkerId: "w", Leases: []*jobrunv1.Lease{{JobId: "nope", Token: 1}}})
			return err
		}, codes.InvalidArgument},
		{"heartbeat zero token", func() error {
			_, err := c.Heartbeat(ctx, &jobrunv1.HeartbeatRequest{WorkerId: "w", Leases: []*jobrunv1.Lease{{JobId: good.GetJobId()}}})
			return err
		}, codes.InvalidArgument},
		{"heartbeat too many leases", func() error {
			_, err := c.Heartbeat(ctx, &jobrunv1.HeartbeatRequest{WorkerId: "w", Leases: many})
			return err
		}, codes.InvalidArgument},
		{"heartbeat empty is fine", func() error {
			_, err := c.Heartbeat(ctx, &jobrunv1.HeartbeatRequest{WorkerId: "w"})
			return err
		}, codes.OK},
		{"complete without lease", func() error {
			_, err := c.Complete(ctx, &jobrunv1.CompleteRequest{WorkerId: "w"})
			return err
		}, codes.InvalidArgument},
		{"complete bad worker", func() error {
			_, err := c.Complete(ctx, &jobrunv1.CompleteRequest{WorkerId: "\x00", Lease: good})
			return err
		}, codes.InvalidArgument},
		{"complete unknown job", func() error {
			_, err := c.Complete(ctx, &jobrunv1.CompleteRequest{WorkerId: "w", Lease: good})
			return err
		}, codes.FailedPrecondition},
		{"fail negative token", func() error {
			_, err := c.Fail(ctx, &jobrunv1.FailRequest{WorkerId: "w", Lease: &jobrunv1.Lease{JobId: good.GetJobId(), Token: -3}})
			return err
		}, codes.InvalidArgument},
		{"fail unknown job", func() error {
			_, err := c.Fail(ctx, &jobrunv1.FailRequest{WorkerId: "w", Lease: good})
			return err
		}, codes.NotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := status.Code(tc.call()); got != tc.want {
				t.Fatalf("code = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestWorkerServiceClaimValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		hello *jobrunv1.ClaimRequest
	}{
		{"no worker id", &jobrunv1.ClaimRequest{Queues: []string{"q"}, Credits: 1}},
		{"no queues", &jobrunv1.ClaimRequest{WorkerId: "w", Credits: 1}},
		{"bad queue", &jobrunv1.ClaimRequest{WorkerId: "w", Queues: []string{"Bad Queue"}, Credits: 1}},
		{"too many credits", &jobrunv1.ClaimRequest{WorkerId: "w", Queues: []string{"q"}, Credits: server.MaxCreditsPerMessage + 1}},
		{"too many queues", &jobrunv1.ClaimRequest{WorkerId: "w", Queues: make([]string, server.MaxClaimQueues+1), Credits: 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, _ := newWorkerClient(t, jobtest.NewLeases())
			stream, err := c.Claim(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.Send(tc.hello); err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("want InvalidArgument, got %v", err)
			}
		})
	}
}

// TestWorkerServiceFencing: every terminal call and heartbeat is checked
// against (owner, token); a lease that moved on is reported lost and refused.
func TestWorkerServiceFencing(t *testing.T) {
	t.Parallel()
	leases := jobtest.NewLeases()
	c, ws := newWorkerClient(t, leases)
	ctx := context.Background()
	leases.Add(
		job.Job{ID: job.NewID(), Queue: "q", Type: "t"},
		job.Job{ID: job.NewID(), Queue: "q", Type: "t"},
		job.Job{ID: job.NewID(), Queue: "q", Type: "t"},
	)
	keep, cancelled, stolen := claimOne(t, leases), claimOne(t, leases), claimOne(t, leases)
	leases.RequestCancel(cancelled.ID)
	leases.Steal(stolen.ID)

	hb, err := c.Heartbeat(ctx, &jobrunv1.HeartbeatRequest{WorkerId: "w", Leases: []*jobrunv1.Lease{pbLease(keep), pbLease(cancelled), pbLease(stolen)}})
	if err != nil {
		t.Fatal(err)
	}
	one := func(ls []*jobrunv1.Lease, want job.Job) bool {
		return len(ls) == 1 && ls[0].GetJobId() == want.ID.String() && ls[0].GetToken() == want.LeaseToken
	}
	if !one(hb.GetKept(), keep) || !one(hb.GetCancelRequested(), cancelled) || !one(hb.GetLost(), stolen) ||
		hb.GetLeaseTtl().AsDuration() != time.Second {
		t.Fatalf("heartbeat = %v", hb)
	}
	if ws.Stats().Fenced != 1 {
		t.Fatalf("stats = %+v", ws.Stats())
	}
	// Another worker id is a different owner: nothing is kept.
	if hb, err := c.Heartbeat(ctx, &jobrunv1.HeartbeatRequest{WorkerId: "other", Leases: []*jobrunv1.Lease{pbLease(keep)}}); err != nil || len(hb.GetLost()) != 1 {
		t.Fatalf("other owner heartbeat = %v / %v", hb, err)
	}
	// The stolen lease cannot complete or fail.
	if _, err := c.Complete(ctx, &jobrunv1.CompleteRequest{WorkerId: "w", Lease: pbLease(stolen)}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("complete stolen: %v", err)
	}
	if _, err := c.Fail(ctx, &jobrunv1.FailRequest{WorkerId: "w", Lease: pbLease(stolen), Error: "x"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("fail stolen: %v", err)
	}
	// The cancel-requested one fails into cancelled; the kept one completes.
	fr, err := c.Fail(ctx, &jobrunv1.FailRequest{WorkerId: "w", Lease: pbLease(cancelled), Error: "context canceled"})
	if err != nil || fr.GetStatus() != jobrunv1.JobStatus_JOB_STATUS_CANCELLED {
		t.Fatalf("fail cancelled: %v / %v", fr, err)
	}
	if _, err := c.Complete(ctx, &jobrunv1.CompleteRequest{WorkerId: "w", Lease: pbLease(keep)}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if _, err := c.Complete(ctx, &jobrunv1.CompleteRequest{WorkerId: "w", Lease: pbLease(keep)}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second complete: %v", err)
	}
	if st := ws.Stats(); st.Completed != 1 || st.Failed != 1 || st.LeaseLost != 3 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestWorkerServiceFailOutcomes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		attempts int32 // prior attempts before the claim
		perm     bool
		errText  string
		want     jobrunv1.JobStatus
		wantErr  string
	}{
		{"retry", 0, false, "boom", jobrunv1.JobStatus_JOB_STATUS_QUEUED, "boom"},
		{"permanent", 0, true, "bad", jobrunv1.JobStatus_JOB_STATUS_DEAD, "bad"},
		{"exhausted", 2, false, "boom", jobrunv1.JobStatus_JOB_STATUS_DEAD, "boom"},
		{"hostile cause is sanitised", 0, false, "nul\x00byte", jobrunv1.JobStatus_JOB_STATUS_QUEUED, "nul"},
		{"empty cause gets a default", 0, false, "", jobrunv1.JobStatus_JOB_STATUS_QUEUED, "remote worker reported failure"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			leases := jobtest.NewLeases()
			c, _ := newWorkerClient(t, leases)
			leases.Add(job.Job{ID: job.NewID(), Queue: "q", Type: "t", Attempts: tc.attempts, MaxAttempts: 3})
			j := claimOne(t, leases)
			resp, err := c.Fail(context.Background(), &jobrunv1.FailRequest{WorkerId: "w", Lease: pbLease(j), Error: tc.errText, Permanent: tc.perm})
			if err != nil || resp.GetStatus() != tc.want {
				t.Fatalf("fail = %v / %v, want %v", resp, err, tc.want)
			}
			call := <-leases.Calls
			if call.Perm != tc.perm || !strings.Contains(call.Cause, tc.wantErr) || strings.ContainsRune(call.Cause, 0) {
				t.Fatalf("store call = %+v", call)
			}
		})
	}
}

// TestEnqueueStoresTraceParent: the caller's trace reaches the server through
// the gRPC metadata, Enqueue opens jobrun.enqueue under it and stores that
// span's traceparent with the job. Without a traced caller nothing is stored
// when tracing is off (the no-op tracer has no valid span context).
func TestEnqueueStoresTraceParent(t *testing.T) { //nolint:paralleltest // swaps the global tracer provider
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev); _ = tp.Shutdown(context.Background()) })
	prop := propagation.TraceContext{}

	var stored job.Job
	fake := &fakeStore{enqueue: func(_ context.Context, j job.Job) (job.Job, error) {
		stored = j
		return j, nil
	}}
	conn := dial(t, server.NewJobServer(fake, nil).Register,
		[]grpc.ServerOption{grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithTracerProvider(tp), otelgrpc.WithPropagators(prop)))},
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelgrpc.WithTracerProvider(tp), otelgrpc.WithPropagators(prop))),
	)
	client := jobrunv1.NewJobServiceClient(conn)

	ctx, caller := tp.Tracer("test").Start(context.Background(), "producer")
	resp, err := client.Enqueue(ctx, &jobrunv1.EnqueueRequest{Queue: "q", Type: "t"})
	caller.End()
	if err != nil {
		t.Fatal(err)
	}
	if stored.TraceParent == "" || resp.GetJob().GetTraceparent() != stored.TraceParent {
		t.Fatalf("stored traceparent %q, response %q", stored.TraceParent, resp.GetJob().GetTraceparent())
	}
	var enqueue tracetest.SpanStub
	for _, s := range exp.GetSpans() {
		if s.Name == obs.SpanEnqueue {
			enqueue = s
		}
	}
	if enqueue.SpanContext.TraceID() != caller.SpanContext().TraceID() {
		t.Fatalf("enqueue span trace %s, caller trace %s", enqueue.SpanContext.TraceID(), caller.SpanContext().TraceID())
	}
	want := "00-" + enqueue.SpanContext.TraceID().String() + "-" + enqueue.SpanContext.SpanID().String() + "-01"
	if stored.TraceParent != want {
		t.Fatalf("traceparent %q, want the enqueue span %q", stored.TraceParent, want)
	}

	// An untraced caller with tracing off (no-op provider) stores nothing.
	otel.SetTracerProvider(tracenoop.NewTracerProvider())
	plain := dial(t, server.NewJobServer(fake, nil).Register, nil)
	if _, err := jobrunv1.NewJobServiceClient(plain).Enqueue(context.Background(), &jobrunv1.EnqueueRequest{Queue: "q", Type: "t"}); err != nil {
		t.Fatal(err)
	}
	if stored.TraceParent != "" {
		t.Fatalf("untraced enqueue stored %q", stored.TraceParent)
	}
}

// TestWorkerServiceDrainEndsStreams: Drain ends open Claim streams (so
// GracefulStop is not held up by them) and refuses new ones.
func TestWorkerServiceDrainEndsStreams(t *testing.T) {
	t.Parallel()
	c, ws := newWorkerClient(t, jobtest.NewLeases())
	stream, err := c.Claim(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&jobrunv1.ClaimRequest{WorkerId: "w", Queues: []string{"q"}, Credits: 4}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for ws.Stats().Streams != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	ws.Drain()
	ws.Drain() // idempotent
	if _, err := stream.Recv(); err == nil {
		t.Fatal("stream still open after Drain")
	}
	again, err := c.Claim(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := again.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("new stream after Drain: want Unavailable got %v", err)
	}
	if ws.Stats().Streams != 0 {
		t.Fatalf("stats = %+v", ws.Stats())
	}
}
