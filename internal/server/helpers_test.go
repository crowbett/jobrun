package server_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	jobrunv1 "github.com/crowbett/jobrun/gen/jobrun/v1"
	"github.com/crowbett/jobrun/internal/interceptor"
	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/internal/server"
)

// fakeStore is a function-table server.Jobs + server.Schedules; unset methods
// fail loudly so a test cannot pass by accident.
type fakeStore struct {
	enqueue func(context.Context, job.Job) (job.Job, error)
	get     func(context.Context, job.ID) (job.Job, error)
	events  func(context.Context, job.ID) ([]job.Event, error)
	list    func(context.Context, job.ListQuery) ([]job.Job, job.Cursor, error)
	cancel  func(context.Context, job.ID) (job.Job, error)
	retry   func(context.Context, job.ID) (job.Job, error)
	purge   func(context.Context, job.PurgeQuery) (int64, error)
	depth   func(context.Context) ([]job.DepthRow, error)

	createSchedule func(context.Context, job.Schedule) (job.Schedule, error)
	updateSchedule func(context.Context, job.Schedule) (job.Schedule, error)
	deleteSchedule func(context.Context, job.ID) error
	getSchedule    func(context.Context, job.ID) (job.Schedule, error)
	listSchedules  func(context.Context, string, int32) ([]job.Schedule, error)

	mu    sync.Mutex
	calls int
}

var errUnexpectedCall = errors.New("unexpected call to fake")

func (f *fakeStore) called() {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
}

func (f *fakeStore) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeStore) Enqueue(ctx context.Context, j job.Job) (job.Job, error) {
	f.called()
	if f.enqueue == nil {
		return job.Job{}, errUnexpectedCall
	}
	return f.enqueue(ctx, j)
}

func (f *fakeStore) Get(ctx context.Context, id job.ID) (job.Job, error) {
	f.called()
	if f.get == nil {
		return job.Job{}, errUnexpectedCall
	}
	return f.get(ctx, id)
}

func (f *fakeStore) Events(ctx context.Context, id job.ID) ([]job.Event, error) {
	f.called()
	if f.events == nil {
		return nil, errUnexpectedCall
	}
	return f.events(ctx, id)
}

func (f *fakeStore) List(ctx context.Context, q job.ListQuery) ([]job.Job, job.Cursor, error) {
	f.called()
	if f.list == nil {
		return nil, job.Cursor{}, errUnexpectedCall
	}
	return f.list(ctx, q)
}

func (f *fakeStore) Cancel(ctx context.Context, id job.ID) (job.Job, error) {
	f.called()
	if f.cancel == nil {
		return job.Job{}, errUnexpectedCall
	}
	return f.cancel(ctx, id)
}

func (f *fakeStore) Retry(ctx context.Context, id job.ID) (job.Job, error) {
	f.called()
	if f.retry == nil {
		return job.Job{}, errUnexpectedCall
	}
	return f.retry(ctx, id)
}

func (f *fakeStore) Purge(ctx context.Context, q job.PurgeQuery) (int64, error) {
	f.called()
	if f.purge == nil {
		return 0, errUnexpectedCall
	}
	return f.purge(ctx, q)
}

func (f *fakeStore) Depth(ctx context.Context) ([]job.DepthRow, error) {
	f.called()
	if f.depth == nil {
		return nil, errUnexpectedCall
	}
	return f.depth(ctx)
}

func (f *fakeStore) CreateSchedule(ctx context.Context, s job.Schedule) (job.Schedule, error) {
	f.called()
	if f.createSchedule == nil {
		return job.Schedule{}, errUnexpectedCall
	}
	return f.createSchedule(ctx, s)
}

func (f *fakeStore) UpdateSchedule(ctx context.Context, s job.Schedule) (job.Schedule, error) {
	f.called()
	if f.updateSchedule == nil {
		return job.Schedule{}, errUnexpectedCall
	}
	return f.updateSchedule(ctx, s)
}

func (f *fakeStore) DeleteSchedule(ctx context.Context, id job.ID) error {
	f.called()
	if f.deleteSchedule == nil {
		return errUnexpectedCall
	}
	return f.deleteSchedule(ctx, id)
}

func (f *fakeStore) GetSchedule(ctx context.Context, id job.ID) (job.Schedule, error) {
	f.called()
	if f.getSchedule == nil {
		return job.Schedule{}, errUnexpectedCall
	}
	return f.getSchedule(ctx, id)
}

func (f *fakeStore) ListSchedules(ctx context.Context, after string, limit int32) ([]job.Schedule, error) {
	f.called()
	if f.listSchedules == nil {
		return nil, errUnexpectedCall
	}
	return f.listSchedules(ctx, after, limit)
}

// syncBuffer is a goroutine-safe io.Writer for capturing log output.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// clients bundles the two service clients over one bufconn.
type clients struct {
	jobs      jobrunv1.JobServiceClient
	schedules jobrunv1.ScheduleServiceClient
	logs      *syncBuffer
}

// newTestClients spins up an in-memory gRPC server with the production
// interceptor chain and both services on top of fake. Everything is torn down
// via t.Cleanup.
func newTestClients(t *testing.T, fake *fakeStore, jobOpts ...server.JobOption) clients {
	t.Helper()

	logs := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	lis := bufconn.Listen(1024 * 1024)

	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			interceptor.UnaryRecover(logger),
			interceptor.UnaryRequestID(),
			interceptor.UnaryLogging(logger),
		),
		grpc.ChainStreamInterceptor(
			interceptor.StreamRecover(logger),
			interceptor.StreamRequestID(),
			interceptor.StreamLogging(logger),
		),
	)
	server.NewJobServer(fake, logger, jobOpts...).Register(srv)
	server.NewScheduleServer(fake, logger).Register(srv)

	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	})
	return clients{
		jobs:      jobrunv1.NewJobServiceClient(conn),
		schedules: jobrunv1.NewScheduleServiceClient(conn),
		logs:      logs,
	}
}

var (
	fixedNow   = time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)
	jobID      = uuid.MustParse("019210a0-1234-7abc-8def-0123456789ab")
	scheduleID = uuid.MustParse("019210a0-5678-7abc-8def-0123456789ab")
	// errBoom carries a string that must never reach the client.
	errBoom = errors.New("pg: dial db-secret-host:5434: connection refused")
)

func sampleJob() job.Job {
	started := fixedNow.Add(time.Second)
	return job.Job{
		ID:             jobID,
		Queue:          "default",
		Type:           "email.send",
		Payload:        []byte(`{"to":"x@y.z"}`),
		Status:         job.StatusLeased,
		Priority:       7,
		RunAt:          fixedNow,
		Attempts:       1,
		MaxAttempts:    5,
		Backoff:        job.DefaultPolicy,
		LeaseOwner:     "host-1",
		LeaseUntil:     ptr(fixedNow.Add(30 * time.Second)),
		IdempotencyKey: "key-1",
		CreatedAt:      fixedNow,
		UpdatedAt:      fixedNow,
		StartedAt:      &started,
	}
}

func sampleSchedule() job.Schedule {
	return job.Schedule{
		ID:        scheduleID,
		Name:      "nightly",
		Cron:      "0 3 * * *",
		Timezone:  "UTC",
		Template:  job.NewJob{Queue: "default", Type: "prune", Payload: []byte(`{}`), MaxAttempts: 3},
		Enabled:   true,
		NextRun:   fixedNow.Add(time.Hour),
		CreatedAt: fixedNow,
		UpdatedAt: fixedNow,
	}
}

func ptr[T any](v T) *T { return &v }

// errCase is one row of the per-RPC error tables: what the fake returns and
// what the client must see.
type errCase struct {
	name     string
	svcErr   error
	wantCode codes.Code
}

func jobErrCases() []errCase {
	return []errCase{
		{name: "ok", svcErr: nil, wantCode: codes.OK},
		{name: "invalid argument", svcErr: fmt.Errorf("%w: queue", job.ErrInvalidArgument), wantCode: codes.InvalidArgument},
		{name: "invalid cursor", svcErr: job.ErrInvalidCursor, wantCode: codes.InvalidArgument},
		{name: "not found", svcErr: fmt.Errorf("get: %w", job.ErrNotFound), wantCode: codes.NotFound},
		{name: "not cancellable", svcErr: job.ErrNotCancellable, wantCode: codes.FailedPrecondition},
		{name: "not retryable", svcErr: job.ErrNotRetryable, wantCode: codes.FailedPrecondition},
		{name: "invalid transition", svcErr: fmt.Errorf("%w: succeeded -> queued", job.ErrInvalidTransition), wantCode: codes.FailedPrecondition},
		{name: "already exists", svcErr: job.ErrIdempotencyConflict, wantCode: codes.AlreadyExists},
		{name: "cancelled", svcErr: context.Canceled, wantCode: codes.Canceled},
		{name: "internal", svcErr: errBoom, wantCode: codes.Internal},
	}
}

func scheduleErrCases() []errCase {
	return []errCase{
		{name: "ok", svcErr: nil, wantCode: codes.OK},
		{name: "not found", svcErr: fmt.Errorf("get: %w", job.ErrScheduleNotFound), wantCode: codes.NotFound},
		{name: "already exists", svcErr: job.ErrScheduleExists, wantCode: codes.AlreadyExists},
		{name: "invalid argument", svcErr: fmt.Errorf("%w: name", job.ErrInvalidArgument), wantCode: codes.InvalidArgument},
		{name: "internal", svcErr: errBoom, wantCode: codes.Internal},
	}
}

// checkStatus asserts the code and, for Internal, that the real error did not
// leak into the status message but did reach the log with the request id.
func checkStatus(t *testing.T, err error, want codes.Code, logs *syncBuffer, id string) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("code = %v, want %v (err %v)", got, want, err)
	}
	if want != codes.Internal {
		return
	}
	msg := status.Convert(err).Message()
	if msg != "internal error" {
		t.Errorf("internal message = %q, want %q", msg, "internal error")
	}
	if strings.Contains(msg, "secret") {
		t.Errorf("internal message leaks the cause: %q", msg)
	}
	out := logs.String()
	if !strings.Contains(out, requestID(id)) {
		t.Errorf("log does not carry request id %q:\n%s", requestID(id), out)
	}
	if !strings.Contains(out, "db-secret-host") {
		t.Errorf("log does not carry the real error:\n%s", out)
	}
}

// ctxWithRequestID sends id as x-request-id. Subtest names carry spaces,
// which the interceptor rejects (printable ASCII only), so they become
// dashes; checkStatus looks for the same spelling.
func ctxWithRequestID(t *testing.T, id string) context.Context {
	t.Helper()
	return metadata.AppendToOutgoingContext(t.Context(), interceptor.RequestIDHeader, requestID(id))
}

func requestID(id string) string { return strings.ReplaceAll(id, " ", "-") }

func checkJob(t *testing.T, got *jobrunv1.Job, want job.Job) {
	t.Helper()
	if got == nil {
		t.Fatal("job is nil")
	}
	if got.GetId() != want.ID.String() || got.GetQueue() != want.Queue || got.GetType() != want.Type ||
		!bytes.Equal(got.GetPayload(), want.Payload) || got.GetPriority() != int32(want.Priority) ||
		got.GetAttempts() != want.Attempts || got.GetMaxAttempts() != want.MaxAttempts ||
		got.GetLeaseOwner() != want.LeaseOwner || got.GetIdempotencyKey() != want.IdempotencyKey ||
		got.GetLastError() != want.LastError || got.GetCancelRequested() != want.CancelRequested {
		t.Errorf("job = %v, want %+v", got, want)
	}
	if got.GetStatus().String() != "JOB_STATUS_"+strings.ToUpper(want.Status.String()) {
		t.Errorf("status = %v, want %s", got.GetStatus(), want.Status)
	}
	if !got.GetRunAt().AsTime().Equal(want.RunAt) || !got.GetCreatedAt().AsTime().Equal(want.CreatedAt) {
		t.Errorf("timestamps = %v/%v, want %v", got.GetRunAt().AsTime(), got.GetCreatedAt().AsTime(), want.RunAt)
	}
	if (got.GetLeaseUntil() == nil) != (want.LeaseUntil == nil) {
		t.Errorf("lease_until = %v, want %v", got.GetLeaseUntil(), want.LeaseUntil)
	}
	if want.LeaseUntil != nil && !got.GetLeaseUntil().AsTime().Equal(*want.LeaseUntil) {
		t.Errorf("lease_until = %v, want %v", got.GetLeaseUntil().AsTime(), *want.LeaseUntil)
	}
	if (got.GetStartedAt() == nil) != (want.StartedAt == nil) || (got.GetFinishedAt() == nil) != (want.FinishedAt == nil) {
		t.Errorf("started/finished = %v/%v, want %v/%v", got.GetStartedAt(), got.GetFinishedAt(), want.StartedAt, want.FinishedAt)
	}
	b := got.GetBackoff()
	if want.Backoff.IsZero() {
		if b != nil {
			t.Errorf("backoff = %v, want nil for a zero policy", b)
		}
		return
	}
	if b == nil || b.GetBase().AsDuration() != want.Backoff.Base || b.GetMax().AsDuration() != want.Backoff.Max ||
		b.GetFactor() != want.Backoff.Factor || b.GetJitter() != want.Backoff.Jitter {
		t.Errorf("backoff = %v, want %+v", b, want.Backoff)
	}
}

func checkSchedule(t *testing.T, got *jobrunv1.Schedule, want job.Schedule) {
	t.Helper()
	if got == nil {
		t.Fatal("schedule is nil")
	}
	if got.GetId() != want.ID.String() || got.GetName() != want.Name || got.GetCron() != want.Cron ||
		got.GetTimezone() != want.Timezone || got.GetEnabled() != want.Enabled {
		t.Errorf("schedule = %v, want %+v", got, want)
	}
	if !got.GetNextRun().AsTime().Equal(want.NextRun) {
		t.Errorf("next_run = %v, want %v", got.GetNextRun().AsTime(), want.NextRun)
	}
	tm := got.GetTemplate()
	if tm.GetQueue() != want.Template.Queue || tm.GetType() != want.Template.Type ||
		tm.GetMaxAttempts() != want.Template.MaxAttempts || tm.GetPriority() != int32(want.Template.Priority) {
		t.Errorf("template = %v, want %+v", tm, want.Template)
	}
}
