// Package jobworker runs jobrun handlers in a separate process: it leases jobs
// from jobrund over jobrun.v1.WorkerService, runs them with bounded
// concurrency, heartbeats every lease with its fencing token and reports each
// outcome with Complete or Fail.
//
// # Delivery guarantees
//
// Remote handlers get at-least-once execution and at-least-once effects. A job
// is re-run when the worker dies or loses its lease mid-job, and whatever the
// handler already did outside jobrun (an HTTP call, a row in another database)
// is not rolled back. Make handlers idempotent - key their effects by Job.ID.
// The exactly-once path (job.TxHandler, whose effect commits in the same
// Postgres transaction as the job row) exists only for in-process handlers in
// jobrund: a remote process cannot join that transaction.
//
// # Lost-lock detection
//
// A lease is fenced - the handler's context is cancelled and the job is never
// completed or failed by this worker - when a heartbeat reports it lost
// (expired, reaped, or claimed again with a newer token), or when heartbeats
// keep failing until the lease could have expired (measured on the monotonic
// clock from receipt / the last successful heartbeat, minus one heartbeat
// period). Stats().Fenced counts them.
package jobworker

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	jobrunv1 "github.com/crowbett/jobrun/gen/jobrun/v1"
)

// ErrPermanent marks a handler error that must not be retried: wrap it
// (fmt.Errorf("%w: ...", jobworker.ErrPermanent)) and the job goes to dead.
var ErrPermanent = errors.New("permanent failure")

// ErrNoQueues is returned by Run when Config.Queues is empty.
var ErrNoQueues = errors.New("jobworker: no queues configured")

// Job is one leased job as a handler sees it.
type Job struct {
	ID          string
	Queue       string
	Type        string
	Payload     []byte
	Attempt     int32 // 1-based: attempts count starts, including this one
	MaxAttempts int32
	// TraceParent is the W3C trace context captured at enqueue ("" if untraced).
	TraceParent string
	// LeaseToken is the fencing token of this claim.
	LeaseToken int64
}

// Handler runs one job. It must honour ctx: it is cancelled on a lost lease, a
// cancel request, JobTimeout and when Run's drain budget runs out.
type Handler interface {
	Handle(ctx context.Context, j Job) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, j Job) error

// Handle calls f.
func (f HandlerFunc) Handle(ctx context.Context, j Job) error { return f(ctx, j) }

// Defaults for zero Config fields.
const (
	DefaultConcurrency  = 16
	DefaultJobTimeout   = 10 * time.Minute
	DefaultDrainTimeout = 30 * time.Second
	DefaultRPCTimeout   = 10 * time.Second
	// defaultLeaseTTL is assumed until the server's first lease says otherwise.
	defaultLeaseTTL = 30 * time.Second
)

// Config tunes a Worker. Zero fields take the defaults.
type Config struct {
	// WorkerID names this process in lease_owner ("remote:<id>"); default
	// "<host>-<pid>-<4 hex>", unique per process start.
	WorkerID string
	// Queues are the queues to claim from; Run fails without one.
	Queues []string
	// Concurrency bounds running handlers (DefaultConcurrency).
	Concurrency int
	// HeartbeatEvery is the heartbeat period; 0 means lease_ttl/3 as granted
	// by the server. It is clamped to lease_ttl/3.
	HeartbeatEvery time.Duration
	// JobTimeout bounds one handler call (DefaultJobTimeout).
	JobTimeout time.Duration
	// DrainTimeout bounds how long Run waits for in-flight handlers after ctx
	// is done before cancelling them (DefaultDrainTimeout).
	DrainTimeout time.Duration
	// RPCTimeout bounds each Heartbeat/Complete/Fail call (DefaultRPCTimeout).
	RPCTimeout time.Duration
	// Logger defaults to slog.Default().
	Logger *slog.Logger
	// TracerProvider for the jobrun.run spans; nil means otel's global one.
	TracerProvider trace.TracerProvider
}

// Stats are a Worker's cumulative counters.
type Stats struct {
	Received  int64 // jobs received from the Claim stream
	Succeeded int64 // Complete accepted
	Failed    int64 // Fail accepted (retried, dead or cancelled)
	// Fenced counts handlers cancelled because their lease was lost; their
	// result was dropped without Complete or Fail.
	Fenced int64
	// LeaseLost counts Complete/Fail the server refused (FAILED_PRECONDITION).
	LeaseLost int64
	// Unreported counts results that could not be reported (RPC errors);
	// the lease expires and the job runs again.
	Unreported int64
	InFlight   int64
}

// Worker leases and runs jobs. Create it with New, register handlers with
// Handle, then call Run once.
type Worker struct {
	client jobrunv1.WorkerServiceClient
	cfg    Config
	log    *slog.Logger
	tracer trace.Tracer

	handlers map[string]Handler
	sem      chan struct{}
	wg       sync.WaitGroup
	ttl      atomic.Int64  // lease TTL in ns, learned from the server
	ttlSet   chan struct{} // wakes the heartbeat loop to re-derive its period

	// inflight is keyed by (job id, token): after its lease was reaped this
	// worker may be handed the same job again while the fenced first attempt
	// is still unwinding.
	mu       sync.Mutex
	inflight map[leaseKey]*lease

	// creditMu makes "free a slot + grant it on the current stream" atomic with
	// "count free slots + send the hello + publish the stream", so a freed slot
	// is granted exactly once: by the hello or by a grant, never both.
	creditMu sync.Mutex
	stream   grpc.BidiStreamingClient[jobrunv1.ClaimRequest, jobrunv1.ClaimResponse] // current Claim stream, nil between reconnects
	sendMu   sync.Mutex                                                              // serialises Send on stream

	received, succeeded, failed, fenced, leaseLost, unreported, inFlight atomic.Int64

	now func() time.Time
}

type leaseState uint8

const (
	stateHeld leaseState = iota
	stateCancelRequested
	stateFenced
)

type leaseKey struct {
	id    string
	token int64
}

func keyOf(j Job) leaseKey { return leaseKey{id: j.ID, token: j.LeaseToken} }

type lease struct {
	job      Job
	cancel   context.CancelFunc
	state    leaseState
	deadline time.Time
}

// New builds a Worker over conn (a *grpc.ClientConn to jobrund).
func New(conn grpc.ClientConnInterface, cfg Config) *Worker {
	if cfg.WorkerID == "" {
		cfg.WorkerID = DefaultWorkerID()
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = DefaultConcurrency
	}
	if cfg.JobTimeout <= 0 {
		cfg.JobTimeout = DefaultJobTimeout
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = DefaultDrainTimeout
	}
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = DefaultRPCTimeout
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	tp := cfg.TracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	w := &Worker{
		client:   jobrunv1.NewWorkerServiceClient(conn),
		cfg:      cfg,
		log:      log.With("worker_id", cfg.WorkerID),
		tracer:   tp.Tracer("jobrun/jobworker"),
		handlers: make(map[string]Handler),
		sem:      make(chan struct{}, cfg.Concurrency),
		inflight: make(map[leaseKey]*lease),
		ttlSet:   make(chan struct{}, 1),
		now:      time.Now,
	}
	w.ttl.Store(int64(defaultLeaseTTL))
	return w
}

// DefaultWorkerID returns "<hostname>-<pid>-<4 hex>".
func DefaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	var b [2]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%d-%x", host, os.Getpid(), b)
}

// Handle registers h for job type typ. Call it before Run.
func (w *Worker) Handle(typ string, h Handler) error {
	if _, dup := w.handlers[typ]; dup {
		return fmt.Errorf("jobworker: handler for %q already registered", typ)
	}
	w.handlers[typ] = h
	return nil
}

// Stats returns a snapshot of the counters.
func (w *Worker) Stats() Stats {
	return Stats{
		Received:   w.received.Load(),
		Succeeded:  w.succeeded.Load(),
		Failed:     w.failed.Load(),
		Fenced:     w.fenced.Load(),
		LeaseLost:  w.leaseLost.Load(),
		Unreported: w.unreported.Load(),
		InFlight:   w.inFlight.Load(),
	}
}

func (w *Worker) leaseTTL() time.Duration { return time.Duration(w.ttl.Load()) }

// setTTL records the lease TTL the server granted and, when it changed, tells
// the heartbeat loop to re-derive its period.
func (w *Worker) setTTL(ttl time.Duration) {
	if ttl <= 0 || w.ttl.Swap(int64(ttl)) == int64(ttl) {
		return
	}
	select {
	case w.ttlSet <- struct{}{}:
	default:
	}
}

func (w *Worker) heartbeatEvery() time.Duration {
	every := w.leaseTTL() / 3
	if w.cfg.HeartbeatEvery > 0 && w.cfg.HeartbeatEvery < every {
		every = w.cfg.HeartbeatEvery
	}
	return max(every, time.Millisecond)
}

// Run leases and runs jobs until ctx is done, reconnecting the Claim stream
// with backoff when it breaks. Then it stops taking jobs, waits up to
// DrainTimeout for in-flight handlers (still heartbeating them), cancels the
// rest and returns nil. Leases of cancelled stragglers are still reported
// (Fail) when the handler returns in time; otherwise they expire and the
// reaper requeues them.
func (w *Worker) Run(ctx context.Context) error {
	if len(w.cfg.Queues) == 0 {
		return ErrNoQueues
	}
	base := context.WithoutCancel(ctx)
	hbCtx, hbCancel := context.WithCancel(base)
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		w.heartbeatLoop(hbCtx)
	}()

	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		err := w.session(ctx, base)
		if ctx.Err() != nil {
			break
		}
		w.log.Warn("jobworker: claim stream ended; reconnecting", "err", err, "in", backoff)
		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
		case <-t.C:
		}
		t.Stop()
		backoff = min(2*backoff, 5*time.Second)
		if err == nil {
			backoff = 100 * time.Millisecond
		}
	}

	w.log.Info("jobworker: stopped claiming, draining", "in_flight", w.inFlight.Load(), "budget", w.cfg.DrainTimeout)
	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()
	t := time.NewTimer(w.cfg.DrainTimeout)
	select {
	case <-done:
	case <-t.C:
		w.cancelAll()
		<-done
	}
	t.Stop()
	hbCancel()
	<-hbDone
	w.log.Info("jobworker: stopped", "stats", fmt.Sprintf("%+v", w.Stats()))
	return nil
}

// session runs one Claim stream until it breaks or ctx is done.
func (w *Worker) session(ctx, base context.Context) error {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := w.client.Claim(sctx)
	if err != nil {
		return fmt.Errorf("open claim stream: %w", err)
	}
	w.creditMu.Lock()
	free := int32(cap(w.sem) - len(w.sem)) //nolint:gosec // bounded by Concurrency
	w.sendMu.Lock()
	err = stream.Send(&jobrunv1.ClaimRequest{WorkerId: w.cfg.WorkerID, Queues: w.cfg.Queues, Credits: free})
	w.sendMu.Unlock()
	if err == nil {
		w.stream = stream
	}
	w.creditMu.Unlock()
	if err != nil {
		return fmt.Errorf("claim hello: %w", err)
	}
	defer func() {
		w.creditMu.Lock()
		w.stream = nil
		w.creditMu.Unlock()
		w.sendMu.Lock()
		_ = stream.CloseSend()
		w.sendMu.Unlock()
	}()
	w.log.Info("jobworker: claim stream open", "queues", w.cfg.Queues, "credits", free)
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return nil
			}
			return err
		}
		lj := resp.GetJob()
		if lj == nil || lj.GetJob() == nil {
			continue
		}
		w.setTTL(lj.GetLeaseTtl().AsDuration())
		// The server never sends more than the credits granted, so a slot is
		// free; block anyway rather than overrun Concurrency.
		w.sem <- struct{}{}
		w.received.Add(1)
		w.wg.Add(1)
		go w.run(base, toJob(lj), w.now())
	}
}

func toJob(lj *jobrunv1.LeasedJob) Job {
	pj := lj.GetJob()
	return Job{
		ID:          pj.GetId(),
		Queue:       pj.GetQueue(),
		Type:        pj.GetType(),
		Payload:     pj.GetPayload(),
		Attempt:     pj.GetAttempts(),
		MaxAttempts: pj.GetMaxAttempts(),
		TraceParent: lj.GetTraceparent(),
		LeaseToken:  lj.GetLeaseToken(),
	}
}

// release frees a slot and grants it on the current stream (if any); with no
// stream, the next session's hello counts it.
func (w *Worker) release() {
	w.creditMu.Lock()
	<-w.sem
	stream := w.stream
	w.creditMu.Unlock()
	if stream == nil {
		return
	}
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	if err := stream.Send(&jobrunv1.ClaimRequest{Credits: 1}); err != nil {
		w.log.Debug("jobworker: credit not sent; the stream is reconnecting", "err", err)
	}
}

func (w *Worker) run(base context.Context, j Job, receivedAt time.Time) {
	defer w.wg.Done()
	defer w.release()
	w.inFlight.Add(1)
	defer w.inFlight.Add(-1)
	log := w.log.With("job", j.ID, "queue", j.Queue, "type", j.Type, "attempt", j.Attempt, "token", j.LeaseToken)

	jctx, cancel := context.WithTimeout(base, w.cfg.JobTimeout)
	defer cancel()
	key := keyOf(j)
	w.track(j, cancel, receivedAt)
	defer w.untrack(key)

	rctx := jctx
	if j.TraceParent != "" {
		rctx = propagation.TraceContext{}.Extract(rctx, propagation.MapCarrier{"traceparent": j.TraceParent})
	}
	rctx, span := w.tracer.Start(rctx, "jobrun.run",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("jobrun.queue", j.Queue), attribute.String("jobrun.type", j.Type),
			attribute.String("jobrun.job.id", j.ID), attribute.Int64("jobrun.attempt", int64(j.Attempt)),
		))
	var spanErr error
	defer func() {
		if spanErr != nil {
			span.RecordError(spanErr)
			span.SetStatus(codes.Error, spanErr.Error())
		}
		span.End()
	}()

	err := w.handle(rctx, j)
	if w.state(key) == stateFenced {
		spanErr = errors.New("lease lost: fenced by heartbeat, not completed")
		log.Warn("jobworker: lease fenced; result dropped, the job will run again", "handler_err", err)
		return
	}
	tp := traceParentOf(rctx)
	pl := &jobrunv1.Lease{JobId: j.ID, Token: j.LeaseToken}
	if err == nil {
		rerr := w.call(base, func(ctx context.Context) error {
			_, err := w.client.Complete(ctx, &jobrunv1.CompleteRequest{WorkerId: w.cfg.WorkerID, Lease: pl, Traceparent: tp})
			return err
		})
		if w.reportError(log, "complete", rerr) {
			spanErr = rerr
			return
		}
		w.succeeded.Add(1)
		log.Debug("jobworker: job succeeded")
		return
	}
	spanErr = err
	cause := err.Error()
	if w.state(key) == stateCancelRequested {
		cause = "cancel requested: " + cause
	} else if errors.Is(jctx.Err(), context.DeadlineExceeded) {
		cause = fmt.Sprintf("job timeout after %s: %v", w.cfg.JobTimeout, err)
	}
	var st jobrunv1.JobStatus
	req := &jobrunv1.FailRequest{
		WorkerId: w.cfg.WorkerID, Lease: pl, Error: cause, Permanent: errors.Is(err, ErrPermanent), Traceparent: tp,
	}
	rerr := w.call(base, func(ctx context.Context) error {
		resp, ferr := w.client.Fail(ctx, req)
		st = resp.GetStatus()
		return ferr
	})
	if w.reportError(log, "fail", rerr) {
		return
	}
	w.failed.Add(1)
	log.Warn("jobworker: job failed", "status", st.String(), "err", err)
}

// handle runs the registered handler; a panic becomes a transient error.
func (w *Worker) handle(ctx context.Context, j Job) (err error) {
	h, ok := w.handlers[j.Type]
	if !ok {
		return fmt.Errorf("%w: no handler registered for type %q", ErrPermanent, j.Type)
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return h.Handle(ctx, j)
}

// call runs one report RPC, retrying transport errors (the lease may still be
// held) up to three times while the lease is not fenced.
func (w *Worker) call(base context.Context, fn func(context.Context) error) error {
	var err error
	for attempt := range 3 {
		ctx, cancel := context.WithTimeout(base, w.cfg.RPCTimeout)
		err = fn(ctx)
		cancel()
		switch status.Code(err) {
		case grpccodes.OK:
			return nil
		case grpccodes.Unavailable, grpccodes.DeadlineExceeded, grpccodes.ResourceExhausted, grpccodes.Aborted:
			time.Sleep(time.Duration(100*(attempt+1)) * time.Millisecond)
			continue
		default:
			return err
		}
	}
	return err
}

// reportError classifies a Complete/Fail error; it reports whether err was set.
func (w *Worker) reportError(log *slog.Logger, op string, err error) bool {
	if err == nil {
		return false
	}
	if status.Code(err) == grpccodes.FailedPrecondition {
		w.leaseLost.Add(1)
		log.Warn("jobworker: lease lost, "+op+" refused", "err", err)
		return true
	}
	w.unreported.Add(1)
	log.Error("jobworker: "+op+" failed; the lease will expire and the job run again", "err", err)
	return true
}

func traceParentOf(ctx context.Context) string {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ""
	}
	c := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, c)
	return c.Get("traceparent")
}

// ---- lease tracking and heartbeats ----------------------------------------

func (w *Worker) safeUntil(from time.Time) time.Time {
	return from.Add(w.leaseTTL() - w.heartbeatEvery())
}

func (w *Worker) track(j Job, cancel context.CancelFunc, receivedAt time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.inflight[keyOf(j)] = &lease{job: j, cancel: cancel, deadline: w.safeUntil(receivedAt)}
}

func (w *Worker) untrack(k leaseKey) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.inflight, k)
}

func (w *Worker) state(k leaseKey) leaseState {
	w.mu.Lock()
	defer w.mu.Unlock()
	if l, ok := w.inflight[k]; ok {
		return l.state
	}
	return stateHeld
}

func (w *Worker) fence(k leaseKey) bool {
	w.mu.Lock()
	l, ok := w.inflight[k]
	if !ok || l.state == stateFenced {
		w.mu.Unlock()
		return false
	}
	l.state = stateFenced
	l.cancel()
	w.mu.Unlock()
	w.fenced.Add(1)
	return true
}

func (w *Worker) cancelAll() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, l := range w.inflight {
		l.cancel()
	}
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(w.heartbeatEvery())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.ttlSet: // the server granted a different TTL
			t.Reset(w.heartbeatEvery())
		case <-t.C:
			w.heartbeat(ctx)
		}
	}
}

func (w *Worker) heartbeat(ctx context.Context) {
	w.mu.Lock()
	leases := make([]*jobrunv1.Lease, 0, len(w.inflight))
	for k, l := range w.inflight {
		if l.state != stateFenced {
			leases = append(leases, &jobrunv1.Lease{JobId: k.id, Token: k.token})
		}
	}
	w.mu.Unlock()
	if len(leases) == 0 {
		return
	}
	sentAt := w.now()
	hctx, cancel := context.WithTimeout(ctx, w.cfg.RPCTimeout)
	resp, err := w.client.Heartbeat(hctx, &jobrunv1.HeartbeatRequest{WorkerId: w.cfg.WorkerID, Leases: leases})
	cancel()
	if err != nil {
		w.log.Error("jobworker: heartbeat failed", "in_flight", len(leases), "err", err)
		now := w.now()
		w.mu.Lock()
		var expired []leaseKey
		for k, l := range w.inflight {
			if l.state != stateFenced && !now.Before(l.deadline) {
				expired = append(expired, k)
			}
		}
		w.mu.Unlock()
		for _, k := range expired {
			if w.fence(k) {
				w.log.Warn("jobworker: lease could not be renewed before expiry, fencing handler", "job", k.id, "token", k.token)
			}
		}
		return
	}
	w.setTTL(resp.GetLeaseTtl().AsDuration())
	deadline := w.safeUntil(sentAt)
	w.mu.Lock()
	for _, pl := range resp.GetKept() {
		if l, ok := w.inflight[leaseKey{pl.GetJobId(), pl.GetToken()}]; ok && l.state != stateFenced {
			l.deadline = deadline
		}
	}
	for _, pl := range resp.GetCancelRequested() {
		if l, ok := w.inflight[leaseKey{pl.GetJobId(), pl.GetToken()}]; ok && l.state != stateFenced {
			l.deadline = deadline
			if l.state == stateHeld {
				l.state = stateCancelRequested
				l.cancel()
			}
		}
	}
	w.mu.Unlock()
	for _, pl := range resp.GetLost() {
		if w.fence(leaseKey{pl.GetJobId(), pl.GetToken()}) {
			w.log.Warn("jobworker: lease lost, fencing handler (it will not be completed)", "job", pl.GetJobId(), "token", pl.GetToken())
		}
	}
}
