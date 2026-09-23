package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	jobrunv1 "github.com/crowbett/jobrun/gen/jobrun/v1"
	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/internal/obs"
)

// Leases is the slice of job.Store that WorkerServer drives.
type Leases interface {
	Claim(ctx context.Context, q job.ClaimQuery) ([]job.Job, error)
	Heartbeat(ctx context.Context, owner string, leases []job.Lease, until time.Time) (job.Renewal, error)
	Complete(ctx context.Context, l job.Lease, owner string, fn job.Commit) error
	Fail(ctx context.Context, l job.Lease, owner, cause string, perm bool, retryAt time.Time) (job.Status, error)
	Get(ctx context.Context, id job.ID) (job.Job, error)
}

// EventPublisher receives the transitions WorkerServer drives, exactly like
// the in-process pool publishes its own (admin.Broker satisfies it).
type EventPublisher interface {
	Publish(job.Event)
}

// Limits of the worker protocol.
const (
	// RemoteOwnerPrefix prefixes the lease_owner of every remote worker, so
	// `jobrun list` tells in-process and remote leases apart.
	RemoteOwnerPrefix = "remote:"
	// MaxWorkerIDLen bounds ClaimRequest.worker_id.
	MaxWorkerIDLen = 128
	// MaxCreditsPerMessage bounds ClaimRequest.credits.
	MaxCreditsPerMessage = 1024
	// MaxLeasesPerHeartbeat bounds HeartbeatRequest.leases.
	MaxLeasesPerHeartbeat = 4096
	// MaxClaimQueues bounds ClaimRequest.queues.
	MaxClaimQueues = 32
)

// WorkerConfig tunes WorkerServer; zero fields take the defaults.
type WorkerConfig struct {
	// LeaseTTL is the lease granted per claim and per heartbeat (30 s).
	LeaseTTL time.Duration
	// PollFloor is the claim loop's tick when the dispatcher is silent (200 ms).
	PollFloor time.Duration
	// BatchSize is the most jobs one Claim round trip asks for (32).
	BatchSize int
	// MaxOutstanding caps the credits a stream may hold at once (1024).
	MaxOutstanding int
}

func (c WorkerConfig) withDefaults() WorkerConfig {
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 30 * time.Second
	}
	if c.PollFloor <= 0 {
		c.PollFloor = 200 * time.Millisecond
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 32
	}
	if c.MaxOutstanding <= 0 {
		c.MaxOutstanding = MaxCreditsPerMessage
	}
	return c
}

// WorkerStats are WorkerServer's cumulative counters.
type WorkerStats struct {
	Streams   int64 // Claim streams open right now
	Delivered int64 // jobs pushed down a Claim stream
	Completed int64 // Complete calls that committed
	Failed    int64 // Fail calls that committed
	// Fenced counts leases a Heartbeat reported lost to a remote worker (it
	// must cancel those handlers and never finish them).
	Fenced int64
	// LeaseLost counts Complete/Fail calls refused because the lease was gone.
	LeaseLost int64
	// Undelivered counts jobs claimed for a stream that broke before the send;
	// they are failed straight back to the queue.
	Undelivered int64
}

// WorkerServer implements jobrunv1.WorkerServiceServer: it leases jobs to
// out-of-process workers. Leases are fenced by (owner, token) exactly like the
// in-process pool's, so the reaper and a remote worker can never both finish a
// job. Effects of remote handlers are at-least-once: nothing the worker does
// can commit in the completion transaction.
type WorkerServer struct {
	jobrunv1.UnimplementedWorkerServiceServer

	leases Leases
	disp   job.Dispatcher
	pub    EventPublisher
	log    *slog.Logger
	cfg    WorkerConfig
	now    func() time.Time

	streams, delivered, completed, failed, fenced, leaseLost, undelivered atomic.Int64

	// inflight remembers queue/type/attempt of delivered jobs so Complete can
	// publish a full event without a read; entries a crashed worker leaves
	// behind are swept once they have gone 2 x LeaseTTL without a heartbeat.
	mu        sync.Mutex
	inflight  map[job.Lease]inflightJob
	lastSweep time.Time

	done      chan struct{} // closed by Drain: Claim streams end
	drainOnce sync.Once
}

type inflightJob struct {
	queue, typ string
	attempt    int32
	lastSeen   time.Time
}

// WorkerOption configures NewWorkerServer.
type WorkerOption func(*WorkerServer)

// WithDispatcher wakes Claim streams on new work (postgres.Notifier); without
// one they poll at PollFloor.
func WithDispatcher(d job.Dispatcher) WorkerOption { return func(s *WorkerServer) { s.disp = d } }

// WithPublisher sends remote transitions to the admin broker.
func WithPublisher(p EventPublisher) WorkerOption { return func(s *WorkerServer) { s.pub = p } }

// NewWorkerServer builds the adapter. A nil logger discards.
func NewWorkerServer(l Leases, log *slog.Logger, cfg WorkerConfig, opts ...WorkerOption) *WorkerServer {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	s := &WorkerServer{leases: l, log: log, cfg: cfg.withDefaults(), now: time.Now, inflight: make(map[job.Lease]inflightJob), done: make(chan struct{})}
	for _, o := range opts {
		o(s)
	}
	if s.disp == nil {
		s.disp = job.PollOnly{Every: s.cfg.PollFloor}
	}
	return s
}

// Register attaches the service to a gRPC server (or any ServiceRegistrar).
func (s *WorkerServer) Register(reg grpc.ServiceRegistrar) {
	jobrunv1.RegisterWorkerServiceServer(reg, s)
}

// Drain ends every open Claim stream (and refuses new ones) so that
// grpc.Server.GracefulStop does not wait on long-lived streams. Remote
// workers keep heartbeating and reporting the jobs they already hold until
// the server is gone. Safe to call more than once.
func (s *WorkerServer) Drain() {
	s.drainOnce.Do(func() { close(s.done) })
}

// Stats returns a snapshot of the counters.
func (s *WorkerServer) Stats() WorkerStats {
	return WorkerStats{
		Streams:     s.streams.Load(),
		Delivered:   s.delivered.Load(),
		Completed:   s.completed.Load(),
		Failed:      s.failed.Load(),
		Fenced:      s.fenced.Load(),
		LeaseLost:   s.leaseLost.Load(),
		Undelivered: s.undelivered.Load(),
	}
}

// validWorkerID: 1..MaxWorkerIDLen bytes of printable ASCII without spaces
// (it ends up in lease_owner and in logs).
func validWorkerID(id string) error {
	if id == "" || len(id) > MaxWorkerIDLen {
		return fmt.Errorf("worker_id must be 1..%d bytes", MaxWorkerIDLen)
	}
	for i := range len(id) {
		if c := id[i]; c <= ' ' || c > '~' {
			return errors.New("worker_id must be printable ASCII without spaces")
		}
	}
	return nil
}

func ownerOf(workerID string) (string, error) {
	if err := validWorkerID(workerID); err != nil {
		return "", invalid(err.Error())
	}
	return RemoteOwnerPrefix + workerID, nil
}

func leaseFromProto(l *jobrunv1.Lease) (job.Lease, error) {
	if l == nil {
		return job.Lease{}, invalid("lease is required")
	}
	id, err := parseID(l.GetJobId())
	if err != nil {
		return job.Lease{}, err
	}
	if l.GetToken() <= 0 {
		return job.Lease{}, invalid("lease token must be positive")
	}
	return job.Lease{ID: id, Token: l.GetToken()}, nil
}

// credits is a stream's outstanding credit balance plus a coalescing wake-up.
type credits struct {
	mu    sync.Mutex
	n     int
	limit int
	wake  chan struct{}
}

func (c *credits) add(n int32) error {
	if n < 0 || n > MaxCreditsPerMessage {
		return invalid(fmt.Sprintf("credits must be 0..%d", MaxCreditsPerMessage))
	}
	c.mu.Lock()
	c.n = min(c.n+int(n), c.limit)
	c.mu.Unlock()
	c.signal()
	return nil
}

func (c *credits) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *credits) available() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func (c *credits) spend() {
	c.mu.Lock()
	c.n--
	c.mu.Unlock()
}

// Claim is the lease stream; see the proto for the protocol.
func (s *WorkerServer) Claim(stream grpc.BidiStreamingServer[jobrunv1.ClaimRequest, jobrunv1.ClaimResponse]) error {
	ctx := stream.Context()
	select {
	case <-s.done:
		return status.Error(codes.Unavailable, "worker service is draining")
	default:
	}
	hello, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	owner, err := ownerOf(hello.GetWorkerId())
	if err != nil {
		return err
	}
	queues := hello.GetQueues()
	if len(queues) == 0 || len(queues) > MaxClaimQueues {
		return invalid(fmt.Sprintf("queues: want 1..%d", MaxClaimQueues))
	}
	for _, q := range queues {
		if err := job.ValidateName("queue", q); err != nil {
			return toStatus(ctx, s.log, "Claim", err)
		}
	}
	cr := &credits{limit: s.cfg.MaxOutstanding, wake: make(chan struct{}, 1)}
	if err := cr.add(hello.GetCredits()); err != nil {
		return err
	}
	s.streams.Add(1)
	defer s.streams.Add(-1)
	log := s.log.With("owner", owner, "queues", queues)
	log.InfoContext(ctx, "worker stream opened")
	defer log.InfoContext(ctx, "worker stream closed")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	recvErr := make(chan error, 1)
	go func() {
		for {
			req, err := stream.Recv()
			if err == nil {
				err = cr.add(req.GetCredits())
			}
			if err != nil {
				recvErr <- err
				return
			}
		}
	}()
	work := make(chan struct{}, 1) // coalesced "a queue may have work"
	for _, q := range queues {
		ch := s.disp.Wait(ctx, q)
		go func() {
			for range ch {
				select {
				case work <- struct{}{}:
				default:
				}
			}
		}()
	}
	floor := time.NewTicker(s.cfg.PollFloor)
	defer floor.Stop()

	// A returned credit only triggers a claim when the last fill stopped for
	// lack of credits; when it stopped for lack of work, only the dispatcher or
	// the poll floor does. Otherwise every finished job would cost an empty
	// claim transaction.
	next := 0 // round-robin start so one busy queue cannot starve the others
	needFill, creditBound := true, false
	for {
		if needFill {
			var err error
			if creditBound, err = s.fill(ctx, stream, owner, queues, &next, cr); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-s.done:
			return nil
		case err := <-recvErr:
			if errors.Is(err, io.EOF) {
				return nil // the worker half-closed: stop pushing
			}
			if _, ok := status.FromError(err); ok {
				return err
			}
			return nil // transport gone; ctx follows
		case <-cr.wake:
			needFill = creditBound
		case <-work:
			needFill = true
		case <-floor.C:
			needFill = true
		}
	}
}

// fill claims and sends while the stream has credits and some queue has work.
// creditBound reports that it stopped because the credits ran out (so a new
// credit should trigger another fill) rather than because the queues were empty.
func (s *WorkerServer) fill(ctx context.Context, stream grpc.BidiStreamingServer[jobrunv1.ClaimRequest, jobrunv1.ClaimResponse],
	owner string, queues []string, next *int, cr *credits,
) (creditBound bool, err error) {
	for {
		got := false
		for i := range queues {
			want := min(cr.available(), s.cfg.BatchSize)
			if want <= 0 {
				return true, nil
			}
			q := queues[(*next+i)%len(queues)]
			jobs, err := s.leases.Claim(ctx, job.ClaimQuery{
				Queue: q, Owner: owner, Limit: int32(want), LeaseTTL: s.cfg.LeaseTTL, //nolint:gosec // want <= BatchSize
			})
			if err != nil {
				if ctx.Err() != nil {
					return false, ctx.Err() // the stream is gone; nobody reads this status
				}
				return false, toStatus(ctx, s.log, "Claim", err)
			}
			for k := range jobs {
				if err := s.send(stream, jobs[k]); err != nil {
					s.release(owner, jobs[k:], err)
					return false, nil // the stream is gone; its ctx ends the loop
				}
				cr.spend()
			}
			got = got || len(jobs) > 0
		}
		*next = (*next + 1) % len(queues)
		if !got {
			return false, nil
		}
	}
}

func (s *WorkerServer) send(stream grpc.BidiStreamingServer[jobrunv1.ClaimRequest, jobrunv1.ClaimResponse], j job.Job) error {
	err := stream.Send(&jobrunv1.ClaimResponse{Job: &jobrunv1.LeasedJob{
		Job:         jobToProto(j),
		LeaseToken:  j.LeaseToken,
		LeaseTtl:    durationpb.New(s.cfg.LeaseTTL),
		Traceparent: j.TraceParent,
	}})
	if err != nil {
		return err
	}
	s.delivered.Add(1)
	s.track(j)
	s.publish(j, job.StatusQueued, job.StatusLeased, "")
	return nil
}

// track remembers a delivered job and sweeps stale entries at most once per
// LeaseTTL.
func (s *WorkerServer) track(j job.Job) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflight[job.LeaseOf(j)] = inflightJob{queue: j.Queue, typ: j.Type, attempt: j.Attempts, lastSeen: now}
	if now.Sub(s.lastSweep) < s.cfg.LeaseTTL {
		return
	}
	s.lastSweep = now
	for l, e := range s.inflight {
		if now.Sub(e.lastSeen) > 2*s.cfg.LeaseTTL {
			delete(s.inflight, l)
		}
	}
}

// touch marks leases as heartbeated.
func (s *WorkerServer) touch(ls []job.Lease) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range ls {
		if e, ok := s.inflight[l]; ok {
			e.lastSeen = now
			s.inflight[l] = e
		}
	}
}

// untrack forgets l and returns what was known about its job (ID only when unknown).
func (s *WorkerServer) untrack(l job.Lease) job.Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.inflight[l]
	if !ok {
		return job.Job{ID: l.ID}
	}
	delete(s.inflight, l)
	return job.Job{ID: l.ID, Queue: e.queue, Type: e.typ, Attempts: e.attempt}
}

func leasesToProto(ls []job.Lease) []*jobrunv1.Lease {
	out := make([]*jobrunv1.Lease, 0, len(ls))
	for _, l := range ls {
		out = append(out, &jobrunv1.Lease{JobId: l.ID.String(), Token: l.Token})
	}
	return out
}

// release fails jobs claimed for a stream that broke before they were sent,
// straight back to the queue (the attempt is burned: attempts count starts,
// and the claim was one). Anything that cannot be released is reaped later.
func (s *WorkerServer) release(owner string, js []job.Job, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := range js {
		s.undelivered.Add(1)
		msg := job.SanitizeError("undelivered: worker stream closed: " + cause.Error())
		st, err := s.leases.Fail(ctx, job.LeaseOf(js[i]), owner, msg, false, s.now())
		if err != nil {
			s.log.Warn("worker stream: could not release an undelivered job; the reaper will", "job", js[i].ID, "err", err)
			continue
		}
		s.publish(js[i], job.StatusLeased, st, msg)
	}
}

// Heartbeat extends a remote worker's leases and reports kept, cancelled and lost.
func (s *WorkerServer) Heartbeat(ctx context.Context, req *jobrunv1.HeartbeatRequest) (*jobrunv1.HeartbeatResponse, error) {
	owner, err := ownerOf(req.GetWorkerId())
	if err != nil {
		return nil, err
	}
	if len(req.GetLeases()) > MaxLeasesPerHeartbeat {
		return nil, invalid(fmt.Sprintf("at most %d leases per heartbeat", MaxLeasesPerHeartbeat))
	}
	leases := make([]job.Lease, 0, len(req.GetLeases()))
	for _, pl := range req.GetLeases() {
		l, err := leaseFromProto(pl)
		if err != nil {
			return nil, err
		}
		leases = append(leases, l)
	}
	r, err := s.leases.Heartbeat(ctx, owner, leases, s.now().Add(s.cfg.LeaseTTL))
	if err != nil {
		return nil, toStatus(ctx, s.log, "Heartbeat", err)
	}
	s.touch(r.Kept)
	s.touch(r.CancelRequested)
	seen := make(map[job.Lease]struct{}, len(r.Kept)+len(r.CancelRequested))
	for _, l := range r.Kept {
		seen[l] = struct{}{}
	}
	for _, l := range r.CancelRequested {
		seen[l] = struct{}{}
	}
	var lost []job.Lease
	for _, l := range leases {
		if _, ok := seen[l]; !ok {
			lost = append(lost, l)
			s.untrack(l)
		}
	}
	resp := &jobrunv1.HeartbeatResponse{
		Kept:            leasesToProto(r.Kept),
		CancelRequested: leasesToProto(r.CancelRequested),
		Lost:            leasesToProto(lost),
		LeaseTtl:        durationpb.New(s.cfg.LeaseTTL),
	}
	if n := len(resp.GetLost()); n > 0 {
		s.fenced.Add(int64(n))
		s.log.WarnContext(ctx, "remote worker lost leases", "owner", owner, "lost", len(lost))
	}
	return resp, nil
}

// Complete marks a remote job succeeded (or cancelled when a cancel was
// requested meanwhile). There is no Commit: remote effects are at-least-once.
func (s *WorkerServer) Complete(ctx context.Context, req *jobrunv1.CompleteRequest) (resp *jobrunv1.CompleteResponse, err error) {
	owner, err := ownerOf(req.GetWorkerId())
	if err != nil {
		return nil, err
	}
	l, err := leaseFromProto(req.GetLease())
	if err != nil {
		return nil, err
	}
	ctx = obs.WithTraceParent(ctx, req.GetTraceparent())
	ctx, span := obs.StartComplete(ctx, job.Job{ID: l.ID})
	defer func() { obs.End(span, err) }()
	if err := s.leases.Complete(ctx, l, owner, nil); err != nil {
		if errors.Is(err, job.ErrLeaseLost) {
			s.leaseLost.Add(1)
		}
		return nil, toStatus(ctx, s.log, "Complete", err)
	}
	s.completed.Add(1)
	s.publish(s.untrack(l), job.StatusLeased, job.StatusSucceeded, "")
	return &jobrunv1.CompleteResponse{Status: jobrunv1.JobStatus_JOB_STATUS_SUCCEEDED}, nil
}

// Fail records a remote attempt failure with the job's own backoff policy.
func (s *WorkerServer) Fail(ctx context.Context, req *jobrunv1.FailRequest) (resp *jobrunv1.FailResponse, err error) {
	owner, err := ownerOf(req.GetWorkerId())
	if err != nil {
		return nil, err
	}
	l, err := leaseFromProto(req.GetLease())
	if err != nil {
		return nil, err
	}
	ctx = obs.WithTraceParent(ctx, req.GetTraceparent())
	_, span := obs.StartComplete(ctx, job.Job{ID: l.ID})
	defer func() { obs.End(span, err) }()
	cur, err := s.leases.Get(ctx, l.ID)
	if err != nil {
		return nil, toStatus(ctx, s.log, "Fail", err)
	}
	if cur.Status != job.StatusLeased || cur.LeaseOwner != owner || cur.LeaseToken != l.Token {
		s.leaseLost.Add(1)
		return nil, toStatus(ctx, s.log, "Fail", fmt.Errorf("%w: job %s", job.ErrLeaseLost, l.ID))
	}
	cause := job.SanitizeError(req.GetError())
	if cause == "" {
		cause = "remote worker reported failure"
	}
	retryAt := s.now().Add(cur.Backoff.Next(cur.Attempts, nil))
	st, err := s.leases.Fail(ctx, l, owner, cause, req.GetPermanent(), retryAt)
	if err != nil {
		if errors.Is(err, job.ErrLeaseLost) {
			s.leaseLost.Add(1)
		}
		return nil, toStatus(ctx, s.log, "Fail", err)
	}
	s.failed.Add(1)
	s.untrack(l)
	s.publish(cur, job.StatusLeased, st, cause)
	return &jobrunv1.FailResponse{Status: statusToProto(st)}, nil
}

func (s *WorkerServer) publish(j job.Job, from, to job.Status, cause string) {
	if s.pub == nil {
		return
	}
	s.pub.Publish(job.Event{
		JobID: j.ID, Queue: j.Queue, Type: j.Type, From: from, To: to, Attempt: j.Attempts, At: s.now(), Err: cause,
	})
}
