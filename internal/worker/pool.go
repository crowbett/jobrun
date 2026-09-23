package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/internal/obs"
)

// EventPublisher receives every transition the pool drives (queued -> leased,
// leased -> succeeded|queued|dead|cancelled). Publish must not block: the admin
// broker drops for slow subscribers.
type EventPublisher interface {
	Publish(job.Event)
}

type nopPublisher struct{}

func (nopPublisher) Publish(job.Event) {}

// ErrNoQueues is returned by Run when Config.Queues is empty.
var ErrNoQueues = errors.New("worker: no queues configured")

// storeTimeout bounds each Complete/Fail/Heartbeat round trip.
const storeTimeout = 10 * time.Second

// Pool claims jobs from the configured queues and runs their handlers.
//
// Run may be called once; the pool is not reusable.
type Pool struct {
	store job.Store
	disp  job.Dispatcher
	reg   *job.Registry
	pub   EventPublisher
	log   *slog.Logger
	cfg   Config

	sem chan struct{} // one token per running handler, cap Concurrency
	hb  *heartbeater
	wg  sync.WaitGroup // in-flight handler goroutines
	st  counters

	now          func() time.Time
	claimBackoff time.Duration // pause after a failed claim
}

// NewPool wires a pool. d may be nil (a job.PollOnly ticker on PollFloor is
// used), pub may be nil (events are dropped), log may be nil (slog.Default).
func NewPool(s job.Store, d job.Dispatcher, reg *job.Registry, pub EventPublisher, log *slog.Logger, c Config) *Pool {
	c = c.withDefaults()
	if log == nil {
		log = slog.Default()
	}
	if pub == nil {
		pub = nopPublisher{}
	}
	if d == nil {
		d = job.PollOnly{Every: c.PollFloor}
	}
	p := &Pool{
		store:        s,
		disp:         d,
		reg:          reg,
		pub:          pub,
		log:          log,
		cfg:          c,
		sem:          make(chan struct{}, c.Concurrency),
		now:          time.Now,
		claimBackoff: time.Second,
	}
	p.hb = newHeartbeater(s, c.Owner, c.HeartbeatEvery, c.LeaseTTL, storeTimeout, log, p.now)
	p.hb.onFence = func() { p.st.fenced.Add(1) }
	return p
}

// Owner returns the lease owner string this pool claims with.
func (p *Pool) Owner() string { return p.cfg.Owner }

// Stats returns a snapshot of the counters.
func (p *Pool) Stats() Stats { return p.st.snapshot() }

// Run claims and executes jobs until ctx is done, then stops claiming, waits up
// to JobTimeout for in-flight handlers (heartbeating them meanwhile), cancels
// whatever is still running, waits for those goroutines to return and returns
// nil. Handler contexts are not cancelled by ctx: they end by finishing, by
// JobTimeout, by a lost lease or a cancel request, or by the drain budget.
func (p *Pool) Run(ctx context.Context) error {
	if len(p.cfg.Queues) == 0 {
		return ErrNoQueues
	}
	// base keeps ctx's values (trace, request id) but not its cancellation:
	// handlers and store round trips must outlive the stop signal.
	base := context.WithoutCancel(ctx)
	hbCtx, hbCancel := context.WithCancel(base)
	defer hbCancel()
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		p.hb.run(hbCtx)
	}()

	g, gctx := errgroup.WithContext(ctx)
	for _, q := range p.cfg.Queues {
		g.Go(func() error {
			p.claimLoop(gctx, base, q)
			return nil
		})
	}
	_ = g.Wait() // the loops only return once ctx is done and never with an error
	p.log.Info("worker: stopped claiming, draining", "in_flight", p.st.inFlight.Load(), "budget", p.cfg.JobTimeout)
	p.drain()
	hbCancel()
	<-hbDone
	return nil
}

func (p *Pool) drain() {
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(p.cfg.JobTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		n := p.hb.cancelAll()
		p.log.Warn("worker: drain budget exceeded, cancelling in-flight handlers", "in_flight", n)
		<-done
	}
}

// claimLoop claims for one queue until ctx is done. A full batch is followed by
// another claim at once; anything shorter waits for the dispatcher or the poll
// floor, whichever fires first.
func (p *Pool) claimLoop(ctx, base context.Context, queue string) {
	wake := p.disp.Wait(ctx, queue)
	floor := time.NewTicker(p.cfg.PollFloor)
	defer floor.Stop()
	for {
		full, err := p.claimOnce(ctx, base, queue)
		switch {
		case ctx.Err() != nil:
			return
		case err != nil:
			p.log.Error("worker: claim failed", "queue", queue, "err", err)
			if !sleepCtx(ctx, p.claimBackoff) {
				return
			}
			continue
		case full:
			continue
		}
		select {
		case <-ctx.Done():
			return
		case _, ok := <-wake:
			if !ok { // the dispatcher gave up; the poll floor carries the loop
				wake = nil
			}
		case <-floor.C:
		}
	}
}

// claimOnce holds one concurrency slot before asking the store, so a saturated
// pool never claims work it cannot start, and asks for at most the free slots.
// Every claimed job gets a slot (blocking if another queue's loop raced us: the
// lease is ours, the job must run) and a goroutine. full is true when the store
// returned as many jobs as asked.
func (p *Pool) claimOnce(ctx, base context.Context, queue string) (full bool, err error) {
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	free := cap(p.sem) - len(p.sem) // besides the slot we hold
	limit := min(p.cfg.BatchSize, 1+free)
	claimedAt := p.now() // before the round trip: the lease clock starts no earlier than this
	jobs, err := p.store.Claim(ctx, job.ClaimQuery{
		Queue:    queue,
		Owner:    p.cfg.Owner,
		Limit:    int32(limit), //nolint:gosec // limit <= BatchSize, a small positive int
		LeaseTTL: p.cfg.LeaseTTL,
	})
	if err != nil || len(jobs) == 0 {
		<-p.sem
		return false, err
	}
	for i := range jobs {
		if i > 0 {
			p.sem <- struct{}{}
		}
		p.wg.Add(1)
		go p.runJob(base, jobs[i], claimedAt)
	}
	return len(jobs) == limit, nil
}

// runJob owns one slot and one wg count; it releases both when it returns.
//
// A job whose lease the heartbeater fenced is never completed or failed: the
// store would refuse it anyway (token guard), and not asking keeps a stale
// worker from even trying to commit an effect. The reaper owns its fate.
func (p *Pool) runJob(base context.Context, j job.Job, claimedAt time.Time) {
	defer p.wg.Done()
	defer func() { <-p.sem }()
	p.st.claimed.Add(1)
	p.st.inFlight.Add(1)
	defer p.st.inFlight.Add(-1)

	log := p.log.With("job", j.ID, "queue", j.Queue, "type", j.Type, "attempt", j.Attempts)
	p.publish(j, job.StatusQueued, job.StatusLeased, "")

	jctx, cancel := context.WithTimeout(base, p.cfg.JobTimeout)
	defer cancel()
	lease := job.LeaseOf(j)
	p.hb.add(lease, cancel, claimedAt)
	defer p.hb.remove(lease)

	rctx, span := obs.StartRun(jctx, j)
	var spanErr error
	defer func() { obs.End(span, spanErr) }()

	started := p.now()
	commit, err := p.handle(rctx, j)
	elapsed := p.now().Sub(started)
	if p.cfg.Durations != nil {
		p.cfg.Durations.ObserveDuration(j.Queue, j.Type, elapsed)
	}

	state := p.hb.state(lease)
	if state == leaseFenced {
		spanErr = fmt.Errorf("%w: fenced by heartbeat, not completed", job.ErrLeaseLost)
		log.Warn("worker: lease fenced; result dropped, the reaper requeues the job", "took", elapsed, "handler_err", err)
		return
	}
	// Stop heartbeating before reporting: the store guards Complete/Fail by
	// token anyway, and a heartbeat racing the commit must not count the
	// finished lease as fenced.
	p.hb.remove(lease)

	sctx, scancel := context.WithTimeout(trace.ContextWithSpan(base, span), storeTimeout)
	defer scancel()

	if err == nil {
		cctx, cspan := obs.StartComplete(sctx, j)
		cerr := p.store.Complete(cctx, lease, p.cfg.Owner, commit)
		obs.End(cspan, cerr)
		if cerr != nil {
			spanErr = cerr
			p.storeError(log, "complete", cerr)
			return
		}
		p.st.succeeded.Add(1)
		p.publish(j, job.StatusLeased, job.StatusSucceeded, "")
		log.Info("worker: job succeeded", "took", elapsed)
		return
	}

	spanErr = err
	p.st.failed.Add(1)
	perm := errors.Is(err, job.ErrPermanent)
	cause := p.cause(jctx, state, err)
	// retryAt is app-clock based: Fail only stores it as run_at, and the claim
	// query compares run_at against the database clock (docs/PLAN.md, Risks).
	retryAt := p.now().Add(j.Backoff.Next(j.Attempts, nil))
	status, ferr := p.store.Fail(sctx, lease, p.cfg.Owner, cause, perm, retryAt)
	if ferr != nil {
		p.storeError(log, "fail", ferr)
		return
	}
	switch status { //nolint:exhaustive // the store only returns these three
	case job.StatusQueued:
		p.st.retried.Add(1)
	case job.StatusDead:
		p.st.dead.Add(1)
	case job.StatusCancelled:
		// cooperative cancel landed; nothing to count as ours
	}
	p.publish(j, job.StatusLeased, status, cause)
	log.Warn("worker: job failed", "status", status.String(), "permanent", perm, "took", elapsed, "err", err)
}

// handle dispatches to the registered handler. A type bound to both interfaces
// runs as a TxHandler. A panic becomes a transient error rather than taking the
// process down.
func (p *Pool) handle(ctx context.Context, j job.Job) (commit job.Commit, err error) {
	h, ok := p.reg.Lookup(j.Type)
	if !ok {
		return nil, fmt.Errorf("%w: %w %q", job.ErrPermanent, job.ErrNoHandler, j.Type)
	}
	defer func() {
		if r := recover(); r != nil {
			commit, err = nil, fmt.Errorf("handler panicked: %v", r)
		}
	}()
	switch h := h.(type) {
	case job.TxHandler:
		return h.HandleTx(ctx, j)
	case job.Handler:
		return nil, h.Handle(ctx, j)
	default:
		return nil, fmt.Errorf("%w: %T is neither Handler nor TxHandler", job.ErrPermanent, h)
	}
}

// cause names why the attempt failed for last_error / the event, bounded and
// cleaned by job.SanitizeError.
func (p *Pool) cause(jctx context.Context, state leaseState, err error) string {
	var s string
	switch {
	case state == leaseCancelRequested:
		s = "cancel requested: " + err.Error()
	case errors.Is(jctx.Err(), context.DeadlineExceeded):
		s = fmt.Sprintf("job timeout after %s: %v", p.cfg.JobTimeout, err)
	default:
		s = err.Error()
	}
	return job.SanitizeError(s)
}

func (p *Pool) storeError(log *slog.Logger, op string, err error) {
	if errors.Is(err, job.ErrLeaseLost) {
		// Reaped, stolen or cancelled under us: the store already decided the
		// job's fate and the effect (if any) rolled back. Nothing to retry.
		p.st.leaseLost.Add(1)
		log.Warn("worker: lease lost, "+op+" rejected", "err", err)
		return
	}
	// The job stays leased and the reaper requeues it once the lease expires.
	log.Error("worker: "+op+" failed, lease left to the reaper", "err", err)
}

func (p *Pool) publish(j job.Job, from, to job.Status, cause string) {
	p.pub.Publish(job.Event{
		JobID:   j.ID,
		Queue:   j.Queue,
		Type:    j.Type,
		From:    from,
		To:      to,
		Attempt: j.Attempts,
		At:      p.now(),
		Err:     cause,
	})
}

// sleepCtx pauses for d and reports false when ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
