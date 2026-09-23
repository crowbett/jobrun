package job

import (
	"context"
	"time"
)

// Execer is the slice of a transaction a Commit closure may use.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (int64, error)
}

// Commit is a handler's durable effect, run inside the completion transaction
// (writes only, no network; the transaction has a statement_timeout). nil means
// the handler has no durable effect.
type Commit func(ctx context.Context, ex Execer) error

// Store is the persistence port consumed by worker, scheduler, reaper and server;
// internal/postgres is the only implementation. Every time comparison uses the
// database clock; the time.Time arguments below are "db now" as read by the
// caller in the same tick, never the app clock.
type Store interface {
	// Enqueue inserts j (ON CONFLICT (queue, idempotency_key) DO NOTHING) and
	// writes the enqueue event. A replay returns the stored job, so the caller
	// detects it with stored.ID != j.ID; a replay with a different body returns
	// ErrIdempotencyConflict.
	Enqueue(ctx context.Context, j Job) (Job, error)
	// EnqueueBatch inserts many jobs with pgx CopyFrom (bench + fan-out); no
	// idempotency handling, returns the rows written.
	EnqueueBatch(ctx context.Context, js []Job) (int64, error)
	// Claim leases up to q.Limit ready jobs on q.Queue to q.Owner in one round
	// trip (FOR UPDATE SKIP LOCKED) and increments attempts.
	Claim(ctx context.Context, q ClaimQuery) ([]Job, error)
	// Heartbeat extends the unexpired leases held by owner with a matching
	// fencing token to until and says which were kept and which had a cancel
	// requested; a lease in neither list was lost and the worker must fence it
	// (cancel the handler, never Complete/Fail).
	Heartbeat(ctx context.Context, owner string, leases []Lease, until time.Time) (Renewal, error)
	// Complete runs fn (may be nil) and the owner- and token-guarded status
	// update in one transaction; a lease no longer held rolls fn back and
	// returns ErrLeaseLost. A Complete after a cooperative cancel is recorded as
	// an event but the status stays cancelled.
	Complete(ctx context.Context, l Lease, owner string, fn Commit) error
	// Fail records cause and moves the job to queued at retryAt, or to dead when
	// perm is set or attempts >= max_attempts; returns the resulting status.
	// Owner- and token-guarded like Complete (ErrLeaseLost).
	Fail(ctx context.Context, l Lease, owner, cause string, perm bool, retryAt time.Time) (Status, error)
	// ReapExpired requeues up to limit leases expired at now with a backoff from
	// the job's own policy (p is the fallback), or marks them dead at
	// max_attempts, and returns the transitions it made.
	ReapExpired(ctx context.Context, now time.Time, limit int32, p Policy) ([]Event, error)
	// Get returns one job or ErrNotFound.
	Get(ctx context.Context, id ID) (Job, error)
	// Events returns a job's history oldest first.
	Events(ctx context.Context, id ID) ([]Event, error)
	// List pages newest first; the returned cursor is zero when there is no next page.
	List(ctx context.Context, q ListQuery) ([]Job, Cursor, error)
	// Cancel: queued -> cancelled at once; leased -> cancel_requested (the job
	// is returned as still leased); terminal -> ErrNotCancellable.
	Cancel(ctx context.Context, id ID) (Job, error)
	// Retry: dead|cancelled|failed -> queued with attempts=0, run_at=now();
	// anything else -> ErrNotRetryable.
	Retry(ctx context.Context, id ID) (Job, error)
	// Purge deletes matching finished jobs (and their events) and returns the count.
	Purge(ctx context.Context, q PurgeQuery) (int64, error)
	// Depth returns the (queue, status) counts. It is a full scan of the
	// non-purged rows: admin caches it on a ticker, never per scrape.
	Depth(ctx context.Context) ([]DepthRow, error)
	Schedules
}

// Schedules is the schedule half of the Store.
type Schedules interface {
	// CreateSchedule inserts s (NextRun must be set by the caller from cronx);
	// a taken name returns ErrScheduleExists.
	CreateSchedule(ctx context.Context, s Schedule) (Schedule, error)
	// UpdateSchedule replaces cron, timezone, template, enabled and next_run of
	// the schedule with s.ID; ErrScheduleNotFound when missing.
	UpdateSchedule(ctx context.Context, s Schedule) (Schedule, error)
	// DeleteSchedule removes a schedule; ErrScheduleNotFound when missing.
	DeleteSchedule(ctx context.Context, id ID) error
	// GetSchedule returns one schedule or ErrScheduleNotFound.
	GetSchedule(ctx context.Context, id ID) (Schedule, error)
	// ListSchedules pages by name ascending: rows with name > afterName, up to limit.
	ListSchedules(ctx context.Context, afterName string, limit int32) ([]Schedule, error)
	// DueSchedules returns up to limit enabled schedules with next_run <= now,
	// oldest next_run first.
	DueSchedules(ctx context.Context, now time.Time, limit int32) ([]Schedule, error)
	// AdvanceSchedule sets last_run and next_run after a materialisation.
	AdvanceSchedule(ctx context.Context, id ID, lastRun, nextRun time.Time) error
}

// Dispatcher is the wake-up seam between producers and the claim loops. v1 has
// postgres.Notifier (LISTEN/NOTIFY over a poll floor) and PollOnly; a NATS
// dispatcher for remote workers is the follow-up that keeps the worker untouched.
type Dispatcher interface {
	// Ready tells consumers that work exists on queue. Implementations must be
	// cheap and coalescing; losing a Ready is safe because Wait always also ticks.
	Ready(ctx context.Context, queue string) error
	// Wait is called once per claim loop. The returned channel receives a value
	// whenever queue may have work (coalesced: one pending signal at most) and at
	// least every poll floor; it is closed when ctx is done.
	Wait(ctx context.Context, queue string) <-chan struct{}
}

// PollOnly is a Dispatcher that only ticks (no notifications). It is the
// fallback when the notifier is down and the dispatcher used by unit tests.
type PollOnly struct {
	// Every is the tick interval; 0 means 200ms.
	Every time.Duration
}

// Ready is a no-op: the ticker covers it.
func (PollOnly) Ready(context.Context, string) error { return nil }

// Wait returns a channel that ticks every p.Every until ctx is done, then closes.
func (p PollOnly) Wait(ctx context.Context, _ string) <-chan struct{} {
	every := p.Every
	if every <= 0 {
		every = 200 * time.Millisecond
	}
	ch := make(chan struct{}, 1)
	go func() {
		defer close(ch)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				select {
				case ch <- struct{}{}:
				default: // a signal is already pending; coalesce
				}
			}
		}
	}()
	return ch
}
