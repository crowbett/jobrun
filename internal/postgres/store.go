package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/internal/postgres/sqlc"
)

// Defaults for the Store options.
const (
	// DefaultCommitTimeout is the statement_timeout of the completion
	// transaction: a handler's Commit closure is "writes only, no network".
	DefaultCommitTimeout = 5 * time.Second
	// DefaultPurgeLimit bounds one Purge call when PurgeQuery.Limit is 0.
	DefaultPurgeLimit = 1000
	// leaseExpiredCause is the last_error / event error written by ReapExpired.
	leaseExpiredCause = "lease expired"
)

// Store implements job.Store on a pgxpool.Pool. Every mutating method is one
// transaction (see the package doc for the exact boundaries).
type Store struct {
	pool          *pgxpool.Pool
	q             *sqlc.Queries
	log           *slog.Logger
	events        EventMode
	commitTimeout time.Duration
	channel       string
}

var _ job.Store = (*Store)(nil)

// Option configures a Store.
type Option func(*Store)

// WithEventMode selects which transitions are written to job_events (JOB_EVENTS).
func WithEventMode(m EventMode) Option { return func(s *Store) { s.events = m } }

// WithCommitTimeout sets the statement_timeout of the completion transaction;
// 0 disables it. The default is DefaultCommitTimeout.
func WithCommitTimeout(d time.Duration) Option { return func(s *Store) { s.commitTimeout = d } }

// WithNotifyChannel sets the LISTEN/NOTIFY channel the store signals from
// inside its enqueue transactions; "" disables the in-transaction notify
// (PollOnly deployments). The default is NotifyChannel.
func WithNotifyChannel(name string) Option { return func(s *Store) { s.channel = name } }

// NewStore returns a Store over pool.
func NewStore(pool *pgxpool.Pool, log *slog.Logger, opts ...Option) *Store {
	s := &Store{
		pool:          pool,
		q:             sqlc.New(pool),
		log:           log,
		events:        EventsAll,
		commitTimeout: DefaultCommitTimeout,
		channel:       NotifyChannel,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// withTx runs fn inside one transaction and commits when it returns nil. Any
// error rolls back (a rollback after commit is a harmless no-op).
func (s *Store) withTx(ctx context.Context, fn func(q *sqlc.Queries, tx pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(sqlc.New(tx), tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// notify signals the queue from inside the transaction; Postgres delivers it
// only when the transaction commits, so there is no notify-without-row window.
func (s *Store) notify(ctx context.Context, q *sqlc.Queries, queue string) error {
	if s.channel == "" {
		return nil
	}
	if err := q.NotifyReady(ctx, sqlc.NotifyReadyParams{Channel: s.channel, Queue: queue}); err != nil {
		return fmt.Errorf("notify %s: %w", queue, err)
	}
	return nil
}

// Now returns the database clock. Callers that pass "now" into Heartbeat,
// ReapExpired or DueSchedules read it here, never from time.Now.
func (s *Store) Now(ctx context.Context) (time.Time, error) {
	t, err := s.q.DBNow(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("db now: %w", err)
	}
	return t.UTC(), nil
}

// insertParams maps a domain job onto the insert row. A zero ID gets a fresh
// one; RunAt zero becomes now() + Delay on the database clock.
func insertParams(j job.Job) (sqlc.InsertJobParams, error) {
	backoff, err := policyToDB(j.Backoff)
	if err != nil {
		return sqlc.InsertJobParams{}, err
	}
	id := j.ID
	if id == uuid.Nil {
		id = job.NewID()
	}
	maxAttempts := j.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = job.DefaultMaxAttempts
	}
	p := sqlc.InsertJobParams{
		ID:             id,
		Queue:          j.Queue,
		Type:           j.Type,
		Payload:        payloadToDB(j.Payload),
		Priority:       j.Priority,
		Delay:          interval(j.Delay),
		MaxAttempts:    maxAttempts,
		Backoff:        backoff,
		IdempotencyKey: nullStr(j.IdempotencyKey),
	}
	if !j.RunAt.IsZero() {
		runAt := j.RunAt
		p.RunAt = &runAt
	}
	return p, nil
}

// Enqueue inserts j in one transaction: INSERT jobs ON CONFLICT (queue,
// idempotency_key) DO NOTHING, the enqueue event and the NOTIFY. No row from
// the insert means a replay: the stored job is returned unchanged (the caller
// sees stored.ID != j.ID); a replay with a different body is
// job.ErrIdempotencyConflict.
func (s *Store) Enqueue(ctx context.Context, j job.Job) (job.Job, error) {
	params, err := insertParams(j)
	if err != nil {
		return job.Job{}, err
	}
	var out job.Job
	err = s.withTx(ctx, func(q *sqlc.Queries, _ pgx.Tx) error {
		row, err := q.InsertJob(ctx, params)
		if errors.Is(err, pgx.ErrNoRows) {
			stored, err := q.GetJobByIdem(ctx, sqlc.GetJobByIdemParams{Queue: j.Queue, IdempotencyKey: j.IdempotencyKey})
			if err != nil {
				return mapNotFound(err, job.ErrNotFound, "re-read after idempotency conflict")
			}
			if out, err = toJob(&stored); err != nil {
				return err
			}
			if !sameBody(out, j) {
				return fmt.Errorf("%w: queue %q key %q", job.ErrIdempotencyConflict, j.Queue, j.IdempotencyKey)
			}
			return nil // replay: nothing written
		}
		if err != nil {
			return fmt.Errorf("insert job: %w", err)
		}
		if out, err = toJob(&row); err != nil {
			return err
		}
		if err := s.recordEvent(ctx, q, job.Event{
			JobID: out.ID, Queue: out.Queue, Type: out.Type,
			From: job.StatusUnspecified, To: job.StatusQueued, At: out.CreatedAt,
		}); err != nil {
			return err
		}
		return s.notify(ctx, q, out.Queue)
	})
	return out, err
}

// sameBody reports whether a replay carries the same job as the stored one:
// type, payload (compared as JSON documents, since jsonb normalises the text),
// priority, max_attempts and backoff.
func sameBody(stored, replay job.Job) bool {
	maxAttempts := replay.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = job.DefaultMaxAttempts
	}
	if stored.Type != replay.Type || stored.Priority != replay.Priority ||
		stored.MaxAttempts != maxAttempts || stored.Backoff != replay.Backoff.OrDefault() {
		return false
	}
	var a, b any
	if json.Unmarshal(payloadToDB(stored.Payload), &a) != nil || json.Unmarshal(payloadToDB(replay.Payload), &b) != nil {
		return false
	}
	return reflect.DeepEqual(a, b)
}

// EnqueueBatch inserts js with COPY in one transaction (no idempotency
// handling: a duplicate key fails the whole batch), writes the enqueue events
// and notifies each distinct queue once. Zero RunAt values resolve against the
// database clock read at the start of the transaction.
func (s *Store) EnqueueBatch(ctx context.Context, js []job.Job) (int64, error) {
	if len(js) == 0 {
		return 0, nil
	}
	var n int64
	err := s.withTx(ctx, func(q *sqlc.Queries, _ pgx.Tx) error {
		now, err := q.DBNow(ctx)
		if err != nil {
			return fmt.Errorf("db now: %w", err)
		}
		rows := make([]sqlc.InsertJobsCopyParams, 0, len(js))
		evs := make([]job.Event, 0, len(js))
		queues := make(map[string]struct{})
		for i := range js {
			p, err := insertParams(js[i])
			if err != nil {
				return fmt.Errorf("job %d: %w", i, err)
			}
			runAt := now.Add(js[i].Delay)
			if p.RunAt != nil {
				runAt = *p.RunAt
			}
			rows = append(rows, sqlc.InsertJobsCopyParams{
				ID: p.ID, Queue: p.Queue, Type: p.Type, Payload: p.Payload, Priority: p.Priority,
				RunAt: runAt, MaxAttempts: p.MaxAttempts, Backoff: p.Backoff, IdempotencyKey: p.IdempotencyKey,
			})
			evs = append(evs, job.Event{JobID: p.ID, Queue: p.Queue, Type: p.Type, To: job.StatusQueued, At: now})
			queues[p.Queue] = struct{}{}
		}
		if n, err = q.InsertJobsCopy(ctx, rows); err != nil {
			return fmt.Errorf("copy jobs: %w", err)
		}
		if err := s.recordEvents(ctx, q, evs); err != nil {
			return err
		}
		for queue := range queues {
			if err := s.notify(ctx, q, queue); err != nil {
				return err
			}
		}
		return nil
	})
	return n, err
}

// Claim leases up to q.Limit ready jobs on q.Queue to q.Owner in one round trip
// (FOR UPDATE SKIP LOCKED), increments attempts and records the claim events.
func (s *Store) Claim(ctx context.Context, q job.ClaimQuery) ([]job.Job, error) {
	if q.Limit <= 0 || q.Owner == "" || q.LeaseTTL <= 0 {
		return nil, fmt.Errorf("%w: claim needs a positive limit, an owner and a lease ttl", job.ErrInvalidArgument)
	}
	params := sqlc.ClaimJobsParams{QueueName: q.Queue, Owner: q.Owner, BatchLimit: q.Limit, Ttl: interval(q.LeaseTTL)}
	if !s.events.records(job.StatusLeased) {
		rows, err := s.q.ClaimJobs(ctx, params)
		if err != nil {
			return nil, fmt.Errorf("claim jobs: %w", err)
		}
		return toJobs(rows)
	}
	var out []job.Job
	err := s.withTx(ctx, func(qq *sqlc.Queries, _ pgx.Tx) error {
		rows, err := qq.ClaimJobs(ctx, params)
		if err != nil {
			return fmt.Errorf("claim jobs: %w", err)
		}
		if out, err = toJobs(rows); err != nil {
			return err
		}
		evs := make([]job.Event, 0, len(out))
		for i := range out {
			j := &out[i]
			evs = append(evs, job.Event{
				JobID: j.ID, Queue: j.Queue, Type: j.Type,
				From: job.StatusQueued, To: job.StatusLeased, Attempt: j.Attempts, At: j.UpdatedAt,
			})
		}
		return s.recordEvents(ctx, qq, evs)
	})
	return out, err
}

// Heartbeat extends the unexpired leases of ids owned by owner to until and
// returns the ids kept whose cancel is not requested. Missing ids mean the
// lease expired, was reaped or stolen, or the job was cancelled: the worker
// cancels those contexts.
func (s *Store) Heartbeat(ctx context.Context, owner string, ids []job.ID, until time.Time) ([]job.ID, error) {
	if len(ids) == 0 {
		return []job.ID{}, nil
	}
	rows, err := s.q.HeartbeatLeases(ctx, sqlc.HeartbeatLeasesParams{Until: until, Ids: ids, Owner: owner})
	if err != nil {
		return nil, fmt.Errorf("heartbeat leases: %w", err)
	}
	kept := make([]job.ID, 0, len(rows))
	for _, r := range rows {
		if !r.CancelRequested {
			kept = append(kept, r.ID)
		}
	}
	return kept, nil
}

// txExecer is the slice of the completion transaction handed to a Commit closure.
type txExecer struct{ tx pgx.Tx }

func (e txExecer) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	tag, err := e.tx.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// Complete finishes a leased job in one transaction: lock the row and check
// the owner (job.ErrLeaseLost otherwise), insert the processed_effects row, run
// fn when the effect is new, flip the job to succeeded (or cancelled when a
// cancel was requested meanwhile) and record the event. Zero rows on the
// owner-guarded update rolls everything back, fn's writes included.
func (s *Store) Complete(ctx context.Context, id job.ID, owner string, fn job.Commit) error {
	return s.withTx(ctx, func(q *sqlc.Queries, tx pgx.Tx) error {
		if s.commitTimeout > 0 {
			ms := strconv.FormatInt(s.commitTimeout.Milliseconds(), 10)
			if _, err := tx.Exec(ctx, "SELECT set_config('statement_timeout', $1, true)", ms); err != nil {
				return fmt.Errorf("set statement_timeout: %w", err)
			}
		}
		row, err := q.GetJobForUpdate(ctx, id)
		if err != nil {
			return mapNotFound(err, job.ErrNotFound, "lock job")
		}
		cur, err := toJob(&row)
		if err != nil {
			return err
		}
		if cur.Status != job.StatusLeased || cur.LeaseOwner != owner {
			return fmt.Errorf("%w: job %s is %s (owner %q)", job.ErrLeaseLost, id, cur.Status, cur.LeaseOwner)
		}
		fresh, err := markEffect(ctx, q, cur)
		if err != nil {
			return err
		}
		if fresh && fn != nil {
			if err := fn(ctx, txExecer{tx: tx}); err != nil {
				return fmt.Errorf("commit effect: %w", err)
			}
		}
		done, err := q.CompleteJob(ctx, sqlc.CompleteJobParams{ID: id, Owner: owner})
		if err != nil {
			return mapNotFound(err, job.ErrLeaseLost, "complete job")
		}
		to, err := statusFromDB(done.Status)
		if err != nil {
			return err
		}
		return s.recordEvent(ctx, q, job.Event{
			JobID: id, Queue: done.Queue, Type: done.Type,
			From: job.StatusLeased, To: to, Attempt: done.Attempts, At: done.UpdatedAt,
		})
	})
}

// Fail records cause and moves the job to queued at retryAt, to dead when perm
// is set or attempts reached max_attempts, or to cancelled when a cancel was
// requested; it returns the resulting status. Owner-guarded (job.ErrLeaseLost).
func (s *Store) Fail(ctx context.Context, id job.ID, owner, cause string, perm bool, retryAt time.Time) (job.Status, error) {
	var to job.Status
	err := s.withTx(ctx, func(q *sqlc.Queries, _ pgx.Tx) error {
		row, err := q.FailJob(ctx, sqlc.FailJobParams{
			ID: id, Owner: owner, Permanent: perm, RetryAt: retryAt, Cause: nullStr(truncateErr(cause)),
		})
		if err != nil {
			return mapNotFound(err, job.ErrLeaseLost, "fail job")
		}
		if to, err = statusFromDB(row.Status); err != nil {
			return err
		}
		return s.recordEvent(ctx, q, job.Event{
			JobID: id, Queue: row.Queue, Type: row.Type,
			From: job.StatusLeased, To: to, Attempt: row.Attempts, At: row.UpdatedAt, Err: truncateErr(cause),
		})
	})
	return to, err
}

// ReapExpired requeues up to limit leases expired at now with a backoff from
// the job's own policy (p when the stored one is unreadable), marks them dead
// at max_attempts or cancelled when a cancel was requested, and returns the
// transitions it made. One transaction: SELECT ... FOR UPDATE SKIP LOCKED,
// one batched UPDATE, the events.
func (s *Store) ReapExpired(ctx context.Context, now time.Time, limit int32, p job.Policy) ([]job.Event, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("%w: reap limit must be positive", job.ErrInvalidArgument)
	}
	evs := []job.Event{}
	err := s.withTx(ctx, func(q *sqlc.Queries, _ pgx.Tx) error {
		rows, err := q.SelectExpiredLeases(ctx, sqlc.SelectExpiredLeasesParams{Now: now, BatchLimit: limit})
		if err != nil {
			return fmt.Errorf("select expired leases: %w", err)
		}
		if len(rows) == 0 {
			return nil
		}
		updates := make([]sqlc.RequeueExpiredJobParams, 0, len(rows))
		for i := range rows {
			r := &rows[i]
			to, runAt := job.StatusQueued, now
			switch {
			case r.CancelRequested:
				to = job.StatusCancelled
			case r.Attempts >= r.MaxAttempts:
				to = job.StatusDead
			default:
				pol, perr := policyFromDB(r.Backoff)
				if perr != nil {
					s.log.WarnContext(ctx, "unreadable backoff policy, using fallback",
						slog.String("job_id", r.ID.String()), slog.String("err", perr.Error()))
					pol = p
				}
				runAt = now.Add(pol.Next(r.Attempts, nil))
			}
			updates = append(updates, sqlc.RequeueExpiredJobParams{
				ID: r.ID, Status: to.String(), RunAt: runAt, Cause: leaseExpiredCause,
			})
			evs = append(evs, job.Event{
				JobID: r.ID, Queue: r.Queue, Type: r.Type,
				From: job.StatusLeased, To: to, Attempt: r.Attempts, At: now, Err: leaseExpiredCause,
			})
		}
		var first error
		q.RequeueExpiredJob(ctx, updates).Exec(func(i int, err error) {
			if err != nil && first == nil {
				first = fmt.Errorf("requeue expired %s: %w", updates[i].ID, err)
			}
		})
		if first != nil {
			return first
		}
		return s.recordEvents(ctx, q, evs)
	})
	if err != nil {
		return nil, err
	}
	return evs, nil
}

// Get returns one job or job.ErrNotFound.
func (s *Store) Get(ctx context.Context, id job.ID) (job.Job, error) {
	row, err := s.q.GetJob(ctx, id)
	if err != nil {
		return job.Job{}, mapNotFound(err, job.ErrNotFound, "get job")
	}
	return toJob(&row)
}

// List pages newest first over the (created_at, id) keyset. Limit 0 means
// job.DefaultListLimit and anything above job.MaxListLimit is clamped; the
// returned cursor is zero when there is no next page.
func (s *Store) List(ctx context.Context, q job.ListQuery) ([]job.Job, job.Cursor, error) {
	limit := q.Limit
	switch {
	case limit <= 0:
		limit = job.DefaultListLimit
	case limit > job.MaxListLimit:
		limit = job.MaxListLimit
	}
	params := sqlc.ListJobsParams{Queue: nullStr(q.Queue), Type: nullStr(q.Type), PageLimit: limit + 1}
	if q.Status != nil {
		st := sqlc.JobStatus(q.Status.String())
		params.Status = &st
	}
	if !q.Cursor.IsZero() {
		after := q.Cursor.CreatedAt
		params.AfterCreatedAt = &after
		params.AfterID = uuid.NullUUID{UUID: q.Cursor.ID, Valid: true}
	}
	rows, err := s.q.ListJobs(ctx, params)
	if err != nil {
		return nil, job.Cursor{}, fmt.Errorf("list jobs: %w", err)
	}
	var next job.Cursor
	if len(rows) > int(limit) {
		rows = rows[:limit]
		last := rows[len(rows)-1]
		next = job.Cursor{CreatedAt: last.CreatedAt.UTC(), ID: last.ID}
	}
	out, err := toJobs(rows)
	if err != nil {
		return nil, job.Cursor{}, err
	}
	return out, next, nil
}

// Cancel moves a queued job to cancelled at once, sets cancel_requested on a
// leased one (returned still leased; the worker gives it up on its next
// heartbeat), and returns job.ErrNotCancellable for a finished job.
func (s *Store) Cancel(ctx context.Context, id job.ID) (job.Job, error) {
	var out job.Job
	err := s.withTx(ctx, func(q *sqlc.Queries, _ pgx.Tx) error {
		cur, err := q.GetJobForUpdate(ctx, id)
		if err != nil {
			return mapNotFound(err, job.ErrNotFound, "lock job")
		}
		var row sqlc.Job
		switch cur.Status {
		case sqlc.JobStatusQueued:
			if row, err = q.CancelQueuedJob(ctx, id); err != nil {
				return fmt.Errorf("cancel queued job: %w", err)
			}
			if err := s.recordEvent(ctx, q, job.Event{
				JobID: id, Queue: row.Queue, Type: row.Type,
				From: job.StatusQueued, To: job.StatusCancelled, Attempt: row.Attempts, At: row.UpdatedAt,
			}); err != nil {
				return err
			}
		case sqlc.JobStatusLeased:
			if row, err = q.RequestCancel(ctx, id); err != nil {
				return fmt.Errorf("request cancel: %w", err)
			}
		case sqlc.JobStatusSucceeded, sqlc.JobStatusFailed, sqlc.JobStatusDead, sqlc.JobStatusCancelled:
			return fmt.Errorf("%w: job %s is %s", job.ErrNotCancellable, id, cur.Status)
		default:
			return fmt.Errorf("%w: job %s has unknown status %q", job.ErrNotCancellable, id, cur.Status)
		}
		out, err = toJob(&row)
		return err
	})
	return out, err
}

// Retry moves a dead, cancelled or failed job back to queued with attempts=0
// and run_at=now(), records the event and notifies the queue; anything else is
// job.ErrNotRetryable.
func (s *Store) Retry(ctx context.Context, id job.ID) (job.Job, error) {
	var out job.Job
	err := s.withTx(ctx, func(q *sqlc.Queries, _ pgx.Tx) error {
		cur, err := q.GetJobForUpdate(ctx, id)
		if err != nil {
			return mapNotFound(err, job.ErrNotFound, "lock job")
		}
		from, err := statusFromDB(cur.Status)
		if err != nil {
			return err
		}
		if !from.CanTransition(job.StatusQueued) || from == job.StatusUnspecified {
			return fmt.Errorf("%w: job %s is %s", job.ErrNotRetryable, id, from)
		}
		row, err := q.RetryJob(ctx, id)
		if err != nil {
			return mapNotFound(err, job.ErrNotRetryable, "retry job")
		}
		if out, err = toJob(&row); err != nil {
			return err
		}
		if err := s.recordEvent(ctx, q, job.Event{
			JobID: id, Queue: out.Queue, Type: out.Type,
			From: from, To: job.StatusQueued, At: out.UpdatedAt,
		}); err != nil {
			return err
		}
		return s.notify(ctx, q, out.Queue)
	})
	return out, err
}

// Purge deletes finished jobs (and their events) matching q, oldest first,
// and returns the count. Empty Statuses means succeeded, cancelled and dead;
// OlderThan is measured from finished_at on the database clock.
func (s *Store) Purge(ctx context.Context, q job.PurgeQuery) (int64, error) {
	if err := q.Validate(); err != nil {
		return 0, err
	}
	statuses := make([]string, 0, len(q.Statuses))
	for _, st := range q.Statuses {
		statuses = append(statuses, st.String())
	}
	limit := q.Limit
	if limit <= 0 {
		limit = DefaultPurgeLimit
	}
	n, err := s.q.PurgeJobs(ctx, sqlc.PurgeJobsParams{
		Statuses: statuses, Queue: nullStr(q.Queue), OlderThan: interval(q.OlderThan), BatchLimit: limit,
	})
	if err != nil {
		return 0, fmt.Errorf("purge jobs: %w", err)
	}
	return n, nil
}

// Depth returns the (queue, status) counts with the oldest run_at per cell. It
// scans every non-purged row: admin caches it on a ticker, never per scrape.
func (s *Store) Depth(ctx context.Context) ([]job.DepthRow, error) {
	rows, err := s.q.QueueDepth(ctx)
	if err != nil {
		return nil, fmt.Errorf("queue depth: %w", err)
	}
	out := make([]job.DepthRow, 0, len(rows))
	for _, r := range rows {
		st, err := statusFromDB(r.Status)
		if err != nil {
			return nil, err
		}
		out = append(out, job.DepthRow{Queue: r.Queue, Status: st, N: r.N, OldestRunAt: r.OldestRunAt.UTC()})
	}
	return out, nil
}
