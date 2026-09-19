// Package postgres is the only package in jobrun that touches SQL. It
// implements the ports the rest of the system consumes (job.Store with its
// embedded job.Schedules, job.Dispatcher, and the scheduler's Locker) on top of
// a pgxpool.Pool and the sqlc-generated queries in sqlc/ (GENERATED from
// db/query/*.sql against migrations/; never hand-edited).
//
// # Files
//
//   - pool.go: Open / OpenConfig, a pgxpool with the production limits
//     (MaxConns = WORKER_CONCURRENCY + 8), pinged.
//   - migrate.go: Migrate, goose migrations embedded from migrations/ and run
//     through a *sql.DB borrowed from the pool. Idempotent.
//   - store.go: Store (job.Store) - Enqueue, EnqueueBatch, Claim, Heartbeat,
//     Complete, Fail, ReapExpired, Get, List, Cancel, Retry, Purge, Depth, Now,
//     plus withTx and the Options (WithEventMode, WithCommitTimeout,
//     WithNotifyChannel).
//   - schedules.go: the job.Schedules half of the Store.
//   - events.go: EventMode (JOB_EVENTS=all|terminal), the job_events writers
//     used inside every transition, Events and PruneEvents.
//   - effects.go: EffectKey, EffectDone and the processed_effects ledger row
//     written by Complete.
//   - notify.go: Notifier (job.Dispatcher) over LISTEN/NOTIFY with one
//     dedicated listener connection, reconnect and a 1-slot channel per waiter.
//   - lock.go: Locker and AdvisoryLocker (pg_try_advisory_lock on a dedicated
//     connection) for scheduler leader election.
//   - convert.go: row <-> domain conversion and the small pg helpers.
//
// # Transaction boundaries
//
// A transaction opens only inside this package, always through Store.withTx,
// and every mutating Store method is exactly one transaction:
//
//   - Enqueue: INSERT jobs ON CONFLICT DO NOTHING (+ re-read on a replay) +
//     INSERT job_events + pg_notify. Postgres delivers the notification only at
//     commit, so a consumer can never wake before the row is visible.
//   - EnqueueBatch: SELECT now() + COPY jobs + batched INSERT job_events + one
//     pg_notify per distinct queue.
//   - Claim: the CTE claim statement (FOR UPDATE SKIP LOCKED, attempts++) +
//     batched INSERT job_events. Without claim events (EventsTerminal) it is
//     the single statement, no explicit transaction.
//   - Complete: set_config(statement_timeout) + SELECT jobs FOR UPDATE (owner
//     check, job.ErrLeaseLost) + INSERT processed_effects + the handler's
//     Commit closure (skipped when the effect row already existed) + the
//     owner-guarded UPDATE + INSERT job_events. Any failure, including a lost
//     lease, rolls the handler's writes back with everything else: that is the
//     exactly-once story for handlers on the tx path.
//   - Fail: the owner-guarded UPDATE (queued at retry_at, dead, or cancelled
//     when a cancel was requested) + INSERT job_events.
//   - ReapExpired: SELECT expired leases FOR UPDATE SKIP LOCKED + one pgx
//     batch of per-row UPDATEs (verdict and backoff computed in Go from the
//     job's own policy) + batched INSERT job_events.
//   - Cancel / Retry: SELECT jobs FOR UPDATE + the UPDATE + INSERT job_events
//     (+ pg_notify for Retry, which makes the job claimable now).
//   - DueSchedules: SELECT ... FOR UPDATE SKIP LOCKED in its own short
//     transaction (defensive against two leaders; the sched:<id>:<next_run>
//     idempotency key on the materialised job is the durable guard).
//
// Heartbeat, Purge, Depth, Get, List, Events and the remaining schedule
// methods are single statements on the pool.
//
// # The clock rule
//
// Every comparison and default uses the database clock: run_at defaults to
// now() + delay, claims read run_at <= now(), leases are now() + ttl, Fail and
// ReapExpired stamp finished_at = now(), Purge measures age from now(). The
// time.Time arguments the ports carry (Heartbeat until, ReapExpired now,
// DueSchedules now, Fail retryAt) are "db now as read by the caller" - use
// Store.Now, never time.Now - so an app clock that drifts can never hide a job
// or fire it early. Timestamps come back normalised to UTC.
//
// # Leases and fencing
//
// A claim flips queued -> leased and increments attempts, so a process that
// crashes mid-job burns exactly one attempt. Heartbeat extends only leases
// that have not yet expired and omits ids whose cancel was requested; an id
// missing from the kept set means the worker must cancel that job's context.
// Complete and Fail are owner-guarded: after the reaper requeued the job (or
// another owner claimed it) they return job.ErrLeaseLost and change nothing.
// A job with cancel_requested ends cancelled whichever of Complete or Fail
// arrives; the event is recorded either way.
//
// # Idempotency
//
// jobs_idem_uniq is a partial unique index on (queue, idempotency_key). Enqueue
// uses it as the ON CONFLICT target: a replay with the same type, payload
// (compared as JSON), priority, max_attempts and backoff returns the stored job
// unchanged (stored.ID != j.ID tells the caller), a different body is
// job.ErrIdempotencyConflict. processed_effects is keyed (scope, key) =
// ("job:"+type, idempotency_key or id), so a Retry of a job whose effect
// already committed completes without running the Commit closure again.
package postgres
