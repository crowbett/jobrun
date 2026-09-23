-- Every time comparison and default below uses the database clock (now()), never a
-- value computed by the application; see internal/postgres/doc.go "The clock rule".

-- name: InsertJob :one
-- run_at: an absolute client-supplied time, or NULL for now() + delay on the DB clock.
-- The partial unique index jobs_idem_uniq makes the ON CONFLICT target; a NULL key
-- never conflicts. No row returned means the (queue, idempotency_key) already exists.
INSERT INTO jobs (id, queue, type, payload, priority, run_at, max_attempts, backoff, idempotency_key, traceparent)
VALUES (
    sqlc.arg('id'), sqlc.arg('queue'), sqlc.arg('type'), sqlc.arg('payload'), sqlc.arg('priority'),
    COALESCE(sqlc.narg('run_at')::timestamptz, now() + sqlc.arg('delay')::interval),
    sqlc.arg('max_attempts'), sqlc.arg('backoff'), sqlc.narg('idempotency_key'), sqlc.narg('traceparent')
)
ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING *;

-- name: InsertJobsCopy :copyfrom
INSERT INTO jobs (id, queue, type, payload, priority, run_at, max_attempts, backoff, idempotency_key, traceparent)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);

-- name: GetJob :one
SELECT * FROM jobs WHERE id = $1;

-- name: GetJobForUpdate :one
SELECT * FROM jobs WHERE id = $1 FOR UPDATE;

-- name: GetJobByIdem :one
SELECT * FROM jobs WHERE queue = sqlc.arg('queue') AND idempotency_key = sqlc.arg('idempotency_key')::text;

-- name: ClaimJobs :many
-- One round trip = batch claim + lease + attempts++. The CTE takes the row locks
-- (SKIP LOCKED, so concurrent claimers never wait on each other) and the UPDATE
-- flips them to leased. started_at is the first claim; attempts counts starts;
-- lease_token is bumped so every claim carries a fresh fencing token.
WITH ready AS (
    SELECT jobs.id FROM jobs
     WHERE jobs.status = 'queued' AND jobs.queue = sqlc.arg('queue_name') AND jobs.run_at <= now()
     ORDER BY jobs.priority DESC, jobs.run_at, jobs.id
     LIMIT sqlc.arg('batch_limit')
       FOR UPDATE SKIP LOCKED
)
UPDATE jobs AS j
   SET status = 'leased',
       lease_owner = sqlc.arg('owner')::text,
       lease_until = now() + sqlc.arg('ttl')::interval,
       attempts = j.attempts + 1,
       lease_token = j.lease_token + 1,
       started_at = COALESCE(j.started_at, now()),
       updated_at = now()
  FROM ready
 WHERE j.id = ready.id
RETURNING j.*;

-- name: HeartbeatLeases :many
-- Returns the leases actually extended: (id, token) pairs zipped by the parallel
-- unnest, guarded by owner and token. A lease that already expired is not revived
-- (fencing: once lease_until has passed the reaper may take the job at any moment),
-- and cancel_requested is returned so the store can report cancelled ids apart.
UPDATE jobs AS j
   SET lease_until = sqlc.arg('until')::timestamptz, updated_at = now()
  FROM (SELECT unnest(sqlc.arg('ids')::uuid[]) AS id, unnest(sqlc.arg('tokens')::bigint[]) AS token) AS l
 WHERE j.id = l.id
   AND j.lease_token = l.token
   AND j.status = 'leased'
   AND j.lease_owner = sqlc.arg('owner')::text
   AND j.lease_until > now()
RETURNING j.id, j.lease_token, j.cancel_requested;

-- name: CompleteJob :one
-- Owner- and token-guarded so a stale worker cannot resurrect a reaped job. A job
-- whose cancel was requested while it ran ends cancelled, not succeeded. No row =
-- ErrLeaseLost.
UPDATE jobs
   SET status = (CASE WHEN cancel_requested THEN 'cancelled' ELSE 'succeeded' END)::job_status,
       lease_owner = NULL, lease_until = NULL, last_error = NULL,
       finished_at = now(), updated_at = now()
 WHERE id = sqlc.arg('id') AND status = 'leased' AND lease_owner = sqlc.arg('owner')::text
   AND lease_token = sqlc.arg('token')::bigint
RETURNING *;

-- name: FailJob :one
-- retry_at is computed in Go from job.Policy (the single source of backoff truth).
-- cancelled wins over dead wins over queued; a retry is queued with a future run_at.
UPDATE jobs
   SET status = (CASE
                   WHEN cancel_requested THEN 'cancelled'
                   WHEN sqlc.arg('permanent')::bool OR attempts >= max_attempts THEN 'dead'
                   ELSE 'queued'
                 END)::job_status,
       run_at = CASE
                  WHEN cancel_requested OR sqlc.arg('permanent')::bool OR attempts >= max_attempts THEN run_at
                  ELSE sqlc.arg('retry_at')::timestamptz
                END,
       finished_at = CASE
                       WHEN cancel_requested OR sqlc.arg('permanent')::bool OR attempts >= max_attempts THEN now()
                       ELSE NULL
                     END,
       lease_owner = NULL, lease_until = NULL,
       last_error = sqlc.narg('cause'),
       updated_at = now()
 WHERE id = sqlc.arg('id') AND status = 'leased' AND lease_owner = sqlc.arg('owner')::text
   AND lease_token = sqlc.arg('token')::bigint
RETURNING *;

-- name: SelectExpiredLeases :many
-- Reaper step 1 (inside the reaper transaction). Step 2 is RequeueExpired with the
-- per-id verdict computed in Go so the backoff policy has exactly one implementation.
SELECT id, queue, type, attempts, max_attempts, backoff, cancel_requested
  FROM jobs
 WHERE status = 'leased' AND lease_until < sqlc.arg('now')::timestamptz
 ORDER BY lease_until
 LIMIT sqlc.arg('batch_limit')
   FOR UPDATE SKIP LOCKED;

-- name: RequeueExpiredJob :batchexec
-- Reaper step 2, one row per expired lease, sent as one pgx batch inside the reaper
-- transaction. run_at is only applied to rows going back to queued.
UPDATE jobs
   SET status = sqlc.arg('status')::text::job_status,
       run_at = CASE WHEN sqlc.arg('status')::text = 'queued' THEN sqlc.arg('run_at')::timestamptz ELSE run_at END,
       finished_at = CASE WHEN sqlc.arg('status')::text = 'queued' THEN NULL ELSE now() END,
       lease_owner = NULL, lease_until = NULL,
       last_error = sqlc.arg('cause')::text,
       updated_at = now()
 WHERE id = sqlc.arg('id');

-- name: ListJobs :many
-- Keyset page over (created_at, id) newest first; every filter is optional. The page
-- limit is passed as limit+1 by the store to detect the next page.
SELECT * FROM jobs
 WHERE (sqlc.narg('queue')::text IS NULL OR queue = sqlc.narg('queue')::text)
   AND (sqlc.narg('status')::job_status IS NULL OR status = sqlc.narg('status')::job_status)
   AND (sqlc.narg('type')::text IS NULL OR type = sqlc.narg('type')::text)
   AND (sqlc.narg('after_created_at')::timestamptz IS NULL
        OR (created_at, id) < (sqlc.narg('after_created_at')::timestamptz, sqlc.narg('after_id')::uuid))
 ORDER BY created_at DESC, id DESC
 LIMIT sqlc.arg('page_limit');

-- name: CancelQueuedJob :one
UPDATE jobs
   SET status = 'cancelled', finished_at = now(), updated_at = now()
 WHERE id = $1 AND status = 'queued'
RETURNING *;

-- name: RequestCancel :one
UPDATE jobs
   SET cancel_requested = true, updated_at = now()
 WHERE id = $1 AND status = 'leased'
RETURNING *;

-- name: RetryJob :one
UPDATE jobs
   SET status = 'queued', attempts = 0, run_at = now(),
       lease_owner = NULL, lease_until = NULL, cancel_requested = false,
       last_error = NULL, finished_at = NULL, updated_at = now()
 WHERE id = $1 AND status IN ('dead', 'cancelled', 'failed')
RETURNING *;

-- name: PurgeJobs :execrows
-- Deletes finished jobs (and their events) matching queue/statuses/age, oldest first.
WITH victims AS (
    SELECT id FROM jobs
     WHERE status = ANY(sqlc.arg('statuses')::text[]::job_status[])
       AND (sqlc.narg('queue')::text IS NULL OR queue = sqlc.narg('queue')::text)
       AND finished_at IS NOT NULL
       AND finished_at < now() - sqlc.arg('older_than')::interval
     ORDER BY finished_at
     LIMIT sqlc.arg('batch_limit')
), events AS (
    DELETE FROM job_events WHERE job_id IN (SELECT id FROM victims)
)
DELETE FROM jobs WHERE id IN (SELECT id FROM victims);

-- name: QueueDepth :many
SELECT queue, status, count(*)::bigint AS n, min(run_at)::timestamptz AS oldest_run_at
  FROM jobs
 GROUP BY queue, status
 ORDER BY queue, status;

-- name: DBNow :one
SELECT now()::timestamptz AS now;

-- name: NotifyReady :exec
SELECT pg_notify(sqlc.arg('channel'), sqlc.arg('queue'));
