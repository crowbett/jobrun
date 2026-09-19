-- name: InsertSchedule :one
INSERT INTO schedules (id, name, cron, timezone, queue, job_type, payload, priority, max_attempts, backoff, enabled, next_run)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
RETURNING *;

-- name: UpdateSchedule :one
UPDATE schedules
   SET cron = $2, timezone = $3, queue = $4, job_type = $5, payload = $6, priority = $7,
       max_attempts = $8, backoff = $9, enabled = $10, next_run = $11, updated_at = now()
 WHERE id = $1
RETURNING *;

-- name: DeleteSchedule :execrows
DELETE FROM schedules WHERE id = $1;

-- name: GetSchedule :one
SELECT * FROM schedules WHERE id = $1;

-- name: ListSchedules :many
SELECT * FROM schedules WHERE name > sqlc.arg('after_name') ORDER BY name LIMIT sqlc.arg('page_limit');

-- name: DueSchedules :many
-- Run inside a short transaction by the store: the row locks keep two leaders (or a
-- retried tick) from materialising the same schedule at the same moment.
SELECT * FROM schedules
 WHERE enabled AND next_run <= sqlc.arg('now')
 ORDER BY next_run
 LIMIT sqlc.arg('batch_limit')
   FOR UPDATE SKIP LOCKED;

-- name: AdvanceSchedule :execrows
UPDATE schedules
   SET last_run = sqlc.arg('last_run')::timestamptz, next_run = sqlc.arg('next_run'), updated_at = now()
 WHERE id = sqlc.arg('id');
