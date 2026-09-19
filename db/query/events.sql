-- name: InsertJobEvent :exec
INSERT INTO job_events (job_id, queue, type, from_status, to_status, attempt, error, at)
VALUES (
    sqlc.arg('job_id'), sqlc.arg('queue'), sqlc.arg('type'),
    NULLIF(sqlc.arg('from_status')::text, '')::job_status, sqlc.arg('to_status')::text::job_status,
    sqlc.arg('attempt'), sqlc.narg('error'), COALESCE(sqlc.narg('at')::timestamptz, now())
);

-- name: InsertJobEventsBatch :batchexec
-- Batch form for claims: the same row as InsertJobEvent, sent as one pgx batch.
INSERT INTO job_events (job_id, queue, type, from_status, to_status, attempt, error, at)
VALUES (
    sqlc.arg('job_id'), sqlc.arg('queue'), sqlc.arg('type'),
    NULLIF(sqlc.arg('from_status')::text, '')::job_status, sqlc.arg('to_status')::text::job_status,
    sqlc.arg('attempt'), sqlc.narg('error'), COALESCE(sqlc.narg('at')::timestamptz, now())
);

-- name: ListJobEvents :many
SELECT * FROM job_events WHERE job_id = $1 ORDER BY id;

-- name: PruneJobEvents :execrows
DELETE FROM job_events WHERE at < now() - sqlc.arg('older_than')::interval;
