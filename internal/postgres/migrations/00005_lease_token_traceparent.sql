-- +goose Up
-- lease_token is the fencing token: every claim bumps it, and heartbeat, complete
-- and fail are guarded by (id, lease_owner, lease_token). A worker holding an
-- earlier lease of the same job - reaped and claimed again, even by the same
-- owner - can no longer extend or finish it. ADD COLUMN with a constant default
-- is metadata-only (no rewrite, brief ACCESS EXCLUSIVE).
ALTER TABLE jobs ADD COLUMN lease_token bigint NOT NULL DEFAULT 0;
-- traceparent is the W3C trace context of the enqueue span (NULL when untraced);
-- the run span is started as its child, in-process or over WorkerService.
ALTER TABLE jobs ADD COLUMN traceparent text;

-- +goose Down
ALTER TABLE jobs DROP COLUMN IF EXISTS traceparent;
ALTER TABLE jobs DROP COLUMN IF EXISTS lease_token;
