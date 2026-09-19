-- +goose Up
-- The orders processed_events pattern, generalised: one row per durable effect,
-- inserted in the completion transaction next to the handler's own writes.
-- scope = 'job:' || type, key = idempotency_key (or the job id when there is none).
CREATE TABLE processed_effects (
    scope        text NOT NULL,
    key          text NOT NULL,
    job_id       uuid NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, key)
);

-- +goose Down
DROP TABLE IF EXISTS processed_effects;
