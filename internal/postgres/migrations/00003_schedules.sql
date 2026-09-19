-- +goose Up
CREATE TABLE schedules (
    id           uuid PRIMARY KEY,
    name         text NOT NULL UNIQUE,
    cron         text NOT NULL,
    timezone     text NOT NULL DEFAULT 'UTC',
    queue        text NOT NULL,
    job_type     text NOT NULL,
    payload      jsonb NOT NULL DEFAULT '{}'::jsonb,
    priority     smallint NOT NULL DEFAULT 0,
    max_attempts int NOT NULL DEFAULT 5,
    backoff      jsonb NOT NULL DEFAULT '{}'::jsonb,
    enabled      bool NOT NULL DEFAULT true,
    next_run     timestamptz NOT NULL,
    last_run     timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX schedules_due ON schedules (next_run) WHERE enabled;

-- +goose Down
DROP INDEX IF EXISTS schedules_due;
DROP TABLE IF EXISTS schedules;
