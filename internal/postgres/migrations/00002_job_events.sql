-- +goose Up
-- Append-only history, written in the same transaction as the jobs transition it
-- records. from_status is NULL for the enqueue event.
CREATE TABLE job_events (
    id          bigserial PRIMARY KEY,
    job_id      uuid NOT NULL,
    queue       text NOT NULL,
    type        text NOT NULL,
    from_status job_status,
    to_status   job_status NOT NULL,
    attempt     int NOT NULL DEFAULT 0,
    error       text,
    at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX job_events_job ON job_events (job_id, id);
CREATE INDEX job_events_at  ON job_events (at);

-- +goose Down
DROP INDEX IF EXISTS job_events_at;
DROP INDEX IF EXISTS job_events_job;
DROP TABLE IF EXISTS job_events;
