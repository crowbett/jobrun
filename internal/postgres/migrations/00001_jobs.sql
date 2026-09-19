-- +goose Up
-- job_status: 'failed' is reserved ("last attempt failed, not yet dead") and is never
-- written in v1. A job waiting for its next attempt is 'queued' with a future run_at,
-- so the partial claim index below is the only thing a claimer ever reads.
CREATE TYPE job_status AS ENUM ('queued', 'leased', 'succeeded', 'failed', 'dead', 'cancelled');

CREATE TABLE jobs (
    id               uuid PRIMARY KEY,
    queue            text NOT NULL,
    type             text NOT NULL,
    payload          jsonb NOT NULL DEFAULT '{}'::jsonb,
    status           job_status NOT NULL DEFAULT 'queued',
    priority         smallint NOT NULL DEFAULT 0,
    run_at           timestamptz NOT NULL DEFAULT now(),
    attempts         int NOT NULL DEFAULT 0,
    max_attempts     int NOT NULL DEFAULT 5 CHECK (max_attempts BETWEEN 1 AND 100),
    backoff          jsonb NOT NULL DEFAULT '{"base":"1s","max":"5m","factor":2,"jitter":true}'::jsonb,
    lease_owner      text,
    lease_until      timestamptz,
    last_error       text,
    idempotency_key  text,
    cancel_requested bool NOT NULL DEFAULT false,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    started_at       timestamptz,
    finished_at      timestamptz,
    CHECK ((status = 'leased') = (lease_owner IS NOT NULL))
);

CREATE UNIQUE INDEX jobs_idem_uniq ON jobs (queue, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX jobs_ready  ON jobs (queue, priority DESC, run_at, id) WHERE status = 'queued'; -- the claim index
CREATE INDEX jobs_leases ON jobs (lease_until) WHERE status = 'leased';                      -- the reaper index
CREATE INDEX jobs_keyset ON jobs (queue, created_at DESC, id DESC);                          -- List
CREATE INDEX jobs_purge  ON jobs (finished_at) WHERE status IN ('succeeded', 'cancelled', 'dead');

-- +goose Down
DROP INDEX IF EXISTS jobs_purge;
DROP INDEX IF EXISTS jobs_keyset;
DROP INDEX IF EXISTS jobs_leases;
DROP INDEX IF EXISTS jobs_ready;
DROP INDEX IF EXISTS jobs_idem_uniq;
DROP TABLE IF EXISTS jobs;
DROP TYPE IF EXISTS job_status;
