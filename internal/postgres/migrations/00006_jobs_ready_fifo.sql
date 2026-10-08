-- +goose NO TRANSACTION
-- +goose Up
-- The FIFO claim index (CLAIM_FIFO_EVERY): every Nth claim per queue takes the
-- oldest ready jobs whatever their priority, so a flood of high-priority work
-- cannot starve low-priority jobs forever. Same partial predicate as
-- jobs_ready, ordered by age only; CONCURRENTLY so a live queue keeps claiming
-- while it builds (hence no transaction).
CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_ready_fifo ON jobs (queue, run_at, id) WHERE status = 'queued';

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS jobs_ready_fifo;
