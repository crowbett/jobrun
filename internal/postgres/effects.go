package postgres

import (
	"context"
	"fmt"

	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/internal/postgres/sqlc"
)

// EffectKey is the processed_effects primary key for a job: scope is
// "job:<type>" and key is the idempotency key when the job has one, otherwise
// the job id. Two enqueues that share a queue and idempotency key are the same
// job by definition, so they also share one effect.
func EffectKey(j job.Job) (scope, key string) {
	scope = "job:" + j.Type
	key = j.IdempotencyKey
	if key == "" {
		key = j.ID.String()
	}
	return scope, key
}

// EffectDone reports whether the effect (scope, key) has already been recorded.
// It is the fast-path check a handler may use before doing expensive work; the
// durable guard is the processed_effects row Complete writes in the completion
// transaction, so a false negative here only costs the work, never correctness.
func (s *Store) EffectDone(ctx context.Context, scope, key string) (bool, error) {
	done, err := s.q.ProcessedEffectExists(ctx, sqlc.ProcessedEffectExistsParams{Scope: scope, Key: key})
	if err != nil {
		return false, fmt.Errorf("processed effect lookup: %w", err)
	}
	return done, nil
}

// markEffect inserts the ledger row inside the completion transaction and
// reports whether it was new. false means the effect was already committed by
// an earlier completion of the same key (a Retry of a job that ended cancelled
// after its effect landed, for instance) and the Commit closure must be skipped.
func markEffect(ctx context.Context, q *sqlc.Queries, j job.Job) (bool, error) {
	scope, key := EffectKey(j)
	n, err := q.InsertProcessedEffect(ctx, sqlc.InsertProcessedEffectParams{Scope: scope, Key: key, JobID: j.ID})
	if err != nil {
		return false, fmt.Errorf("insert processed effect: %w", err)
	}
	return n == 1, nil
}
