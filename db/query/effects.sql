-- name: InsertProcessedEffect :execrows
INSERT INTO processed_effects (scope, key, job_id)
VALUES ($1, $2, $3)
ON CONFLICT (scope, key) DO NOTHING;

-- name: ProcessedEffectExists :one
SELECT EXISTS (SELECT 1 FROM processed_effects WHERE scope = $1 AND key = $2);
