package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool limits applied by Open. MaxConns is a parameter: the plan sizes it as
// WORKER_CONCURRENCY + 8 so the claim loop, heartbeater, reaper, scheduler and
// the gRPC surface never starve behind handlers holding completion transactions.
const (
	// DefaultMaxConns is used when Open is given a non-positive maxConns.
	DefaultMaxConns       = 24
	poolMaxConnLifetime   = 30 * time.Minute
	poolHealthCheckPeriod = 30 * time.Second
)

// Open parses url, applies the production pool limits (maxConns connections,
// 30m lifetime, 30s health check) and returns a pinged pool.
func Open(ctx context.Context, url string, maxConns int32, log *slog.Logger) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if maxConns <= 0 {
		maxConns = DefaultMaxConns
	}
	cfg.MaxConns = maxConns
	cfg.MaxConnLifetime = poolMaxConnLifetime
	cfg.HealthCheckPeriod = poolHealthCheckPeriod
	return OpenConfig(ctx, cfg, log)
}

// OpenConfig opens a pool from an already-built config (tests use it to set a
// per-test search_path) and pings it before returning. The pool is closed again
// if the ping fails.
func OpenConfig(ctx context.Context, cfg *pgxpool.Config, log *slog.Logger) (*pgxpool.Pool, error) {
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	log.InfoContext(ctx, "postgres pool open",
		slog.String("host", cfg.ConnConfig.Host),
		slog.Int("port", int(cfg.ConnConfig.Port)),
		slog.String("database", cfg.ConnConfig.Database),
		slog.Int("max_conns", int(cfg.MaxConns)))
	return pool, nil
}
