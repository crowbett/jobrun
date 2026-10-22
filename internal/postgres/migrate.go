package postgres

import (
	"context"
	"embed"
	"fmt"
	"hash/fnv"
	"io/fs"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migration lock polling: retry every second for up to 25 s, inside the
// 30 s startup budget of cmd/jobrund, so a replica that loses the race waits
// for the winner instead of failing.
const (
	migrateLockPeriodSec = 1
	migrateLockAttempts  = 25
)

// Migrate applies every pending goose migration embedded in migrations/ to the
// database behind pool. It is idempotent: a second call is a no-op. The goose
// version table lives in the first schema of the connection's search_path, so
// a pool configured with a private search_path migrates that schema only.
//
// Concurrent callers (several jobrund replicas starting at once) are
// serialised by a Postgres session advisory lock keyed on that schema: the
// first applies the migrations, the rest wait and then find nothing to do.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	sub, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("migrations fs: %w", err)
	}
	var schema string
	if err := pool.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		return fmt.Errorf("current schema: %w", err)
	}
	locker, err := lock.NewPostgresSessionLocker(
		lock.WithLockID(migrateLockID(schema)),
		lock.WithLockTimeout(migrateLockPeriodSec, migrateLockAttempts),
	)
	if err != nil {
		return fmt.Errorf("migration locker: %w", err)
	}

	// The *sql.DB borrows connections from pool; closing it releases them and
	// leaves the pool open.
	db := stdlib.OpenDBFromPool(pool)
	defer func() { _ = db.Close() }()

	p, err := goose.NewProvider(goose.DialectPostgres, db, sub, goose.WithSessionLocker(locker))
	if err != nil {
		return fmt.Errorf("goose provider: %w", err)
	}
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}

// migrateLockID is the advisory lock key for migrating schema: one lock per
// schema, so callers migrating different schemas (the integration tests run
// each test in its own) never wait on each other.
func migrateLockID(schema string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("jobrun.migrate:" + schema))
	return int64(h.Sum64()) //nolint:gosec // reinterpreting the hash bits as a signed lock key is intended
}
