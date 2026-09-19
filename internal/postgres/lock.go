package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Locker is the leader-election port the scheduler consumes (docs/PLAN.md
// spells it as scheduler.Locker; this is the same signature so the scheduler
// can define its own interface and AdvisoryLocker satisfies both). release
// must be called exactly once when the caller gives leadership up.
type Locker interface {
	TryLock(ctx context.Context, key int64) (release func(context.Context) error, acquired bool, err error)
}

// AdvisoryLocker implements Locker with a session-level pg_try_advisory_lock
// on a dedicated connection (not a pool connection, so a busy pool can never
// stall the leader and a leader can never exhaust the pool). The lock is
// released by release, or by Postgres when the connection dies, in which case
// the scheduler re-elects on its next TryLock.
type AdvisoryLocker struct {
	cfg *pgx.ConnConfig
}

var _ Locker = (*AdvisoryLocker)(nil)

// NewLocker returns an AdvisoryLocker that dials with pool's connection config.
func NewLocker(pool *pgxpool.Pool) *AdvisoryLocker {
	return &AdvisoryLocker{cfg: pool.Config().ConnConfig.Copy()}
}

// TryLock dials a dedicated connection and tries the advisory lock for key.
// acquired false (and a nil release) means another session holds it; the
// connection is closed again. On success the connection stays open until
// release is called.
func (l *AdvisoryLocker) TryLock(ctx context.Context, key int64) (func(context.Context) error, bool, error) {
	conn, err := pgx.ConnectConfig(ctx, l.cfg.Copy())
	if err != nil {
		return nil, false, fmt.Errorf("connect locker: %w", err)
	}
	var ok bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&ok); err != nil {
		_ = conn.Close(ctx)
		return nil, false, fmt.Errorf("pg_try_advisory_lock(%d): %w", key, err)
	}
	if !ok {
		if err := conn.Close(ctx); err != nil {
			return nil, false, fmt.Errorf("close locker: %w", err)
		}
		return nil, false, nil
	}
	var once sync.Once
	release := func(ctx context.Context) error {
		err := errors.New("advisory lock already released")
		once.Do(func() {
			err = nil
			closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if _, uerr := conn.Exec(closeCtx, "SELECT pg_advisory_unlock($1)", key); uerr != nil {
				err = fmt.Errorf("pg_advisory_unlock(%d): %w", key, uerr)
			}
			if cerr := conn.Close(closeCtx); cerr != nil && err == nil {
				err = fmt.Errorf("close locker: %w", cerr)
			}
		})
		return err
	}
	return release, true, nil
}
