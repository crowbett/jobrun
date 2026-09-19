// Package scheduler materialises cron schedules into jobs. Exactly one process
// does it at a time: every instance tries an advisory lock on each tick and
// only the holder (the leader) reads due schedules (docs/PLAN.md, step 8).
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/crowbett/jobrun/internal/cronx"
	"github.com/crowbett/jobrun/internal/job"
)

// Locker is the leader-election port; postgres.Locker (pg_try_advisory_lock on
// a dedicated connection) is the v1 implementation. TryLock returns ok=false
// without error when another process holds key. release gives the lock back
// and must be called with a context that is still alive.
type Locker interface {
	TryLock(ctx context.Context, key int64) (release func(context.Context) error, ok bool, err error)
}

// Defaults applied by NewScheduler to zero Config fields.
const (
	DefaultTick = 5 * time.Second
	// DefaultLockKey is "jobrun" as a big-endian ASCII integer.
	DefaultLockKey int64 = 0x6a6f6272756e
	DefaultBatch   int32 = 100

	// releaseTimeout bounds the lock release on stop.
	releaseTimeout = 5 * time.Second
	// maxCache bounds the parsed-cron cache; it is cleared when exceeded.
	maxCache = 1024
)

// Config tunes a Scheduler.
type Config struct {
	// Tick is the period of both the lock attempt and the materialisation (DefaultTick).
	Tick time.Duration
	// LockKey is the advisory lock key shared by every instance (DefaultLockKey).
	LockKey int64
	// Lookahead materialises schedules due within now+Lookahead early; the job
	// still carries run_at = next_run so it never runs early (default 0).
	Lookahead time.Duration
	// Batch is the most schedules handled per tick (DefaultBatch).
	Batch int32
}

func (c Config) withDefaults() Config {
	if c.Tick <= 0 {
		c.Tick = DefaultTick
	}
	if c.LockKey == 0 {
		c.LockKey = DefaultLockKey
	}
	if c.Lookahead < 0 {
		c.Lookahead = 0
	}
	if c.Batch <= 0 {
		c.Batch = DefaultBatch
	}
	return c
}

// Scheduler is one instance of the leader-elected materialiser.
type Scheduler struct {
	store job.Store
	lk    Locker
	log   *slog.Logger
	cfg   Config
	now   func() time.Time

	leader atomic.Bool
	ticks  atomic.Int64 // leader ticks

	cache map[string]cronEntry // only touched by Run's goroutine
}

type cronEntry struct {
	spec cronx.Schedule
	loc  *time.Location
}

// NewScheduler wires a scheduler; log may be nil.
func NewScheduler(s job.Store, lk Locker, log *slog.Logger, cfg Config) *Scheduler {
	if log == nil {
		log = slog.Default()
	}
	return &Scheduler{
		store: s,
		lk:    lk,
		log:   log,
		cfg:   cfg.withDefaults(),
		now:   time.Now,
		cache: make(map[string]cronEntry),
	}
}

// Leader reports whether this instance currently holds the lock.
func (s *Scheduler) Leader() bool { return s.leader.Load() }

// Ticks returns how many ticks this instance has completed as leader.
func (s *Scheduler) Ticks() int64 { return s.ticks.Load() }

// IdempotencyKey is the key the scheduler stamps on a materialised job, so a
// double-elected leader or a retried tick cannot enqueue the same firing twice.
func IdempotencyKey(id job.ID, nextRun time.Time) string {
	return "sched:" + id.String() + ":" + nextRun.UTC().Format(time.RFC3339)
}

// Run tries the lock at once and then every Tick until it is acquired, then
// materialises due schedules every Tick until ctx is done, and releases the
// lock before returning nil.
func (s *Scheduler) Run(ctx context.Context) error {
	t := time.NewTicker(s.cfg.Tick)
	defer t.Stop()
	var release func(context.Context) error
	defer func() {
		if release == nil {
			return
		}
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
		defer cancel()
		if err := release(rctx); err != nil {
			s.log.Error("scheduler: lock release failed", "err", err)
		}
		s.leader.Store(false)
		s.log.Info("scheduler: stepped down", "lock_key", s.cfg.LockKey)
	}()
	loggedFollower := false
	for {
		if release == nil {
			rel, ok, err := s.lk.TryLock(ctx, s.cfg.LockKey)
			switch {
			case err != nil:
				if ctx.Err() != nil {
					return nil
				}
				s.log.Error("scheduler: lock attempt failed", "err", err)
			case ok:
				release = rel
				s.leader.Store(true)
				s.log.Info("scheduler: became leader", "lock_key", s.cfg.LockKey)
			case !loggedFollower:
				loggedFollower = true
				s.log.Info("scheduler: following, another instance holds the lock", "lock_key", s.cfg.LockKey)
			}
		}
		if release != nil {
			s.tick(ctx)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// tick materialises every due schedule once.
func (s *Scheduler) tick(ctx context.Context) {
	defer s.ticks.Add(1)
	// now is the app clock; the store compares next_run <= now on its side and a
	// few seconds of skew only shifts which tick a schedule lands on.
	now := s.now()
	due, err := s.store.DueSchedules(ctx, now.Add(s.cfg.Lookahead), s.cfg.Batch)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Error("scheduler: due schedules failed", "err", err)
		}
		return
	}
	for i := range due {
		if ctx.Err() != nil {
			return
		}
		s.materialise(ctx, due[i])
	}
}

// materialise enqueues sc's firing at NextRun and advances it, or disables it
// when its cron no longer parses, its template is invalid, or it has no next
// run within cronx.MaxLookahead years.
func (s *Scheduler) materialise(ctx context.Context, sc job.Schedule) {
	log := s.log.With("schedule", sc.ID, "name", sc.Name, "next_run", sc.NextRun)
	entry, err := s.parse(sc.Cron, sc.Timezone)
	if err != nil {
		s.disable(ctx, log, sc, err)
		return
	}
	tmpl := sc.Template
	tmpl.RunAt, tmpl.Delay = sc.NextRun, 0
	tmpl.IdempotencyKey = IdempotencyKey(sc.ID, sc.NextRun)
	j, err := job.New(tmpl)
	if err != nil {
		s.disable(ctx, log, sc, fmt.Errorf("template: %w", err))
		return
	}
	stored, err := s.store.Enqueue(ctx, j)
	switch {
	case errors.Is(err, job.ErrIdempotencyConflict):
		log.Warn("scheduler: firing already materialised with a different body, advancing", "key", j.IdempotencyKey)
	case err != nil:
		log.Error("scheduler: enqueue failed, retrying next tick", "err", err)
		return
	case stored.ID != j.ID:
		log.Info("scheduler: firing already materialised, advancing", "job", stored.ID)
	default:
		log.Info("scheduler: materialised", "job", j.ID, "queue", j.Queue, "type", j.Type)
	}
	next := entry.spec.Next(sc.NextRun, entry.loc)
	if next.IsZero() {
		if err := s.store.AdvanceSchedule(ctx, sc.ID, sc.NextRun, sc.NextRun); err != nil {
			log.Error("scheduler: advance failed", "err", err)
		}
		s.disable(ctx, log, sc, fmt.Errorf("no next run within %d years", cronx.MaxLookahead))
		return
	}
	// A failed advance leaves next_run where it was; the next tick re-enqueues
	// the same idempotency key, which is a no-op, and tries again.
	if err := s.store.AdvanceSchedule(ctx, sc.ID, sc.NextRun, next); err != nil {
		log.Error("scheduler: advance failed", "err", err)
	}
}

func (s *Scheduler) disable(ctx context.Context, log *slog.Logger, sc job.Schedule, cause error) {
	sc.Enabled = false
	if _, err := s.store.UpdateSchedule(ctx, sc); err != nil {
		log.Error("scheduler: disabling schedule failed", "cause", cause, "err", err)
		return
	}
	log.Warn("scheduler: schedule disabled", "cause", cause)
}

// parse returns the cached cron spec and location for (expr, tz).
func (s *Scheduler) parse(expr, tz string) (cronEntry, error) {
	key := expr + "\x00" + tz
	if e, ok := s.cache[key]; ok {
		return e, nil
	}
	spec, err := cronx.Parse(expr)
	if err != nil {
		return cronEntry{}, err
	}
	loc := time.UTC
	if tz != "" {
		if loc, err = time.LoadLocation(tz); err != nil {
			return cronEntry{}, fmt.Errorf("timezone %q: %w", tz, err)
		}
	}
	if len(s.cache) >= maxCache {
		clear(s.cache)
	}
	e := cronEntry{spec: spec, loc: loc}
	s.cache[key] = e
	return e, nil
}
