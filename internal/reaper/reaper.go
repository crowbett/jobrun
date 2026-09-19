// Package reaper requeues expired leases (with backoff, or dead at
// max_attempts) and prunes finished jobs past their retention (docs/PLAN.md,
// step 8). It is safe to run in every process: ReapExpired uses SKIP LOCKED.
package reaper

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/crowbett/jobrun/internal/job"
)

// EventPublisher receives the transitions ReapExpired made.
type EventPublisher interface {
	Publish(job.Event)
}

type nopPublisher struct{}

func (nopPublisher) Publish(job.Event) {}

// Defaults applied by NewReaper to zero Config fields.
const (
	DefaultLeaseTTL        = 30 * time.Second
	DefaultRetention       = 7 * 24 * time.Hour
	DefaultBatch     int32 = 500

	// purgeEvery is the tick cadence of the retention purge.
	purgeEvery = 10
	// maxRounds bounds how many full batches one tick drains before yielding.
	maxRounds = 100
)

// Config tunes a Reaper.
type Config struct {
	// LeaseTTL is the worker's lease TTL; the reaper ticks every LeaseTTL/3 (DefaultLeaseTTL).
	LeaseTTL time.Duration
	// Retention is how long succeeded, dead and cancelled jobs are kept (DefaultRetention).
	Retention time.Duration
	// Batch bounds one ReapExpired / Purge round trip (DefaultBatch).
	Batch int32
	// Policy is the fallback backoff for jobs without one (job.DefaultPolicy).
	Policy job.Policy
}

func (c Config) withDefaults() Config {
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = DefaultLeaseTTL
	}
	if c.Retention <= 0 {
		c.Retention = DefaultRetention
	}
	if c.Batch <= 0 {
		c.Batch = DefaultBatch
	}
	c.Policy = c.Policy.OrDefault()
	return c
}

// Stats is a snapshot of the reaper's counters.
type Stats struct {
	Ticks  int64 // reap ticks run
	Reaped int64 // leases requeued or killed
	Purged int64 // finished jobs deleted
}

// Reaper is the expired-lease sweeper and retention pruner.
type Reaper struct {
	store job.Store
	pub   EventPublisher
	log   *slog.Logger
	cfg   Config
	now   func() time.Time

	ticks, reaped, purged atomic.Int64
}

// NewReaper wires a reaper; pub and log may be nil.
func NewReaper(s job.Store, pub EventPublisher, log *slog.Logger, cfg Config) *Reaper {
	if pub == nil {
		pub = nopPublisher{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Reaper{store: s, pub: pub, log: log, cfg: cfg.withDefaults(), now: time.Now}
}

// Stats returns a snapshot of the counters.
func (r *Reaper) Stats() Stats {
	return Stats{Ticks: r.ticks.Load(), Reaped: r.reaped.Load(), Purged: r.purged.Load()}
}

// Interval is the tick period, LeaseTTL/3.
func (r *Reaper) Interval() time.Duration {
	return max(r.cfg.LeaseTTL/3, time.Millisecond)
}

// Run reaps every LeaseTTL/3 and purges every tenth tick until ctx is done,
// then returns nil. Store errors are logged and the next tick tries again.
func (r *Reaper) Run(ctx context.Context) error {
	t := time.NewTicker(r.Interval())
	defer t.Stop()
	for n := int64(0); ; {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		n++
		r.ticks.Store(n)
		r.reap(ctx)
		if n%purgeEvery == 0 {
			r.purge(ctx)
		}
	}
}

// reap drains expired leases in batches, publishing every transition.
func (r *Reaper) reap(ctx context.Context) {
	for range maxRounds {
		if ctx.Err() != nil {
			return
		}
		// now is the app clock; the store's own comparison against lease_until
		// happens on the database clock (docs/PLAN.md, Risks).
		events, err := r.store.ReapExpired(ctx, r.now(), r.cfg.Batch, r.cfg.Policy)
		if err != nil {
			if ctx.Err() == nil {
				r.log.Error("reaper: reap failed", "err", err)
			}
			return
		}
		if len(events) == 0 {
			return
		}
		var dead int
		for _, e := range events {
			r.pub.Publish(e)
			if e.To == job.StatusDead {
				dead++
			}
		}
		r.reaped.Add(int64(len(events)))
		r.log.Warn("reaper: expired leases requeued", "n", len(events), "dead", dead)
		if int32(len(events)) < r.cfg.Batch { //nolint:gosec // len <= Batch
			return
		}
	}
}

// purge deletes finished jobs older than Retention in batches.
func (r *Reaper) purge(ctx context.Context) {
	var total int64
	for range maxRounds {
		if ctx.Err() != nil {
			return
		}
		n, err := r.store.Purge(ctx, job.PurgeQuery{
			Statuses:  []job.Status{job.StatusSucceeded, job.StatusDead, job.StatusCancelled},
			OlderThan: r.cfg.Retention,
			Limit:     r.cfg.Batch,
		})
		if err != nil {
			if ctx.Err() == nil {
				r.log.Error("reaper: purge failed", "err", err)
			}
			break
		}
		total += n
		if n < int64(r.cfg.Batch) {
			break
		}
	}
	if total > 0 {
		r.purged.Add(total)
		r.log.Info("reaper: purged finished jobs", "n", total, "older_than", r.cfg.Retention)
	}
}
