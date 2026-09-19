package admin

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/crowbett/jobrun/internal/job"
)

// DefaultDepthInterval is how often the depth cache refreshes.
const DefaultDepthInterval = 5 * time.Second

// DepthCache refreshes the (queue, status) depth table on a ticker and serves
// the last good value to /metrics, /admin/stats, the SSE snapshot and the
// Stats RPC, so a scrape never runs the full scan itself. It satisfies
// server.DepthSource.
type DepthCache struct {
	fetch func(context.Context) ([]job.DepthRow, error)
	every time.Duration
	log   *slog.Logger

	mu   sync.RWMutex
	rows []job.DepthRow
	at   time.Time
}

// NewDepthCache builds a cache over fetch (normally job.Store.Depth). every
// <= 0 means DefaultDepthInterval. Nothing runs until Run is called; Depth
// returns an empty table with a zero time before the first refresh.
func NewDepthCache(fetch func(context.Context) ([]job.DepthRow, error), every time.Duration, log *slog.Logger) *DepthCache {
	if every <= 0 {
		every = DefaultDepthInterval
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &DepthCache{fetch: fetch, every: every, log: log}
}

// Run refreshes immediately, then every interval, until ctx is done. It only
// returns ctx.Err(); a failed refresh is logged and the last value is kept.
func (c *DepthCache) Run(ctx context.Context) error {
	c.refresh(ctx)
	t := time.NewTicker(c.every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			c.refresh(ctx)
		}
	}
}

// Refresh fetches once, synchronously, and returns the fetch error. Use it
// before serving when the first scrape must not see an empty table.
func (c *DepthCache) Refresh(ctx context.Context) error {
	rows, err := c.fetch(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.rows, c.at = rows, time.Now()
	c.mu.Unlock()
	return nil
}

func (c *DepthCache) refresh(ctx context.Context) {
	if err := c.Refresh(ctx); err != nil && ctx.Err() == nil {
		c.log.Warn("depth cache refresh failed; serving the last snapshot", slog.Any("err", err))
	}
}

// Depth returns the last good table and when it was taken (zero before the
// first successful refresh). The slice is shared: callers must not modify it.
func (c *DepthCache) Depth() ([]job.DepthRow, time.Time) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rows, c.at
}
