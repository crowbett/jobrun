package admin

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crowbett/jobrun/internal/job"
)

func TestDepthCacheRefreshesOnTickerAndKeepsLastGood(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	fetch := func(context.Context) ([]job.DepthRow, error) {
		n := calls.Add(1)
		if n >= 3 {
			return nil, errors.New("db down")
		}
		return []job.DepthRow{{Queue: "q", Status: job.StatusQueued, N: n}}, nil
	}
	c := NewDepthCache(fetch, 10*time.Millisecond, nil)
	if rows, at := c.Depth(); rows != nil || !at.IsZero() {
		t.Fatalf("before Run: rows=%v at=%v, want empty/zero", rows, at)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	// First refresh is immediate, second on the first tick.
	if !waitFor(t, 2*time.Second, func() bool { rows, _ := c.Depth(); return len(rows) == 1 && rows[0].N == 2 }) {
		rows, _ := c.Depth()
		t.Fatalf("cache did not reach the second refresh: %v", rows)
	}
	// Every later fetch fails: the value must stay at N=2.
	if !waitFor(t, 2*time.Second, func() bool { return calls.Load() >= 5 }) {
		t.Fatal("ticker stalled")
	}
	rows, at := c.Depth()
	if len(rows) != 1 || rows[0].N != 2 || at.IsZero() {
		t.Errorf("after failures: rows=%v at=%v, want the last good snapshot", rows, at)
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestDepthCacheRefreshReturnsError(t *testing.T) {
	t.Parallel()
	want := errors.New("nope")
	c := NewDepthCache(func(context.Context) ([]job.DepthRow, error) { return nil, want }, 0, nil)
	if err := c.Refresh(t.Context()); !errors.Is(err, want) {
		t.Fatalf("Refresh = %v, want %v", err, want)
	}
	if c.every != DefaultDepthInterval {
		t.Errorf("every = %v, want default %v", c.every, DefaultDepthInterval)
	}
}

func TestHistogramBuckets(t *testing.T) {
	t.Parallel()
	h := NewHistogram()
	var obs DurationObserver = h                      // the worker-facing interface
	obs.ObserveDuration("q", "t", 5*time.Millisecond) // exactly on a bound: le="0.005" includes it
	obs.ObserveDuration("q", "t", 6*time.Millisecond)
	obs.ObserveDuration("q", "t", -time.Second) // clamped to 0
	obs.ObserveDuration("q", "t", time.Hour)    // +Inf
	s := h.Snapshot()
	if len(s) != 1 || s[0].Key != (SeriesKey{"q", "t"}) || s[0].Count != 4 {
		t.Fatalf("snapshot = %+v", s)
	}
	if s[0].Cumulative[0] != 2 || s[0].Cumulative[1] != 3 || s[0].Cumulative[numBuckets-1] != 3 {
		t.Errorf("cumulative = %v", s[0].Cumulative)
	}
	if want := 0.005 + 0.006 + 3600; s[0].Sum != want {
		t.Errorf("sum = %v, want %v", s[0].Sum, want)
	}
	if NewHistogram().Snapshot() == nil {
		t.Error("empty snapshot should be a non-nil empty slice")
	}
}
