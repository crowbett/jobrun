package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crowbett/jobrun/internal/job"
)

const testQueue = "q"

var errBoom = errors.New("boom")

func newJob(typ string) job.Job {
	return job.Job{
		ID:          job.NewID(),
		Queue:       testQueue,
		Type:        typ,
		Payload:     []byte("{}"),
		Status:      job.StatusQueued,
		MaxAttempts: 3,
		Backoff:     job.DefaultPolicy,
	}
}

func fastConfig() Config {
	return Config{
		Queues:         []string{testQueue},
		Concurrency:    4,
		BatchSize:      8,
		LeaseTTL:       time.Second,
		HeartbeatEvery: 10 * time.Millisecond,
		JobTimeout:     5 * time.Second,
		Owner:          "test-owner",
		PollFloor:      5 * time.Millisecond,
	}
}

// startPool runs p until the test ends and returns a stop func that cancels
// and waits for Run.
func startPool(t *testing.T, p *Pool) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	var once atomic.Bool
	var result error
	stop = func() error {
		if once.CompareAndSwap(false, true) {
			cancel()
			select {
			case result = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("Run did not return after cancel")
			}
		}
		return result
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func recv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

func expectNone[T any](t *testing.T, ch <-chan T, d time.Duration, what string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("unexpected %s: %+v", what, v)
	case <-time.After(d):
	}
}

func registry(t *testing.T, typ string, h job.Handler) *job.Registry {
	t.Helper()
	reg := job.NewRegistry()
	if err := reg.Register(typ, h); err != nil {
		t.Fatal(err)
	}
	return reg
}

func TestSuccessCompletesWithNilCommit(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	pub := &fakePub{}
	reg := registry(t, "ok", job.HandlerFunc(func(context.Context, job.Job) error { return nil }))
	j := newJob("ok")
	store.add(j)
	p := NewPool(store, nil, reg, pub, slog.Default(), fastConfig())
	stop := startPool(t, p)

	c := recv(t, store.completes, "complete")
	if c.ID != j.ID || c.Owner != "test-owner" || c.Commit != nil {
		t.Fatalf("complete = %+v, want id %s owner test-owner nil commit", c, j.ID)
	}
	expectNone(t, store.fails, 20*time.Millisecond, "fail")
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	st := p.Stats()
	if st.Claimed != 1 || st.Succeeded != 1 || st.Failed != 0 || st.InFlight != 0 {
		t.Fatalf("stats = %+v", st)
	}
	ev := pub.all()
	if len(ev) != 2 || ev[0].From != job.StatusQueued || ev[0].To != job.StatusLeased ||
		ev[1].From != job.StatusLeased || ev[1].To != job.StatusSucceeded || ev[1].Attempt != 1 {
		t.Fatalf("events = %+v", ev)
	}
}

func TestTxHandlerCompletesWithCommit(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	reg := job.NewRegistry()
	var ran atomic.Bool
	err := reg.RegisterTx("tx", job.TxHandlerFunc(func(context.Context, job.Job) (job.Commit, error) {
		return func(context.Context, job.Execer) error { ran.Store(true); return nil }, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	store.add(newJob("tx"))
	p := NewPool(store, nil, reg, nil, nil, fastConfig())
	startPool(t, p)

	c := recv(t, store.completes, "complete")
	if c.Commit == nil {
		t.Fatal("TxHandler must hand its Commit to Complete")
	}
	if err := c.Commit(context.Background(), nil); err != nil || !ran.Load() {
		t.Fatalf("commit ran=%v err=%v", ran.Load(), err)
	}
}

func TestPermanentErrorFailsDead(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	pub := &fakePub{}
	reg := registry(t, "perm", job.HandlerFunc(func(context.Context, job.Job) error {
		return fmt.Errorf("%w: bad payload", job.ErrPermanent)
	}))
	store.add(newJob("perm"))
	p := NewPool(store, nil, reg, pub, nil, fastConfig())
	startPool(t, p)

	f := recv(t, store.fails, "fail")
	if !f.Perm || f.Status != job.StatusDead || !strings.Contains(f.Cause, "bad payload") {
		t.Fatalf("fail = %+v", f)
	}
	expectNone(t, store.completes, 20*time.Millisecond, "complete")
	if st := p.Stats(); st.Failed != 1 || st.Dead != 1 || st.Retried != 0 {
		t.Fatalf("stats = %+v", st)
	}
	ev := pub.all()
	if len(ev) != 2 || ev[1].To != job.StatusDead || ev[1].Err == "" {
		t.Fatalf("events = %+v", ev)
	}
}

func TestTransientErrorRetriesInTheFuture(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	reg := registry(t, "flaky", job.HandlerFunc(func(context.Context, job.Job) error { return errBoom }))
	j := newJob("flaky")
	j.Backoff = job.Policy{Base: time.Minute, Max: time.Hour, Factor: 2, Jitter: false}
	store.add(j)
	p := NewPool(store, nil, reg, nil, nil, fastConfig())
	before := time.Now()
	startPool(t, p)

	f := recv(t, store.fails, "fail")
	if f.Perm || f.Status != job.StatusQueued || f.Cause != "boom" {
		t.Fatalf("fail = %+v", f)
	}
	// attempt 1 with jitter off -> exactly Base after "now".
	if got := f.RetryAt.Sub(before); got < time.Minute || got > time.Minute+5*time.Second {
		t.Fatalf("retryAt is %v after start, want ~1m", got)
	}
	if st := p.Stats(); st.Retried != 1 || st.Failed != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestUnknownTypeFailsPermanently(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	store.add(newJob("nobody-home"))
	p := NewPool(store, nil, job.NewRegistry(), nil, nil, fastConfig())
	startPool(t, p)

	f := recv(t, store.fails, "fail")
	if !f.Perm || !strings.Contains(f.Cause, "no handler registered") || !strings.Contains(f.Cause, "nobody-home") {
		t.Fatalf("fail = %+v", f)
	}
}

func TestHandlerPanicIsATransientFailure(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	reg := registry(t, "panic", job.HandlerFunc(func(context.Context, job.Job) error { panic("kaboom") }))
	store.add(newJob("panic"))
	p := NewPool(store, nil, reg, nil, nil, fastConfig())
	startPool(t, p)

	f := recv(t, store.fails, "fail")
	if f.Perm || !strings.Contains(f.Cause, "kaboom") {
		t.Fatalf("fail = %+v", f)
	}
}

func TestJobTimeoutCancelsAndFails(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	reg := registry(t, "slow", job.HandlerFunc(func(ctx context.Context, _ job.Job) error {
		<-ctx.Done()
		return ctx.Err()
	}))
	store.add(newJob("slow"))
	cfg := fastConfig()
	cfg.JobTimeout = 30 * time.Millisecond
	p := NewPool(store, nil, reg, nil, nil, cfg)
	startPool(t, p)

	f := recv(t, store.fails, "fail")
	if f.Perm || !strings.Contains(f.Cause, "job timeout after 30ms") {
		t.Fatalf("fail = %+v", f)
	}
}

func TestHeartbeatDropCancelsHandler(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	reg := registry(t, "hb", job.HandlerFunc(func(ctx context.Context, _ job.Job) error {
		<-ctx.Done()
		return ctx.Err()
	}))
	j := newJob("hb")
	store.mu.Lock()
	store.drop[j.ID] = true // the store stops keeping the lease on the first heartbeat
	store.lost[j.ID] = true // and Fail comes back ErrLeaseLost
	store.mu.Unlock()
	store.add(j)
	p := NewPool(store, nil, reg, nil, nil, fastConfig())
	startPool(t, p)

	hb := recv(t, store.heartbeats, "heartbeat")
	if hb.Owner != "test-owner" || len(hb.IDs) != 1 || hb.IDs[0] != j.ID || len(hb.Kept) != 0 {
		t.Fatalf("heartbeat = %+v", hb)
	}
	if hb.Until.Before(time.Now().Add(500 * time.Millisecond)) {
		t.Fatalf("heartbeat until %v is not ~LeaseTTL ahead", hb.Until)
	}
	f := recv(t, store.fails, "fail")
	if !strings.Contains(f.Cause, "lease lost or cancel requested") {
		t.Fatalf("fail = %+v", f)
	}
	// Give the goroutine a moment to record the ErrLeaseLost answer.
	deadline := time.Now().Add(2 * time.Second)
	for p.Stats().LeaseLost != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if st := p.Stats(); st.LeaseLost != 1 || st.Retried != 0 || st.Dead != 0 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestCooperativeCancelIsNotCountedAsRetry(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	pub := &fakePub{}
	reg := registry(t, "cancel", job.HandlerFunc(func(ctx context.Context, _ job.Job) error {
		<-ctx.Done()
		return ctx.Err()
	}))
	j := newJob("cancel")
	store.mu.Lock()
	store.drop[j.ID] = true
	store.cancelled[j.ID] = true
	store.mu.Unlock()
	store.add(j)
	p := NewPool(store, nil, reg, pub, nil, fastConfig())
	startPool(t, p)

	f := recv(t, store.fails, "fail")
	if f.Status != job.StatusCancelled {
		t.Fatalf("fail = %+v", f)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(pub.all()) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	ev := pub.all()
	if len(ev) != 2 || ev[1].To != job.StatusCancelled {
		t.Fatalf("events = %+v", ev)
	}
	if st := p.Stats(); st.Retried != 0 || st.Dead != 0 || st.Failed != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestConcurrencyIsRespected(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	started := make(chan struct{}, 16)
	release := make(chan struct{})
	var inFlight, maxInFlight atomic.Int64
	reg := registry(t, "block", job.HandlerFunc(func(context.Context, job.Job) error {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			cur := maxInFlight.Load()
			if n <= cur || maxInFlight.CompareAndSwap(cur, n) {
				break
			}
		}
		started <- struct{}{}
		<-release
		return nil
	}))
	for range 6 {
		store.add(newJob("block"))
	}
	cfg := fastConfig()
	cfg.Concurrency = 2
	cfg.BatchSize = 10
	p := NewPool(store, nil, reg, nil, nil, cfg)
	startPool(t, p)

	recv(t, started, "first start")
	recv(t, started, "second start")
	expectNone(t, started, 50*time.Millisecond, "third start while saturated")
	if st := p.Stats(); st.InFlight != 2 {
		t.Fatalf("InFlight = %d, want 2", st.InFlight)
	}
	close(release)
	for range 6 {
		recv(t, store.completes, "complete")
	}
	if maxInFlight.Load() > 2 {
		t.Fatalf("max in flight = %d, want <= 2", maxInFlight.Load())
	}
	for _, q := range store.claimQueries() {
		if q.Limit > 2 || q.Limit < 1 {
			t.Fatalf("claim asked for %d, want 1..2 (free slots)", q.Limit)
		}
		if q.Owner != "test-owner" || q.Queue != testQueue || q.LeaseTTL != time.Second {
			t.Fatalf("claim query = %+v", q)
		}
	}
}

func TestFullBatchLoopsWithoutWaiting(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	reg := registry(t, "ok", job.HandlerFunc(func(context.Context, job.Job) error { return nil }))
	for range 5 {
		store.add(newJob("ok"))
	}
	cfg := fastConfig()
	cfg.BatchSize = 2
	cfg.PollFloor = time.Hour // neither the floor nor the dispatcher may help
	p := NewPool(store, job.PollOnly{Every: time.Hour}, reg, nil, nil, cfg)
	startPool(t, p)

	for range 5 {
		recv(t, store.completes, "complete")
	}
	qs := store.claimQueries()
	if len(qs) < 3 {
		t.Fatalf("expected at least 3 claims (2 full, then a short one), got %d", len(qs))
	}
}

func TestDispatcherWaitWakesTheLoop(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	reg := registry(t, "ok", job.HandlerFunc(func(context.Context, job.Job) error { return nil }))
	d := chanDispatcher{ch: make(chan struct{}, 1)}
	cfg := fastConfig()
	cfg.PollFloor = time.Hour
	p := NewPool(store, d, reg, nil, nil, cfg)
	startPool(t, p)

	time.Sleep(20 * time.Millisecond) // the first (empty) claim has happened
	store.add(newJob("ok"))
	expectNone(t, store.completes, 30*time.Millisecond, "complete before the wake-up")
	d.ch <- struct{}{}
	recv(t, store.completes, "complete after the wake-up")
}

func TestClaimErrorBacksOffAndRetries(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	store.claimErrs = 2
	reg := registry(t, "ok", job.HandlerFunc(func(context.Context, job.Job) error { return nil }))
	store.add(newJob("ok"))
	p := NewPool(store, nil, reg, nil, nil, fastConfig())
	p.claimBackoff = 5 * time.Millisecond
	startPool(t, p)

	recv(t, store.completes, "complete after claim errors")
	if len(store.claimQueries()) < 3 {
		t.Fatalf("expected the two failed claims to be retried, got %d claims", len(store.claimQueries()))
	}
}

func TestDrainWaitsForInFlightHandlers(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	reg := registry(t, "block", job.HandlerFunc(func(ctx context.Context, _ job.Job) error {
		started <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err() // must not happen: stop does not cancel handlers
		}
	}))
	store.add(newJob("block"))
	cfg := fastConfig()
	cfg.JobTimeout = time.Hour
	p := NewPool(store, nil, reg, nil, nil, cfg)
	stop := startPool(t, p)

	recv(t, started, "start")
	stopped := make(chan error, 1)
	go func() { stopped <- stop() }()
	expectNone(t, stopped, 50*time.Millisecond, "Run returning with a handler in flight")
	// The heartbeater keeps running during the drain.
	recv(t, store.heartbeats, "heartbeat during drain")
	close(release)
	if err := recv(t, stopped, "Run to return"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	c := recv(t, store.completes, "complete")
	if c.ID == (job.ID{}) {
		t.Fatal("complete not recorded")
	}
	if st := p.Stats(); st.Succeeded != 1 || st.InFlight != 0 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestDrainBudgetCancelsStragglers(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	started := make(chan struct{}, 1)
	reg := registry(t, "hang", job.HandlerFunc(func(ctx context.Context, _ job.Job) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}))
	store.add(newJob("hang"))
	cfg := fastConfig()
	cfg.JobTimeout = 40 * time.Millisecond
	p := NewPool(store, nil, reg, nil, nil, cfg)
	stop := startPool(t, p)

	recv(t, started, "start")
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	f := recv(t, store.fails, "fail")
	if f.Status != job.StatusQueued {
		t.Fatalf("fail = %+v", f)
	}
}

func TestRunWithoutQueuesFails(t *testing.T) {
	t.Parallel()
	p := NewPool(newFakeStore(), nil, job.NewRegistry(), nil, nil, Config{})
	if err := p.Run(context.Background()); !errors.Is(err, ErrNoQueues) {
		t.Fatalf("Run = %v, want ErrNoQueues", err)
	}
}

func TestConfigDefaults(t *testing.T) {
	t.Parallel()
	c := Config{}.withDefaults()
	if c.Concurrency != DefaultConcurrency || c.BatchSize != DefaultBatchSize || c.LeaseTTL != DefaultLeaseTTL ||
		c.HeartbeatEvery != DefaultHeartbeatEvery || c.JobTimeout != DefaultJobTimeout || c.PollFloor != DefaultPollFloor {
		t.Fatalf("defaults = %+v", c)
	}
	if c.Owner == "" || !strings.Contains(c.Owner, "-") {
		t.Fatalf("owner = %q", c.Owner)
	}
	c = Config{LeaseTTL: 3 * time.Second, HeartbeatEvery: 10 * time.Second}.withDefaults()
	if c.HeartbeatEvery != time.Second {
		t.Fatalf("HeartbeatEvery = %v, want LeaseTTL/3", c.HeartbeatEvery)
	}
}
