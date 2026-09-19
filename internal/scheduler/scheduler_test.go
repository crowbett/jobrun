package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/crowbett/jobrun/internal/cronx"
	"github.com/crowbett/jobrun/internal/job"
)

// fakeStore covers the Schedules half plus Enqueue; anything else panics.
type fakeStore struct {
	job.Store

	mu        sync.Mutex
	schedules map[job.ID]job.Schedule
	jobs      map[string]job.Job // by idempotency key
	enqueues  int
	advances  []advanceCall
	updates   []job.Schedule
}

type advanceCall struct {
	ID      job.ID
	LastRun time.Time
	NextRun time.Time
}

func newFakeStore() *fakeStore {
	return &fakeStore{schedules: make(map[job.ID]job.Schedule), jobs: make(map[string]job.Job)}
}

func (f *fakeStore) put(s job.Schedule) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.schedules[s.ID] = s
}

func (f *fakeStore) get(id job.ID) job.Schedule {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.schedules[id]
}

func (f *fakeStore) DueSchedules(_ context.Context, now time.Time, limit int32) ([]job.Schedule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []job.Schedule
	for id := range f.schedules {
		s := f.schedules[id]
		if s.Enabled && !s.NextRun.After(now) && int32(len(out)) < limit {
			out = append(out, s)
		}
	}
	return out, nil
}

func (f *fakeStore) AdvanceSchedule(_ context.Context, id job.ID, lastRun, nextRun time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.schedules[id]
	if !ok {
		return job.ErrScheduleNotFound
	}
	lr := lastRun
	s.LastRun, s.NextRun = &lr, nextRun
	f.schedules[id] = s
	f.advances = append(f.advances, advanceCall{ID: id, LastRun: lastRun, NextRun: nextRun})
	return nil
}

func (f *fakeStore) UpdateSchedule(_ context.Context, s job.Schedule) (job.Schedule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur, ok := f.schedules[s.ID]
	if !ok {
		return job.Schedule{}, job.ErrScheduleNotFound
	}
	cur.Cron, cur.Timezone, cur.Template, cur.Enabled, cur.NextRun = s.Cron, s.Timezone, s.Template, s.Enabled, s.NextRun
	f.schedules[s.ID] = cur
	f.updates = append(f.updates, cur)
	return cur, nil
}

func (f *fakeStore) Enqueue(_ context.Context, j job.Job) (job.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enqueues++
	if j.IdempotencyKey != "" {
		if stored, ok := f.jobs[j.IdempotencyKey]; ok {
			return stored, nil
		}
	}
	f.jobs[j.IdempotencyKey] = j
	return j, nil
}

// memLocker is one advisory lock shared by every scheduler in a test.
type memLocker struct {
	mu     sync.Mutex
	holder int
	next   int
	fail   error
}

func (l *memLocker) TryLock(_ context.Context, _ int64) (func(context.Context) error, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fail != nil {
		return nil, false, l.fail
	}
	if l.holder != 0 {
		return nil, false, nil
	}
	l.next++
	me := l.next
	l.holder = me
	release := func(context.Context) error {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.holder != me {
			return errors.New("release: not the holder")
		}
		l.holder = 0
		return nil
	}
	return release, true, nil
}

func (l *memLocker) held() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.holder != 0
}

func schedule(cron, tz string, nextRun time.Time) job.Schedule {
	return job.Schedule{
		ID:       job.NewID(),
		Name:     "nightly",
		Cron:     cron,
		Timezone: tz,
		Template: job.NewJob{Queue: "default", Type: "noop", Payload: []byte(`{"k":1}`), MaxAttempts: 3},
		Enabled:  true,
		NextRun:  nextRun,
	}
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestExactlyOneLeader(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	lk := &memLocker{}
	cfg := Config{Tick: 5 * time.Millisecond}
	a := NewScheduler(store, lk, nil, cfg)
	b := NewScheduler(store, lk, nil, cfg)

	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	doneA, doneB := make(chan error, 1), make(chan error, 1)
	go func() { doneA <- a.Run(ctxA) }()
	go func() { doneB <- b.Run(ctxB) }()

	waitFor(t, func() bool { return a.Leader() || b.Leader() }, "a leader")
	time.Sleep(30 * time.Millisecond) // several ticks of both
	if a.Leader() == b.Leader() {
		t.Fatalf("leaders: a=%v b=%v, want exactly one", a.Leader(), b.Leader())
	}
	leader, follower, cancelLeader, doneLeader := a, b, cancelA, doneA
	cancelFollower, doneFollower := cancelB, doneB
	if b.Leader() {
		leader, follower, cancelLeader, doneLeader = b, a, cancelB, doneB
		cancelFollower, doneFollower = cancelA, doneA
	}
	if follower.Ticks() != 0 {
		t.Fatalf("follower ticked %d times", follower.Ticks())
	}

	// Stopping the leader releases the lock and the follower takes over.
	cancelLeader()
	if err := <-doneLeader; err != nil {
		t.Fatalf("leader Run: %v", err)
	}
	if leader.Leader() {
		t.Fatal("stopped scheduler still claims leadership")
	}
	waitFor(t, func() bool { return follower.Leader() }, "follower to take over")
	cancelFollower()
	if err := <-doneFollower; err != nil {
		t.Fatalf("follower Run: %v", err)
	}
	if lk.held() {
		t.Fatal("lock still held after both stopped")
	}
}

func TestTickMaterialisesOneJobPerDueSchedule(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	now := time.Date(2026, 9, 19, 10, 2, 30, 0, time.UTC)
	dueA := schedule("*/5 * * * *", "UTC", time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC))
	dueB := schedule("0 9 * * *", "America/New_York", time.Date(2026, 9, 18, 13, 0, 0, 0, time.UTC)) // yesterday 09:00 EDT
	future := schedule("* * * * *", "", now.Add(time.Hour))
	store.put(dueA)
	store.put(dueB)
	store.put(future)

	s := NewScheduler(store, &memLocker{}, nil, Config{Tick: time.Hour})
	s.now = func() time.Time { return now }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }() // Run ticks once immediately after taking the lock
	waitFor(t, func() bool { return s.Ticks() == 1 }, "first tick")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.enqueues != 2 || len(store.jobs) != 2 {
		t.Fatalf("enqueues=%d distinct=%d, want 2/2", store.enqueues, len(store.jobs))
	}
	keyA := IdempotencyKey(dueA.ID, dueA.NextRun)
	jA, ok := store.jobs[keyA]
	if !ok {
		t.Fatalf("job for %s missing; have %v", keyA, keysOf(store.jobs))
	}
	if keyA != "sched:"+dueA.ID.String()+":2026-09-19T10:00:00Z" {
		t.Fatalf("key = %q", keyA)
	}
	if !jA.RunAt.Equal(dueA.NextRun) || jA.Queue != "default" || jA.Type != "noop" || jA.MaxAttempts != 3 || string(jA.Payload) != `{"k":1}` {
		t.Fatalf("job = %+v", jA)
	}
	if _, ok := store.jobs[IdempotencyKey(dueB.ID, dueB.NextRun)]; !ok {
		t.Fatal("job for the New York schedule missing")
	}

	if len(store.advances) != 2 {
		t.Fatalf("advances = %+v", store.advances)
	}
	ny, _ := time.LoadLocation("America/New_York")
	wantNext := map[job.ID]time.Time{
		dueA.ID: time.Date(2026, 9, 19, 10, 5, 0, 0, time.UTC),
		dueB.ID: time.Date(2026, 9, 19, 9, 0, 0, 0, ny),
	}
	wantLast := map[job.ID]time.Time{dueA.ID: dueA.NextRun, dueB.ID: dueB.NextRun}
	for _, a := range store.advances {
		if !a.NextRun.Equal(wantNext[a.ID]) || !a.LastRun.Equal(wantLast[a.ID]) {
			t.Fatalf("advance %+v, want last %v next %v", a, wantLast[a.ID], wantNext[a.ID])
		}
		if got := store.schedules[a.ID]; !got.NextRun.Equal(wantNext[a.ID]) || got.LastRun == nil || !got.LastRun.Equal(wantLast[a.ID]) {
			t.Fatalf("schedule after advance = %+v", got)
		}
	}
	if got := store.schedules[future.ID]; got.LastRun != nil {
		t.Fatalf("future schedule was touched: %+v", got)
	}
}

func keysOf(m map[string]job.Job) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestRepeatedTickIsIdempotent(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	now := time.Date(2026, 9, 19, 10, 2, 30, 0, time.UTC)
	sc := schedule("*/5 * * * *", "UTC", time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC))
	store.put(sc)
	s := NewScheduler(store, &memLocker{}, nil, Config{})
	s.now = func() time.Time { return now }

	s.tick(context.Background())
	// Simulate a lost advance: reset next_run and tick again.
	store.mu.Lock()
	cur := store.schedules[sc.ID]
	cur.NextRun = sc.NextRun
	store.schedules[sc.ID] = cur
	store.mu.Unlock()
	s.tick(context.Background())

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.enqueues != 2 || len(store.jobs) != 1 {
		t.Fatalf("enqueues=%d distinct=%d, want the same key reused", store.enqueues, len(store.jobs))
	}
	if got := store.schedules[sc.ID].NextRun; !got.Equal(time.Date(2026, 9, 19, 10, 5, 0, 0, time.UTC)) {
		t.Fatalf("next_run = %v", got)
	}
}

func TestBadCronDisables(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		cron, tz string
		enqueued int
	}{
		{"unparsable cron", "every day", "UTC", 0},
		{"unknown timezone", "0 0 * * *", "Mars/Olympus", 0},
		{"never fires again", "0 0 30 2 *", "UTC", 1}, // the due firing is enqueued, then no next run
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newFakeStore()
			sc := schedule(tc.cron, tc.tz, time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC))
			store.put(sc)
			s := NewScheduler(store, &memLocker{}, nil, Config{})
			s.now = func() time.Time { return time.Date(2026, 9, 19, 10, 2, 30, 0, time.UTC) }
			s.tick(context.Background())

			got := store.get(sc.ID)
			if got.Enabled {
				t.Fatalf("schedule still enabled: %+v", got)
			}
			store.mu.Lock()
			enqueues, updates := store.enqueues, len(store.updates)
			store.mu.Unlock()
			if enqueues != tc.enqueued {
				t.Fatalf("enqueues = %d, want %d", enqueues, tc.enqueued)
			}
			if updates != 1 {
				t.Fatalf("updates = %d, want 1", updates)
			}
			// A disabled schedule is no longer due.
			due, _ := store.DueSchedules(context.Background(), time.Now().Add(24*time.Hour), 10)
			if len(due) != 0 {
				t.Fatalf("disabled schedule still due: %+v", due)
			}
		})
	}
}

func TestLookaheadMaterialisesEarlyButRunsOnTime(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	now := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)
	sc := schedule("30 10 * * *", "UTC", now.Add(30*time.Minute))
	store.put(sc)
	s := NewScheduler(store, &memLocker{}, nil, Config{Lookahead: time.Hour})
	s.now = func() time.Time { return now }
	s.tick(context.Background())

	store.mu.Lock()
	defer store.mu.Unlock()
	j, ok := store.jobs[IdempotencyKey(sc.ID, sc.NextRun)]
	if !ok || !j.RunAt.Equal(sc.NextRun) {
		t.Fatalf("job = %+v ok=%v", j, ok)
	}
}

func TestLockErrorIsRetried(t *testing.T) {
	t.Parallel()
	lk := &memLocker{fail: errors.New("connection refused")}
	s := NewScheduler(newFakeStore(), lk, nil, Config{Tick: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	time.Sleep(20 * time.Millisecond)
	if s.Leader() {
		t.Fatal("leader despite lock errors")
	}
	lk.mu.Lock()
	lk.fail = nil
	lk.mu.Unlock()
	waitFor(t, s.Leader, "leadership after the lock recovers")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestParseCacheAndDefaults(t *testing.T) {
	t.Parallel()
	s := NewScheduler(newFakeStore(), &memLocker{}, nil, Config{})
	if s.cfg.Tick != DefaultTick || s.cfg.LockKey != DefaultLockKey || s.cfg.Batch != DefaultBatch {
		t.Fatalf("defaults = %+v", s.cfg)
	}
	e1, err := s.parse("@daily", "")
	if err != nil || e1.loc != time.UTC || e1.spec.IsZero() {
		t.Fatalf("parse: %+v %v", e1, err)
	}
	e2, _ := s.parse("@daily", "")
	if e1 != e2 || len(s.cache) != 1 {
		t.Fatalf("cache miss on the same expression")
	}
	if _, err := s.parse("nope", ""); !errors.Is(err, cronx.ErrSyntax) {
		t.Fatalf("err = %v, want ErrSyntax", err)
	}
}
