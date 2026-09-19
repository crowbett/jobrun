package reaper

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/crowbett/jobrun/internal/job"
)

// fakeStore answers ReapExpired from a queue of batches and counts Purge calls.
type fakeStore struct {
	job.Store

	mu       sync.Mutex
	batches  [][]job.Event // consumed one per ReapExpired call; empty afterwards
	reapErr  error
	reaps    []reapCall
	purges   []job.PurgeQuery
	purgeN   []int64 // consumed one per Purge call; 0 afterwards
	purgeErr error
}

type reapCall struct {
	Now    time.Time
	Limit  int32
	Policy job.Policy
}

func (f *fakeStore) ReapExpired(_ context.Context, now time.Time, limit int32, p job.Policy) ([]job.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reaps = append(f.reaps, reapCall{Now: now, Limit: limit, Policy: p})
	if f.reapErr != nil {
		return nil, f.reapErr
	}
	if len(f.batches) == 0 {
		return nil, nil
	}
	b := f.batches[0]
	f.batches = f.batches[1:]
	return b, nil
}

func (f *fakeStore) Purge(_ context.Context, q job.PurgeQuery) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.purges = append(f.purges, q)
	if f.purgeErr != nil {
		return 0, f.purgeErr
	}
	if len(f.purgeN) == 0 {
		return 0, nil
	}
	n := f.purgeN[0]
	f.purgeN = f.purgeN[1:]
	return n, nil
}

func (f *fakeStore) counts() (reaps, purges int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reaps), len(f.purges)
}

type fakePub struct {
	mu     sync.Mutex
	events []job.Event
}

func (p *fakePub) Publish(e job.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, e)
}

func (p *fakePub) all() []job.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]job.Event(nil), p.events...)
}

func event(to job.Status) job.Event {
	return job.Event{JobID: job.NewID(), Queue: "q", Type: "t", From: job.StatusLeased, To: to, Attempt: 2}
}

func TestReapPublishesEvents(t *testing.T) {
	t.Parallel()
	want := []job.Event{event(job.StatusQueued), event(job.StatusDead), event(job.StatusQueued)}
	store := &fakeStore{batches: [][]job.Event{want}}
	pub := &fakePub{}
	now := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)
	pol := job.Policy{Base: 2 * time.Second, Max: time.Minute, Factor: 3, Jitter: true}
	r := NewReaper(store, pub, nil, Config{LeaseTTL: time.Second, Batch: 100, Policy: pol})
	r.now = func() time.Time { return now }

	r.reap(context.Background())

	got := pub.all()
	if len(got) != len(want) {
		t.Fatalf("published %d events, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.reaps) != 1 || !store.reaps[0].Now.Equal(now) || store.reaps[0].Limit != 100 || store.reaps[0].Policy != pol {
		t.Fatalf("reaps = %+v", store.reaps)
	}
	if st := r.Stats(); st.Reaped != 3 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestReapDrainsFullBatches(t *testing.T) {
	t.Parallel()
	full := []job.Event{event(job.StatusQueued), event(job.StatusQueued)}
	store := &fakeStore{batches: [][]job.Event{full, full, {event(job.StatusDead)}}}
	pub := &fakePub{}
	r := NewReaper(store, pub, nil, Config{Batch: 2})
	r.reap(context.Background())
	if n := len(pub.all()); n != 5 {
		t.Fatalf("published %d, want 5 (two full batches and a short one)", n)
	}
	if reaps, _ := store.counts(); reaps != 3 {
		t.Fatalf("ReapExpired called %d times, want 3 (stops on the short batch)", reaps)
	}
}

func TestReapErrorIsLoggedNotFatal(t *testing.T) {
	t.Parallel()
	store := &fakeStore{reapErr: errors.New("db down")}
	r := NewReaper(store, nil, nil, Config{})
	r.reap(context.Background())
	if reaps, _ := store.counts(); reaps != 1 {
		t.Fatalf("ReapExpired called %d times, want 1", reaps)
	}
}

func TestPurgeQueryAndDrain(t *testing.T) {
	t.Parallel()
	store := &fakeStore{purgeN: []int64{10, 10, 3}}
	r := NewReaper(store, nil, nil, Config{Batch: 10, Retention: 48 * time.Hour})
	r.purge(context.Background())
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.purges) != 3 {
		t.Fatalf("Purge called %d times, want 3", len(store.purges))
	}
	q := store.purges[0]
	if q.OlderThan != 48*time.Hour || q.Limit != 10 || q.Queue != "" {
		t.Fatalf("purge query = %+v", q)
	}
	wantStatuses := map[job.Status]bool{job.StatusSucceeded: true, job.StatusDead: true, job.StatusCancelled: true}
	if len(q.Statuses) != 3 {
		t.Fatalf("statuses = %v", q.Statuses)
	}
	for _, s := range q.Statuses {
		if !wantStatuses[s] {
			t.Fatalf("unexpected purge status %s", s)
		}
	}
	if st := r.Stats(); st.Purged != 23 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestRunCadence(t *testing.T) {
	t.Parallel()
	store := &fakeStore{}
	r := NewReaper(store, nil, nil, Config{LeaseTTL: 6 * time.Millisecond}) // tick 2ms
	if r.Interval() != 2*time.Millisecond {
		t.Fatalf("interval = %v", r.Interval())
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		reaps, purges := store.counts()
		if reaps >= 20 && purges >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after 5s: reaps=%d purges=%d, want >=20 and >=2", reaps, purges)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	reaps, purges := store.counts()
	// Every purgeEvery-th tick purges: purges is reaps/10 give or take the tick
	// that was in progress at cancel.
	if lo, hi := reaps/purgeEvery-1, reaps/purgeEvery+1; purges < lo || purges > hi {
		t.Fatalf("reaps=%d purges=%d, want purges ~ reaps/%d", reaps, purges, purgeEvery)
	}
	if st := r.Stats(); st.Ticks < 20 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestDefaults(t *testing.T) {
	t.Parallel()
	c := Config{}.withDefaults()
	if c.LeaseTTL != DefaultLeaseTTL || c.Retention != DefaultRetention || c.Batch != DefaultBatch || c.Policy != job.DefaultPolicy {
		t.Fatalf("defaults = %+v", c)
	}
	r := NewReaper(&fakeStore{}, nil, nil, Config{})
	if r.Interval() != 10*time.Second {
		t.Fatalf("interval = %v, want 10s", r.Interval())
	}
}
