package worker

import (
	"context"
	"sync"
	"time"

	"github.com/crowbett/jobrun/internal/job"
)

// fakeStore is an in-memory job.Store with controllable claims, heartbeats and
// lease loss. Methods the pool never calls are left to the embedded nil
// interface and panic if reached, which is the point.
type fakeStore struct {
	job.Store

	mu        sync.Mutex
	ready     map[string][]job.Job // claimable per queue, FIFO
	claims    []job.ClaimQuery
	claimErrs int                // claims left that fail with errClaim
	drop      map[job.ID]bool    // heartbeat reports these lost
	lost      map[job.ID]bool    // Complete/Fail answer ErrLeaseLost
	cancelled map[job.ID]bool    // heartbeat reports cancel requested; Fail answers StatusCancelled
	dropLease map[job.Lease]bool // heartbeat reports this one lease lost
	hbErr     error              // every heartbeat fails with this when set

	completes  chan completeCall
	fails      chan failCall
	heartbeats chan heartbeatCall
}

type completeCall struct {
	ID     job.ID
	Token  int64
	Owner  string
	Commit job.Commit
}

type failCall struct {
	ID      job.ID
	Token   int64
	Owner   string
	Cause   string
	Perm    bool
	RetryAt time.Time
	Status  job.Status
}

type heartbeatCall struct {
	Owner   string
	IDs     []job.ID
	Tokens  []int64
	Until   time.Time
	Kept    []job.Lease
	Cancels []job.Lease
	Err     error
}

var errClaim = claimError{}

type claimError struct{}

func (claimError) Error() string { return "claim: boom" }

func newFakeStore() *fakeStore {
	return &fakeStore{
		ready:      make(map[string][]job.Job),
		drop:       make(map[job.ID]bool),
		dropLease:  make(map[job.Lease]bool),
		lost:       make(map[job.ID]bool),
		cancelled:  make(map[job.ID]bool),
		completes:  make(chan completeCall, 1024),
		fails:      make(chan failCall, 1024),
		heartbeats: make(chan heartbeatCall, 4096),
	}
}

// add makes j claimable on its queue.
func (f *fakeStore) add(js ...job.Job) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range js {
		f.ready[js[i].Queue] = append(f.ready[js[i].Queue], js[i])
	}
}

func (f *fakeStore) claimQueries() []job.ClaimQuery {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]job.ClaimQuery, len(f.claims))
	copy(out, f.claims)
	return out
}

func (f *fakeStore) Claim(_ context.Context, q job.ClaimQuery) ([]job.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims = append(f.claims, q)
	if f.claimErrs > 0 {
		f.claimErrs--
		return nil, errClaim
	}
	pending := f.ready[q.Queue]
	n := min(int(q.Limit), len(pending))
	out := make([]job.Job, 0, n)
	until := time.Now().Add(q.LeaseTTL)
	for i := range pending[:n] {
		j := pending[i]
		j.Status = job.StatusLeased
		j.LeaseOwner = q.Owner
		j.LeaseUntil = &until
		j.Attempts++
		j.LeaseToken++
		out = append(out, j)
	}
	f.ready[q.Queue] = pending[n:]
	return out, nil
}

func (f *fakeStore) Heartbeat(_ context.Context, owner string, leases []job.Lease, until time.Time) (job.Renewal, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	call := heartbeatCall{Owner: owner, Until: until, Err: f.hbErr}
	for _, l := range leases {
		call.IDs = append(call.IDs, l.ID)
		call.Tokens = append(call.Tokens, l.Token)
	}
	if f.hbErr != nil {
		f.heartbeats <- call
		return job.Renewal{}, f.hbErr
	}
	for _, l := range leases {
		switch {
		case f.cancelled[l.ID]:
			call.Cancels = append(call.Cancels, l)
		case !f.drop[l.ID] && !f.dropLease[l]:
			call.Kept = append(call.Kept, l)
		}
	}
	f.heartbeats <- call
	return job.Renewal{Kept: call.Kept, CancelRequested: call.Cancels}, nil
}

func (f *fakeStore) Complete(_ context.Context, l job.Lease, owner string, fn job.Commit) error {
	id := l.ID
	f.mu.Lock()
	lost := f.lost[id]
	f.mu.Unlock()
	f.completes <- completeCall{ID: id, Token: l.Token, Owner: owner, Commit: fn}
	if lost {
		return job.ErrLeaseLost
	}
	return nil
}

func (f *fakeStore) Fail(_ context.Context, l job.Lease, owner, cause string, perm bool, retryAt time.Time) (job.Status, error) {
	id := l.ID
	f.mu.Lock()
	lost, cancelled := f.lost[id], f.cancelled[id]
	f.mu.Unlock()
	status := job.StatusQueued
	switch {
	case lost:
		status = job.StatusUnspecified
	case cancelled:
		status = job.StatusCancelled
	case perm:
		status = job.StatusDead
	}
	f.fails <- failCall{ID: id, Token: l.Token, Owner: owner, Cause: cause, Perm: perm, RetryAt: retryAt, Status: status}
	if lost {
		return job.StatusUnspecified, job.ErrLeaseLost
	}
	return status, nil
}

// fakePub records events in order.
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
	out := make([]job.Event, len(p.events))
	copy(out, p.events)
	return out
}

// chanDispatcher wakes the claim loop only when the test says so.
type chanDispatcher struct {
	ch chan struct{}
}

func (chanDispatcher) Ready(context.Context, string) error { return nil }

func (d chanDispatcher) Wait(context.Context, string) <-chan struct{} { return d.ch }
