// Package jobtest holds in-memory test doubles shared by the WorkerService
// server tests and the pkg/jobworker client tests. It is imported only from
// _test.go files.
package jobtest

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/crowbett/jobrun/internal/job"
)

// Call records one terminal call (Complete or Fail) against the store.
type Call struct {
	Op    string // "complete" | "fail"
	Lease job.Lease
	Owner string
	Cause string
	Perm  bool
	Err   error // what the store answered
}

// Leases is an in-memory lease store with the Postgres semantics that matter
// to a worker: claims bump attempts and the fencing token, Heartbeat/Complete/
// Fail are guarded by (owner, token, leased), cancel_requested is honoured.
// Steal simulates the reaper handing a job to another worker.
type Leases struct {
	mu     sync.Mutex
	jobs   map[job.ID]*job.Job
	order  []job.ID
	hbErr  error
	Calls  chan Call
	Claims chan job.ClaimQuery
}

// NewLeases returns an empty store.
func NewLeases() *Leases {
	return &Leases{jobs: make(map[job.ID]*job.Job), Calls: make(chan Call, 4096), Claims: make(chan job.ClaimQuery, 4096)}
}

// Add enqueues js (queued, attempts 0).
func (l *Leases) Add(js ...job.Job) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := range js {
		j := js[i]
		j.Status = job.StatusQueued
		if j.MaxAttempts == 0 {
			j.MaxAttempts = 3
		}
		if j.Backoff.IsZero() {
			j.Backoff = job.Policy{Base: time.Millisecond, Max: time.Millisecond, Factor: 1}
		}
		l.jobs[j.ID] = &j
		l.order = append(l.order, j.ID)
	}
}

// Job returns a copy of the stored job.
func (l *Leases) Job(id job.ID) job.Job {
	l.mu.Lock()
	defer l.mu.Unlock()
	return *l.jobs[id]
}

// Steal simulates the reaper requeueing a leased job and another worker
// claiming it at once: the job stays leased, to "remote:thief", with a newer
// fencing token, so the current lease is lost.
func (l *Leases) Steal(id job.ID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	j := l.jobs[id]
	j.LeaseOwner = "remote:thief"
	j.Attempts++
	j.LeaseToken++
}

// RequestCancel flags a leased job cancel_requested.
func (l *Leases) RequestCancel(id job.ID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.jobs[id].CancelRequested = true
}

// SetHeartbeatError makes every Heartbeat fail with err (nil restores).
func (l *Leases) SetHeartbeatError(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.hbErr = err
}

// Claim leases up to q.Limit queued jobs on q.Queue in insertion order.
func (l *Leases) Claim(_ context.Context, q job.ClaimQuery) ([]job.Job, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case l.Claims <- q:
	default:
	}
	var out []job.Job
	for _, id := range l.order {
		if len(out) >= int(q.Limit) {
			break
		}
		j := l.jobs[id]
		if j.Queue != q.Queue || j.Status != job.StatusQueued {
			continue
		}
		j.Status, j.LeaseOwner = job.StatusLeased, q.Owner
		j.Attempts++
		j.LeaseToken++
		out = append(out, *j)
	}
	return out, nil
}

func (l *Leases) held(le job.Lease, owner string) (*job.Job, bool) {
	j, ok := l.jobs[le.ID]
	return j, ok && j.Status == job.StatusLeased && j.LeaseOwner == owner && j.LeaseToken == le.Token
}

// Heartbeat reports held leases as kept or cancel-requested.
func (l *Leases) Heartbeat(_ context.Context, owner string, leases []job.Lease, _ time.Time) (job.Renewal, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.hbErr != nil {
		return job.Renewal{}, l.hbErr
	}
	r := job.Renewal{Kept: []job.Lease{}, CancelRequested: []job.Lease{}}
	for _, le := range leases {
		j, ok := l.held(le, owner)
		switch {
		case !ok:
		case j.CancelRequested:
			r.CancelRequested = append(r.CancelRequested, le)
		default:
			r.Kept = append(r.Kept, le)
		}
	}
	return r, nil
}

// Complete finishes a held lease (cancelled when a cancel was requested).
func (l *Leases) Complete(_ context.Context, le job.Lease, owner string, _ job.Commit) error {
	l.mu.Lock()
	j, ok := l.held(le, owner)
	var err error
	if !ok {
		err = job.ErrLeaseLost
	} else {
		j.Status, j.LeaseOwner = job.StatusSucceeded, ""
		if j.CancelRequested {
			j.Status = job.StatusCancelled
		}
	}
	l.mu.Unlock()
	l.Calls <- Call{Op: "complete", Lease: le, Owner: owner, Err: err}
	return err
}

// Fail records a failure: cancelled, dead (perm or attempts exhausted) or queued.
func (l *Leases) Fail(_ context.Context, le job.Lease, owner, cause string, perm bool, _ time.Time) (job.Status, error) {
	l.mu.Lock()
	j, ok := l.held(le, owner)
	var (
		err error
		st  job.Status
	)
	if !ok {
		err = job.ErrLeaseLost
	} else {
		switch {
		case j.CancelRequested:
			st = job.StatusCancelled
		case perm || j.Attempts >= j.MaxAttempts:
			st = job.StatusDead
		default:
			st = job.StatusQueued
		}
		j.Status, j.LeaseOwner, j.LastError = st, "", cause
	}
	l.mu.Unlock()
	l.Calls <- Call{Op: "fail", Lease: le, Owner: owner, Cause: cause, Perm: perm, Err: err}
	return st, err
}

// Get returns a stored job or job.ErrNotFound.
func (l *Leases) Get(_ context.Context, id job.ID) (job.Job, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	j, ok := l.jobs[id]
	if !ok {
		return job.Job{}, job.ErrNotFound
	}
	return *j, nil
}

// ErrDown is a convenient heartbeat failure.
var ErrDown = errors.New("jobtest: store unreachable")
