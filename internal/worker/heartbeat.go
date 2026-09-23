package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/crowbett/jobrun/internal/job"
)

// leaseState is what the heartbeater knows about one in-flight lease.
type leaseState uint8

const (
	// leaseHeld: renewed (or not yet due); the handler may finish normally.
	leaseHeld leaseState = iota
	// leaseCancelRequested: still held, but the job was cancelled. The handler
	// context is cancelled and the terminal Fail/Complete still goes through
	// (the store ends the job cancelled).
	leaseCancelRequested
	// leaseFenced: the lease is gone (expired, reaped, reclaimed with a newer
	// token) or could not be renewed before it would expire. The handler
	// context is cancelled and the pool never calls Complete or Fail.
	leaseFenced
)

// heartbeater extends the leases of every in-flight job on a ticker and fences
// the ones the store no longer keeps.
//
// Fencing is prompt in both directions a lease can die: a heartbeat whose
// verdict omits a lease cancels that handler on the same tick (at most
// HeartbeatEvery after the store dropped it), and a lease that could not be
// renewed - the store is unreachable - is fenced locally once its deadline
// passes. The local deadline is conservative: it counts from the moment the
// claim or the last successful heartbeat was *sent* (the database stamped
// lease_until after that) minus one heartbeat period, on the monotonic app
// clock, so the handler stops before the reaper can hand the job to someone
// else.
type heartbeater struct {
	store        job.Store
	owner        string
	every        time.Duration
	ttl          time.Duration
	storeTimeout time.Duration
	log          *slog.Logger
	now          func() time.Time
	onFence      func() // counts fenced leases; may be nil

	// inflight is keyed by lease, not job id: after its own lease was reaped
	// the pool may reclaim the same job (a new token) while the fenced first
	// attempt is still unwinding.
	mu       sync.Mutex
	inflight map[job.Lease]*inflightJob
}

type inflightJob struct {
	lease    job.Lease
	cancel   context.CancelFunc
	state    leaseState
	deadline time.Time // local: fence when a tick finds now >= deadline without a renewal
}

func newHeartbeater(s job.Store, owner string, every, ttl, storeTimeout time.Duration, log *slog.Logger, now func() time.Time) *heartbeater {
	return &heartbeater{
		store:        s,
		owner:        owner,
		every:        every,
		ttl:          ttl,
		storeTimeout: storeTimeout,
		log:          log,
		now:          now,
		inflight:     make(map[job.Lease]*inflightJob),
	}
}

// safeUntil is the local deadline of a lease stamped no earlier than sentAt.
func (h *heartbeater) safeUntil(sentAt time.Time) time.Time {
	return sentAt.Add(h.ttl - h.every)
}

// add tracks a lease claimed by a request sent at claimedAt.
func (h *heartbeater) add(l job.Lease, cancel context.CancelFunc, claimedAt time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.inflight[l] = &inflightJob{lease: l, cancel: cancel, deadline: h.safeUntil(claimedAt)}
}

func (h *heartbeater) remove(l job.Lease) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.inflight, l)
}

// state reports what the heartbeater knows about l (leaseHeld when unknown).
func (h *heartbeater) state(l job.Lease) leaseState {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e, ok := h.inflight[l]; ok {
		return e.state
	}
	return leaseHeld
}

// renewable returns the leases still worth heartbeating: held and
// cancel-requested ones (the latter until their Fail lands), never fenced ones.
func (h *heartbeater) renewable() []job.Lease {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]job.Lease, 0, len(h.inflight))
	for _, e := range h.inflight {
		if e.state != leaseFenced {
			out = append(out, e.lease)
		}
	}
	return out
}

// fence cancels l's handler and marks it fenced; a no-op if the job already
// finished or was fenced. Reports whether it fenced.
func (h *heartbeater) fence(l job.Lease) bool {
	h.mu.Lock()
	e, ok := h.inflight[l]
	if !ok || e.state == leaseFenced {
		h.mu.Unlock()
		return false
	}
	e.state = leaseFenced
	e.cancel()
	h.mu.Unlock()
	if h.onFence != nil {
		h.onFence()
	}
	return true
}

// cancelRequested cancels l's handler but keeps the lease renewable.
func (h *heartbeater) cancelRequested(l job.Lease, deadline time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.inflight[l]
	if !ok || e.state == leaseFenced {
		return false
	}
	e.deadline = deadline
	if e.state == leaseCancelRequested {
		return false
	}
	e.state = leaseCancelRequested
	e.cancel()
	return true
}

// renewed moves l's local deadline forward after a successful heartbeat.
func (h *heartbeater) renewed(l job.Lease, deadline time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e, ok := h.inflight[l]; ok && e.state != leaseFenced {
		e.deadline = deadline
	}
}

// expired returns the renewable leases whose local deadline has passed.
func (h *heartbeater) expired(now time.Time) []job.Lease {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []job.Lease
	for l, e := range h.inflight {
		if e.state != leaseFenced && !now.Before(e.deadline) {
			out = append(out, l)
		}
	}
	return out
}

// cancelAll cancels every in-flight handler (drain budget exceeded) without
// fencing them: their leases are still ours, so Fail may still land.
func (h *heartbeater) cancelAll() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, e := range h.inflight {
		e.cancel()
	}
	return len(h.inflight)
}

// run ticks until ctx is done. It must outlive the claim loops: a draining pool
// still has handlers whose leases need extending.
func (h *heartbeater) run(ctx context.Context) {
	t := time.NewTicker(h.every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.tick(ctx)
		}
	}
}

func (h *heartbeater) tick(ctx context.Context) {
	leases := h.renewable()
	if len(leases) == 0 {
		return
	}
	hctx, cancel := context.WithTimeout(ctx, h.storeTimeout)
	defer cancel()
	sentAt := h.now()
	// until is app-clock based: the store only stores it, every comparison
	// against it happens on the database clock (docs/PLAN.md, Risks).
	r, err := h.store.Heartbeat(hctx, h.owner, leases, sentAt.Add(h.ttl))
	if err != nil {
		// The leases stay valid until lease_until; the next tick retries, and
		// anything whose local deadline passed is fenced now.
		h.log.Error("worker: heartbeat failed", "in_flight", len(leases), "err", err)
		for _, l := range h.expired(h.now()) {
			if h.fence(l) {
				h.log.Warn("worker: lease could not be renewed before expiry, fencing handler", "job", l.ID, "token", l.Token)
			}
		}
		return
	}
	deadline := h.safeUntil(sentAt)
	seen := make(map[job.Lease]struct{}, len(r.Kept)+len(r.CancelRequested))
	for _, l := range r.Kept {
		seen[l] = struct{}{}
		h.renewed(l, deadline)
	}
	for _, l := range r.CancelRequested {
		seen[l] = struct{}{}
		if h.cancelRequested(l, deadline) {
			h.log.Info("worker: cancel requested, stopping handler", "job", l.ID)
		}
	}
	for _, l := range leases {
		if _, ok := seen[l]; ok {
			continue
		}
		if h.fence(l) {
			h.log.Warn("worker: lease lost, fencing handler (it will not be completed)", "job", l.ID, "token", l.Token)
		}
	}
}
