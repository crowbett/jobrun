package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/crowbett/jobrun/internal/job"
)

// heartbeater extends the leases of every in-flight job on a ticker and fences
// the ones the store no longer keeps: an id missing from the kept set (lease
// expired and reaped, stolen, or cancel requested) gets its handler context
// cancelled and is marked dropped so the pool can name the cause.
type heartbeater struct {
	store        job.Store
	owner        string
	every        time.Duration
	ttl          time.Duration
	storeTimeout time.Duration
	log          *slog.Logger
	now          func() time.Time

	mu       sync.Mutex
	inflight map[job.ID]*inflightJob
}

type inflightJob struct {
	cancel  context.CancelFunc
	dropped bool
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
		inflight:     make(map[job.ID]*inflightJob),
	}
}

func (h *heartbeater) add(id job.ID, cancel context.CancelFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.inflight[id] = &inflightJob{cancel: cancel}
}

func (h *heartbeater) remove(id job.ID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.inflight, id)
}

// dropped reports whether the heartbeater fenced id (and removes nothing).
func (h *heartbeater) dropped(id job.ID) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.inflight[id]
	return ok && e.dropped
}

func (h *heartbeater) ids() []job.ID {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]job.ID, 0, len(h.inflight))
	for id, e := range h.inflight {
		if !e.dropped {
			out = append(out, id)
		}
	}
	return out
}

// drop cancels id's handler and marks it; a no-op if the job already finished.
func (h *heartbeater) drop(id job.ID) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.inflight[id]
	if !ok || e.dropped {
		return false
	}
	e.dropped = true
	e.cancel()
	return true
}

// cancelAll cancels every in-flight handler (drain budget exceeded) without
// marking them dropped: their leases are still ours, so Fail may still land.
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
	ids := h.ids()
	if len(ids) == 0 {
		return
	}
	hctx, cancel := context.WithTimeout(ctx, h.storeTimeout)
	defer cancel()
	// until is app-clock based: the store only stores it, every comparison
	// against it happens on the database clock (docs/PLAN.md, Risks).
	kept, err := h.store.Heartbeat(hctx, h.owner, ids, h.now().Add(h.ttl))
	if err != nil {
		// The leases stay valid until lease_until; the next tick retries.
		h.log.Error("worker: heartbeat failed", "in_flight", len(ids), "err", err)
		return
	}
	keptSet := make(map[job.ID]struct{}, len(kept))
	for _, id := range kept {
		keptSet[id] = struct{}{}
	}
	for _, id := range ids {
		if _, ok := keptSet[id]; ok {
			continue
		}
		if h.drop(id) {
			h.log.Warn("worker: lease lost or cancel requested, cancelling handler", "job", id)
		}
	}
}
