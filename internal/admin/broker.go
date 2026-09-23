// Package admin is the operator surface of jobrund: the Broker that fans job
// transitions out to subscribers, the /admin/events SSE feed, /admin/stats,
// the hand-written /metrics text exposer, the depth cache the two read from,
// and the embedded /admin page.
//
// The package imports internal/job and web only; the worker, scheduler and
// reaper reach it through the small interfaces they define themselves
// (worker.EventPublisher is satisfied by *Broker, DurationObserver by
// *Histogram).
package admin

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/crowbett/jobrun/internal/job"
)

// JobKey labels one jobrun_jobs_total series.
type JobKey struct {
	Queue string
	// Status is the outcome: succeeded, dead, cancelled or retried.
	Status string
}

// Outcome names counted by the Broker.
const (
	OutcomeSucceeded = "succeeded"
	OutcomeDead      = "dead"
	OutcomeCancelled = "cancelled"
	OutcomeRetried   = "retried"
)

// Counters is what /metrics renders as counters. Jobs and Dead are fed by the
// Broker from the events it sees; LeaseExpired and SchedulerTicks come from the
// runtime (obs.Snapshot) and are filled in by the func the integrator wires.
type Counters struct {
	Jobs           map[JobKey]uint64
	Dead           uint64
	LeaseExpired   uint64
	SchedulerTicks uint64
	// LeaseFenced counts handlers whose lease a heartbeat found gone (in-process
	// pool plus leases WorkerService reported lost to remote workers).
	LeaseFenced uint64
}

// Broker fans job.Event values out to subscribers. Publish never blocks: a
// subscriber whose buffer is full misses the event and the drop is counted.
// It also tallies outcomes per queue for /metrics.
type Broker struct {
	log *slog.Logger

	mu      sync.RWMutex
	subs    map[uint64]*subscriber
	nextID  uint64
	tally   map[JobKey]uint64
	dead    uint64
	dropped atomic.Uint64
}

type subscriber struct {
	ch      chan job.Event
	dropped atomic.Uint64
}

// NewBroker builds an empty broker. A nil logger discards.
func NewBroker(log *slog.Logger) *Broker {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Broker{log: log, subs: make(map[uint64]*subscriber), tally: make(map[JobKey]uint64)}
}

// Publish delivers e to every subscriber that has room and counts the
// outcome. It never blocks and is safe from any goroutine.
func (b *Broker) Publish(e job.Event) {
	b.mu.Lock()
	if k, ok := outcomeOf(e); ok {
		b.tally[k]++
		if k.Status == OutcomeDead {
			b.dead++
		}
	}
	for _, s := range b.subs {
		select {
		case s.ch <- e:
		default:
			n := s.dropped.Add(1)
			b.dropped.Add(1)
			if n == 1 || n%1000 == 0 {
				b.log.Warn("admin broker: slow subscriber, dropping events", slog.Uint64("dropped", n))
			}
		}
	}
	b.mu.Unlock()
}

// outcomeOf maps a transition to the outcome it counts as, if any.
func outcomeOf(e job.Event) (JobKey, bool) {
	switch {
	case e.To == job.StatusSucceeded:
		return JobKey{e.Queue, OutcomeSucceeded}, true
	case e.To == job.StatusDead:
		return JobKey{e.Queue, OutcomeDead}, true
	case e.To == job.StatusCancelled:
		return JobKey{e.Queue, OutcomeCancelled}, true
	case e.From == job.StatusLeased && e.To == job.StatusQueued:
		return JobKey{e.Queue, OutcomeRetried}, true
	}
	return JobKey{}, false
}

// Subscribe registers a subscriber with a buffer of buf events (min 1) and
// returns its channel and a cancel func. cancel is idempotent; after it
// returns the channel is closed and Publish no longer sees the subscriber.
func (b *Broker) Subscribe(buf int) (<-chan job.Event, func()) {
	if buf < 1 {
		buf = 1
	}
	s := &subscriber{ch: make(chan job.Event, buf)}
	b.mu.Lock()
	id := b.nextID
	b.nextID++
	b.subs[id] = s
	b.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.subs, id)
			b.mu.Unlock()
			close(s.ch) // safe: Publish only sends under the lock, and we are out of the map
		})
	}
	return s.ch, cancel
}

// Subscribers returns the number of live subscriptions.
func (b *Broker) Subscribers() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs)
}

// Dropped returns the total events dropped across all subscribers since start.
func (b *Broker) Dropped() uint64 { return b.dropped.Load() }

// Counters returns a copy of the outcome tally (Jobs, Dead). LeaseExpired and
// SchedulerTicks are zero here; the integrator fills them from obs.Snapshot.
func (b *Broker) Counters() Counters {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := Counters{Jobs: make(map[JobKey]uint64, len(b.tally)), Dead: b.dead}
	for k, v := range b.tally {
		out.Jobs[k] = v
	}
	return out
}
