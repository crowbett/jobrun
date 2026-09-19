package worker

import "sync/atomic"

// Stats is a snapshot of the pool's counters since it was created.
type Stats struct {
	// Claimed is the number of jobs handed to a handler goroutine.
	Claimed int64
	// Succeeded is the number of Complete calls that went through.
	Succeeded int64
	// Failed is the number of handler calls that returned an error (or panicked).
	Failed int64
	// Retried is the number of failures the store put back in the queue.
	Retried int64
	// Dead is the number of failures the store moved to dead.
	Dead int64
	// LeaseLost is the number of Complete/Fail calls rejected with ErrLeaseLost.
	LeaseLost int64
	// InFlight is the number of handlers running right now.
	InFlight int64
}

type counters struct {
	claimed, succeeded, failed, retried, dead, leaseLost, inFlight atomic.Int64
}

func (c *counters) snapshot() Stats {
	return Stats{
		Claimed:   c.claimed.Load(),
		Succeeded: c.succeeded.Load(),
		Failed:    c.failed.Load(),
		Retried:   c.retried.Load(),
		Dead:      c.dead.Load(),
		LeaseLost: c.leaseLost.Load(),
		InFlight:  c.inFlight.Load(),
	}
}
