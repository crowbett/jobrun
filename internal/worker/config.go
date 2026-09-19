// Package worker runs claimed jobs. A Pool starts one claim loop per queue that
// feeds a bounded set of handler goroutines, plus one heartbeater that extends
// the leases of everything in flight and fences jobs whose lease was lost or
// cancelled (docs/PLAN.md, step 7).
package worker

import (
	"crypto/rand"
	"fmt"
	"os"
	"time"
)

// Defaults applied by NewPool to zero Config fields.
const (
	DefaultConcurrency    = 16
	DefaultBatchSize      = 32
	DefaultLeaseTTL       = 30 * time.Second
	DefaultHeartbeatEvery = 10 * time.Second
	DefaultJobTimeout     = 10 * time.Minute
	DefaultPollFloor      = 200 * time.Millisecond
)

// Config tunes a Pool. Zero fields take the defaults listed on each.
type Config struct {
	// Queues are the queues this pool claims from; Run fails without one.
	Queues []string
	// Concurrency bounds handler goroutines across all queues (DefaultConcurrency).
	Concurrency int
	// BatchSize is the most jobs one claim asks for (DefaultBatchSize).
	BatchSize int
	// LeaseTTL is how long a claim or heartbeat holds a lease (DefaultLeaseTTL).
	LeaseTTL time.Duration
	// HeartbeatEvery is the heartbeat period (DefaultHeartbeatEvery); it is
	// clamped to LeaseTTL/3 when set higher than that.
	HeartbeatEvery time.Duration
	// JobTimeout bounds one handler call and is also the drain budget when Run
	// stops (DefaultJobTimeout).
	JobTimeout time.Duration
	// Owner identifies this process in lease_owner (DefaultOwner()).
	Owner string
	// PollFloor is the claim loop's tick when the dispatcher stays silent
	// (DefaultPollFloor).
	PollFloor time.Duration
	// Durations, when set, receives the wall time of every handler run
	// (success or failure) so jobrun_job_duration_seconds can be exposed
	// without the worker importing internal/admin. Optional.
	Durations DurationObserver
}

// DurationObserver observes one handler run per call. *admin.Histogram
// satisfies it; cmd/jobrund passes it through Config.Durations.
type DurationObserver interface {
	ObserveDuration(queue, typ string, d time.Duration)
}

func (c Config) withDefaults() Config {
	if c.Concurrency <= 0 {
		c.Concurrency = DefaultConcurrency
	}
	if c.BatchSize <= 0 {
		c.BatchSize = DefaultBatchSize
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = DefaultLeaseTTL
	}
	if c.HeartbeatEvery <= 0 {
		c.HeartbeatEvery = DefaultHeartbeatEvery
	}
	if c.HeartbeatEvery > c.LeaseTTL/3 {
		c.HeartbeatEvery = max(c.LeaseTTL/3, time.Millisecond)
	}
	if c.JobTimeout <= 0 {
		c.JobTimeout = DefaultJobTimeout
	}
	if c.PollFloor <= 0 {
		c.PollFloor = DefaultPollFloor
	}
	if c.Owner == "" {
		c.Owner = DefaultOwner()
	}
	return c
}

// DefaultOwner returns "<hostname>-<pid>-<4 hex>", unique per process start so a
// restarted process never confuses its own stale leases with live ones.
func DefaultOwner() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	var b [2]byte
	_, _ = rand.Read(b[:]) // crypto/rand never fails on supported platforms
	return fmt.Sprintf("%s-%d-%x", host, os.Getpid(), b)
}
