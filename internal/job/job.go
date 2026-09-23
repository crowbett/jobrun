// Package job is the domain of the queue: the Job and Schedule types, the status
// transition table, the backoff policy, keyset cursors, validation, the sentinel
// errors, the Store and Dispatcher ports, and the handler registry.
//
// It performs no I/O and imports nothing from this module; every other package
// (postgres, worker, scheduler, reaper, server, admin) depends on it and nothing
// here depends on them. The signatures are frozen by docs/PLAN.md.
package job

import (
	"time"

	"github.com/google/uuid"
)

// ID is a UUID; new ids are v7 (time-sortable) from NewID.
type ID = uuid.UUID

// NewID returns a new time-ordered (v7) id, falling back to v4 if the system
// clock or entropy source misbehaves.
func NewID() ID {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.New()
	}
	return id
}

// Job is one unit of work and its full persisted state.
type Job struct {
	ID    ID
	Queue string
	Type  string
	// Payload is a JSON document (jsonb in Postgres); empty means {}.
	Payload []byte
	Status  Status
	// Priority orders claims within a queue, higher first (-100..100).
	Priority int16
	// RunAt is when the job becomes claimable. Zero at enqueue means "database
	// now" (plus Delay); the store never returns it zero.
	RunAt time.Time
	// Delay is enqueue-only: when RunAt is zero the store sets
	// run_at = now() + Delay on the database clock. It is never read back.
	Delay time.Duration
	// Attempts counts starts; it is incremented at claim time, so a crash mid-job
	// still burns exactly one attempt.
	Attempts    int32
	MaxAttempts int32
	Backoff     Policy
	// LeaseOwner/LeaseUntil are set while Status == StatusLeased and nil/"" otherwise.
	LeaseOwner string
	LeaseUntil *time.Time
	// LeaseToken is the fencing token of the latest claim: every claim bumps
	// it, and Heartbeat/Complete/Fail must present it (see Lease).
	LeaseToken int64
	LastError  string
	// IdempotencyKey is optional and unique per queue.
	IdempotencyKey string
	// CancelRequested is set by Cancel on a leased job; the worker sees it
	// through Heartbeat (the id is omitted from the kept set) and cancels the
	// handler context. The job then ends cancelled even if Complete/Fail arrive.
	CancelRequested bool
	// TraceParent is the W3C trace context captured at enqueue ("" when the
	// producer was not traced); the run span is started as its child.
	TraceParent string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	StartedAt   *time.Time
	FinishedAt  *time.Time
}

// NewJob is an enqueue request after transport decoding and before defaults.
// Exactly one of RunAt / Delay may be set; neither means "now".
type NewJob struct {
	Queue          string
	Type           string
	Payload        []byte
	Priority       int16
	RunAt          time.Time
	Delay          time.Duration
	MaxAttempts    int32
	Backoff        Policy
	IdempotencyKey string
}

// New validates n, applies defaults (MaxAttempts 5, DefaultPolicy, payload {})
// and returns a queued Job with a fresh id, ready for Store.Enqueue.
func New(n NewJob) (Job, error) {
	if err := n.Validate(); err != nil {
		return Job{}, err
	}
	payload := n.Payload
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	maxAttempts := n.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultMaxAttempts
	}
	return Job{
		ID:             NewID(),
		Queue:          n.Queue,
		Type:           n.Type,
		Payload:        payload,
		Status:         StatusQueued,
		Priority:       n.Priority,
		RunAt:          n.RunAt,
		Delay:          n.Delay,
		MaxAttempts:    maxAttempts,
		Backoff:        n.Backoff.OrDefault(),
		IdempotencyKey: n.IdempotencyKey,
	}, nil
}

// ListQuery filters and pages List. Every filter is optional; Status nil means
// every status. Limit 0 means DefaultListLimit; it is capped at MaxListLimit.
type ListQuery struct {
	Queue  string
	Status *Status
	Type   string
	Cursor Cursor
	Limit  int32
}

// Event is one transition, written to job_events in the same transaction as the
// row change and fanned out to the admin SSE feed and the metrics counters.
// From is StatusUnspecified for the enqueue event.
type Event struct {
	JobID   ID
	Queue   string
	Type    string
	From    Status
	To      Status
	Attempt int32
	At      time.Time
	Err     string
}

// Schedule is a cron template the scheduler materialises into jobs. Template
// carries queue/type/payload/priority/max_attempts/backoff; its RunAt, Delay
// and IdempotencyKey are ignored (the scheduler sets run_at = NextRun and the
// key "sched:<id>:<next_run RFC3339>").
type Schedule struct {
	ID        ID
	Name      string
	Cron      string
	Timezone  string
	Template  NewJob
	Enabled   bool
	NextRun   time.Time
	LastRun   *time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Lease names one claim of one job. Token is the job's LeaseToken as returned
// by Claim: it is bumped on every claim, so a lease from an earlier attempt -
// expired, reaped and claimed again, even by the same owner - can never
// heartbeat, complete or fail the job again.
type Lease struct {
	ID    ID
	Token int64
}

// LeaseOf returns the lease a claimed job carries.
func LeaseOf(j Job) Lease { return Lease{ID: j.ID, Token: j.LeaseToken} }

// Renewal is a heartbeat verdict. Kept leases were extended. CancelRequested
// leases were extended too, but the job was cancelled: the worker stops the
// handler and still reports it with Fail or Complete (the job ends cancelled).
// A lease in neither list was lost (expired, reaped, stolen): the worker must
// cancel the handler and never Complete or Fail it.
//
// Verdicts name leases, not job ids: a worker may hold an old and a new lease
// of the same job at once (it reclaimed its own reaped job), and only the new
// one is kept.
type Renewal struct {
	Kept            []Lease
	CancelRequested []Lease
}

// ClaimQuery is one batch claim: up to Limit ready jobs on Queue, leased to
// Owner for LeaseTTL from the database clock.
type ClaimQuery struct {
	Queue    string
	Owner    string
	Limit    int32
	LeaseTTL time.Duration
}

// PurgeQuery deletes finished jobs. Empty Statuses means succeeded, cancelled
// and dead; OlderThan 0 means any age; Limit 0 means the store default.
type PurgeQuery struct {
	Queue     string
	Statuses  []Status
	OlderThan time.Duration
	Limit     int32
}

// DepthRow is one (queue, status) cell of the depth table with the oldest
// run_at in that cell (zero when N == 0).
type DepthRow struct {
	Queue       string
	Status      Status
	N           int64
	OldestRunAt time.Time
}
