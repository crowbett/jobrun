package job

import "fmt"

// Status is the job lifecycle state. The integer values match jobrun.v1.JobStatus
// and String() matches the Postgres job_status enum.
//
//	Queued -> Leased -> Succeeded
//	Leased -> Queued            retry: run_at moves into the future by the backoff policy
//	Leased -> Dead              attempts exhausted, ErrPermanent, or reaped at max_attempts
//	Queued|Leased -> Cancelled  Cancel (cooperative for leased: cancel_requested, then the
//	                            worker gives the job up on its next heartbeat)
//	Dead|Cancelled -> Queued    Retry, attempts reset to 0
//
// A job waiting for its next attempt is Queued with a future RunAt. StatusFailed
// is reserved ("last attempt failed, not yet dead") and is never written in v1;
// it exists so the enum stays aligned with the proto and the database.
type Status int

// Status values. StatusUnspecified is "no prior state": the From of the enqueue event.
const (
	StatusUnspecified Status = iota
	StatusQueued
	StatusLeased
	StatusSucceeded
	StatusFailed
	StatusDead
	StatusCancelled
)

// Statuses lists every real status (StatusUnspecified excluded) in enum order.
var Statuses = []Status{StatusQueued, StatusLeased, StatusSucceeded, StatusFailed, StatusDead, StatusCancelled}

var statusNames = [...]string{
	StatusUnspecified: "unspecified",
	StatusQueued:      "queued",
	StatusLeased:      "leased",
	StatusSucceeded:   "succeeded",
	StatusFailed:      "failed",
	StatusDead:        "dead",
	StatusCancelled:   "cancelled",
}

// String returns the database spelling ("queued", "leased", ...).
func (s Status) String() string {
	if s < 0 || int(s) >= len(statusNames) {
		return fmt.Sprintf("Status(%d)", int(s))
	}
	return statusNames[s]
}

// Valid reports whether s is one of the defined values (StatusUnspecified included).
func (s Status) Valid() bool { return s >= StatusUnspecified && int(s) < len(statusNames) }

// Terminal reports whether the job is finished until someone calls Retry:
// succeeded, dead or cancelled.
func (s Status) Terminal() bool {
	return s == StatusSucceeded || s == StatusDead || s == StatusCancelled
}

// ParseStatus is the inverse of String. It accepts the database spelling only.
func ParseStatus(name string) (Status, error) {
	for i, n := range statusNames {
		if n == name {
			return Status(i), nil
		}
	}
	return StatusUnspecified, fmt.Errorf("%w: unknown status %q", ErrInvalidArgument, name)
}

// MarshalText implements encoding.TextMarshaler with the database spelling.
func (s Status) MarshalText() ([]byte, error) {
	if !s.Valid() {
		return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, s)
	}
	return []byte(s.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (s *Status) UnmarshalText(b []byte) error {
	v, err := ParseStatus(string(b))
	if err != nil {
		return err
	}
	*s = v
	return nil
}

// transitions is the legal-edge table: transitions[from] is a bitmask of allowed
// targets. Every edge not listed here is illegal, self-loops included.
var transitions = [...]uint8{
	StatusUnspecified: bit(StatusQueued),
	StatusQueued:      bit(StatusLeased) | bit(StatusCancelled),
	StatusLeased:      bit(StatusSucceeded) | bit(StatusQueued) | bit(StatusDead) | bit(StatusCancelled),
	StatusSucceeded:   0,
	StatusFailed:      bit(StatusQueued),
	StatusDead:        bit(StatusQueued),
	StatusCancelled:   bit(StatusQueued),
}

func bit(s Status) uint8 { return 1 << uint(s) }

// CanTransition reports whether the edge s -> to is in the transition table.
func (s Status) CanTransition(to Status) bool {
	if !s.Valid() || !to.Valid() {
		return false
	}
	return transitions[s]&bit(to) != 0
}

// Transition returns nil when s -> to is legal and an error wrapping
// ErrInvalidTransition otherwise.
func (s Status) Transition(to Status) error {
	if !s.CanTransition(to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, s, to)
	}
	return nil
}
