package job

import (
	"encoding/json"
	"fmt"
	"time"
)

// Validation limits. The server enforces them before anything reaches the store.
const (
	// MaxNameLen bounds queue, type and schedule names.
	MaxNameLen = 64
	// MaxPayloadBytes bounds the JSON payload (256 KiB).
	MaxPayloadBytes = 256 << 10
	// MaxIdempotencyKeyLen bounds the idempotency key.
	MaxIdempotencyKeyLen = 255
	// MinPriority is the lowest Priority.
	MinPriority = -100
	// MaxPriority is the highest Priority.
	MaxPriority = 100
	// DefaultMaxAttempts is used when MaxAttempts is 0.
	DefaultMaxAttempts = 5
	// MaxAttemptsLimit caps MaxAttempts.
	MaxAttemptsLimit = 100
	// DefaultListLimit is used when ListQuery.Limit is 0.
	DefaultListLimit = 50
	// MaxListLimit caps ListQuery.Limit.
	MaxListLimit = 200
	// MaxErrorLen bounds LastError / Event.Err as stored.
	MaxErrorLen = 4096
)

// ValidateName checks a queue, type or schedule name: 1..MaxNameLen characters
// of [a-z0-9._-]. It returns an error wrapping ErrInvalidArgument.
func ValidateName(field, name string) error {
	if name == "" {
		return fmt.Errorf("%w: %s must not be empty", ErrInvalidArgument, field)
	}
	if len(name) > MaxNameLen {
		return fmt.Errorf("%w: %s must be at most %d characters", ErrInvalidArgument, field, MaxNameLen)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
		default:
			return fmt.Errorf("%w: %s may only contain [a-z0-9._-], got %q at %d", ErrInvalidArgument, field, c, i)
		}
	}
	return nil
}

// Validate checks every field of an enqueue request; MaxAttempts 0 and a zero
// Backoff are accepted as "use the default".
func (n NewJob) Validate() error {
	if err := ValidateName("queue", n.Queue); err != nil {
		return err
	}
	if err := ValidateName("type", n.Type); err != nil {
		return err
	}
	if err := validatePayload(n.Payload); err != nil {
		return err
	}
	if n.Priority < MinPriority || n.Priority > MaxPriority {
		return fmt.Errorf("%w: priority must be in %d..%d", ErrInvalidArgument, MinPriority, MaxPriority)
	}
	if !n.RunAt.IsZero() && n.Delay != 0 {
		return fmt.Errorf("%w: run_at and delay are mutually exclusive", ErrInvalidArgument)
	}
	if n.Delay < 0 {
		return fmt.Errorf("%w: delay must not be negative", ErrInvalidArgument)
	}
	if n.MaxAttempts < 0 || n.MaxAttempts > MaxAttemptsLimit {
		return fmt.Errorf("%w: max_attempts must be in 1..%d (0 = default %d)", ErrInvalidArgument, MaxAttemptsLimit, DefaultMaxAttempts)
	}
	if err := n.Backoff.Validate(); err != nil {
		return err
	}
	if len(n.IdempotencyKey) > MaxIdempotencyKeyLen {
		return fmt.Errorf("%w: idempotency_key must be at most %d characters", ErrInvalidArgument, MaxIdempotencyKeyLen)
	}
	return nil
}

func validatePayload(p []byte) error {
	if len(p) > MaxPayloadBytes {
		return fmt.Errorf("%w: payload must be at most %d bytes, got %d", ErrInvalidArgument, MaxPayloadBytes, len(p))
	}
	if len(p) > 0 && !json.Valid(p) {
		return fmt.Errorf("%w: payload must be valid JSON", ErrInvalidArgument)
	}
	return nil
}

// Validate checks the filters and normalises Limit: 0 becomes DefaultListLimit,
// anything above MaxListLimit is an error. The receiver is a pointer so the
// caller's Limit is updated.
func (q *ListQuery) Validate() error {
	if q.Queue != "" {
		if err := ValidateName("queue", q.Queue); err != nil {
			return err
		}
	}
	if q.Type != "" {
		if err := ValidateName("type", q.Type); err != nil {
			return err
		}
	}
	if q.Status != nil && (!q.Status.Valid() || *q.Status == StatusUnspecified) {
		return fmt.Errorf("%w: unknown status filter %s", ErrInvalidArgument, *q.Status)
	}
	switch {
	case q.Limit < 0 || q.Limit > MaxListLimit:
		return fmt.Errorf("%w: limit must be in 1..%d", ErrInvalidArgument, MaxListLimit)
	case q.Limit == 0:
		q.Limit = DefaultListLimit
	}
	return nil
}

// Validate checks a purge request; empty Statuses is filled with the terminal
// statuses, and a non-terminal status is rejected (purging live jobs is not a thing).
func (q *PurgeQuery) Validate() error {
	if q.Queue != "" {
		if err := ValidateName("queue", q.Queue); err != nil {
			return err
		}
	}
	if len(q.Statuses) == 0 {
		q.Statuses = []Status{StatusSucceeded, StatusCancelled, StatusDead}
	}
	for _, s := range q.Statuses {
		if !s.Terminal() {
			return fmt.Errorf("%w: cannot purge status %s", ErrInvalidArgument, s)
		}
	}
	if q.OlderThan < 0 {
		return fmt.Errorf("%w: older_than must not be negative", ErrInvalidArgument)
	}
	if q.Limit < 0 {
		return fmt.Errorf("%w: limit must not be negative", ErrInvalidArgument)
	}
	return nil
}

// Validate checks name, timezone (an IANA name; "" means UTC) and the template.
// The cron expression is validated by the server with cronx, which this package
// does not import.
func (s Schedule) Validate() error {
	if err := ValidateName("name", s.Name); err != nil {
		return err
	}
	if s.Cron == "" {
		return fmt.Errorf("%w: cron must not be empty", ErrInvalidArgument)
	}
	if s.Timezone != "" {
		if _, err := time.LoadLocation(s.Timezone); err != nil {
			return fmt.Errorf("%w: timezone %q: %w", ErrInvalidArgument, s.Timezone, err)
		}
	}
	t := s.Template
	t.RunAt, t.Delay, t.IdempotencyKey = time.Time{}, 0, ""
	if err := t.Validate(); err != nil {
		return fmt.Errorf("template: %w", err)
	}
	return nil
}
