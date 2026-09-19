package job

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// Policy is exponential backoff with full jitter, the only source of retry
// delays in the system (the reaper and Fail both compute run_at through it).
//
//	delay(n) = rand(0, min(Base * Factor^(n-1), Max))   for attempt n (1-based)
//
// With Jitter off the delay is the capped ladder value itself. A zero Policy
// means "use DefaultPolicy" (see OrDefault).
type Policy struct {
	Base   time.Duration
	Max    time.Duration
	Factor float64
	Jitter bool
}

// DefaultPolicy is 1s, 5m, x2, full jitter.
var DefaultPolicy = Policy{Base: time.Second, Max: 5 * time.Minute, Factor: 2, Jitter: true}

// IsZero reports whether every field is unset.
func (p Policy) IsZero() bool { return p == Policy{} }

// OrDefault returns DefaultPolicy for a zero Policy; otherwise it fills unset
// Base/Max/Factor from DefaultPolicy and keeps Jitter as given.
func (p Policy) OrDefault() Policy {
	if p.IsZero() {
		return DefaultPolicy
	}
	if p.Base <= 0 {
		p.Base = DefaultPolicy.Base
	}
	if p.Max <= 0 {
		p.Max = DefaultPolicy.Max
	}
	if p.Factor <= 0 {
		p.Factor = DefaultPolicy.Factor
	}
	return p
}

// Validate checks Base > 0, Max >= Base and Factor >= 1. A zero Policy is valid
// (it means DefaultPolicy).
func (p Policy) Validate() error {
	if p.IsZero() {
		return nil
	}
	switch {
	case p.Base <= 0:
		return fmt.Errorf("%w: backoff base must be > 0", ErrInvalidArgument)
	case p.Max < p.Base:
		return fmt.Errorf("%w: backoff max must be >= base", ErrInvalidArgument)
	case p.Factor < 1 || math.IsNaN(p.Factor) || math.IsInf(p.Factor, 0):
		return fmt.Errorf("%w: backoff factor must be >= 1", ErrInvalidArgument)
	}
	return nil
}

// Cap returns the un-jittered ladder value for attempt (1-based), bounded by Max.
// attempt <= 1 yields Base. The exponent is computed in float64 and clamped, so
// any attempt count is safe.
func (p Policy) Cap(attempt int32) time.Duration {
	p = p.OrDefault()
	if attempt <= 1 {
		return min(p.Base, p.Max)
	}
	d := float64(p.Base) * math.Pow(p.Factor, float64(attempt-1))
	if math.IsNaN(d) || d >= float64(p.Max) {
		return p.Max
	}
	return time.Duration(d)
}

// Next returns the delay before attempt (1-based) runs again. rnd may be nil,
// in which case the process-global generator is used; tests pass a seeded one
// for a deterministic ladder.
func (p Policy) Next(attempt int32, rnd *rand.Rand) time.Duration {
	p = p.OrDefault()
	c := p.Cap(attempt)
	if !p.Jitter || c <= 0 {
		return c
	}
	if rnd == nil {
		return time.Duration(rand.Int64N(int64(c)))
	}
	return time.Duration(rnd.Int64N(int64(c)))
}

// policyJSON is the jsonb shape stored in jobs.backoff / schedules.backoff:
// {"base":"1s","max":"5m","factor":2,"jitter":true}.
type policyJSON struct {
	Base   string  `json:"base,omitempty"`
	Max    string  `json:"max,omitempty"`
	Factor float64 `json:"factor,omitempty"`
	Jitter bool    `json:"jitter"`
}

// MarshalJSON writes durations as Go duration strings ("1s", "5m").
func (p Policy) MarshalJSON() ([]byte, error) {
	if p.IsZero() {
		return []byte("{}"), nil
	}
	return json.Marshal(policyJSON{
		Base:   p.Base.String(),
		Max:    p.Max.String(),
		Factor: p.Factor,
		Jitter: p.Jitter,
	})
}

// UnmarshalJSON reads the jsonb shape; "{}" or missing fields yield a zero
// Policy (which OrDefault resolves).
func (p *Policy) UnmarshalJSON(b []byte) error {
	var raw policyJSON
	if err := json.Unmarshal(b, &raw); err != nil {
		return fmt.Errorf("backoff policy: %w", err)
	}
	var out Policy
	var err error
	if raw.Base != "" {
		if out.Base, err = time.ParseDuration(raw.Base); err != nil {
			return fmt.Errorf("backoff policy base: %w", err)
		}
	}
	if raw.Max != "" {
		if out.Max, err = time.ParseDuration(raw.Max); err != nil {
			return fmt.Errorf("backoff policy max: %w", err)
		}
	}
	out.Factor = raw.Factor
	out.Jitter = raw.Jitter
	*p = out
	return nil
}
