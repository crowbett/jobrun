package job

import (
	"encoding/json"
	"errors"
	"math/rand/v2"
	"testing"
	"time"
)

func seeded(seed uint64) *rand.Rand { return rand.New(rand.NewPCG(seed, seed)) }

func TestPolicyCapLadder(t *testing.T) {
	t.Parallel()
	p := Policy{Base: time.Second, Max: 5 * time.Minute, Factor: 2}
	tests := []struct {
		attempt int32
		want    time.Duration
	}{
		{0, time.Second},
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{5, 16 * time.Second},
		{9, 256 * time.Second},
		{10, 5 * time.Minute}, // 512s > 300s cap
		{60, 5 * time.Minute},
		{1000, 5 * time.Minute}, // float overflow path
	}
	for _, tc := range tests {
		if got := p.Cap(tc.attempt); got != tc.want {
			t.Errorf("Cap(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
		if got := p.Next(tc.attempt, nil); got != tc.want {
			t.Errorf("Next(%d) without jitter = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

func TestPolicyNextJitterDeterministicAndBounded(t *testing.T) {
	t.Parallel()
	p := Policy{Base: time.Second, Max: 30 * time.Second, Factor: 2, Jitter: true}
	a, b := seeded(42), seeded(42)
	for attempt := int32(1); attempt <= 40; attempt++ {
		x, y := p.Next(attempt, a), p.Next(attempt, b)
		if x != y {
			t.Fatalf("attempt %d: same seed gave %v and %v", attempt, x, y)
		}
		if x < 0 || x >= p.Cap(attempt) || x > p.Max {
			t.Fatalf("attempt %d: %v outside [0, min(cap=%v, max=%v))", attempt, x, p.Cap(attempt), p.Max)
		}
	}
	// Different seeds must not produce the same ladder (sanity that jitter is on).
	c := seeded(7)
	same := true
	a = seeded(42)
	for attempt := int32(1); attempt <= 10; attempt++ {
		if p.Next(attempt, a) != p.Next(attempt, c) {
			same = false
		}
	}
	if same {
		t.Fatal("two seeds produced identical jittered ladders")
	}
}

func TestPolicyNextGlobalRandBounded(t *testing.T) {
	t.Parallel()
	p := DefaultPolicy
	for i := 0; i < 200; i++ {
		if d := p.Next(4, nil); d < 0 || d >= 8*time.Second {
			t.Fatalf("Next(4, nil) = %v outside [0, 8s)", d)
		}
	}
}

func TestPolicyDefaultsAndValidate(t *testing.T) {
	t.Parallel()
	if !(Policy{}).IsZero() || (Policy{}).OrDefault() != DefaultPolicy {
		t.Fatal("zero policy must resolve to DefaultPolicy")
	}
	partial := Policy{Max: time.Minute}.OrDefault()
	if partial.Base != DefaultPolicy.Base || partial.Factor != DefaultPolicy.Factor || partial.Max != time.Minute || partial.Jitter {
		t.Fatalf("partial OrDefault = %+v", partial)
	}
	tests := []struct {
		name string
		p    Policy
		ok   bool
	}{
		{"zero", Policy{}, true},
		{"default", DefaultPolicy, true},
		{"no jitter", Policy{Base: time.Second, Max: time.Second, Factor: 1}, true},
		{"base zero", Policy{Max: time.Minute, Factor: 2}, false},
		{"max below base", Policy{Base: time.Minute, Max: time.Second, Factor: 2}, false},
		{"factor below 1", Policy{Base: time.Second, Max: time.Minute, Factor: 0.5}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.p.Validate()
			if tc.ok && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tc.ok && !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("want ErrInvalidArgument, got %v", err)
			}
		})
	}
}

func TestPolicyJSONRoundTrip(t *testing.T) {
	t.Parallel()
	b, err := json.Marshal(DefaultPolicy)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"base":"1s","max":"5m0s","factor":2,"jitter":true}`; string(b) != want {
		t.Fatalf("marshal = %s, want %s", b, want)
	}
	var back Policy
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back != DefaultPolicy {
		t.Fatalf("round trip = %+v", back)
	}
	// The DDL default shape and the empty object both decode.
	var ddl Policy
	if err := json.Unmarshal([]byte(`{"base":"1s","max":"5m","factor":2,"jitter":true}`), &ddl); err != nil || ddl != DefaultPolicy {
		t.Fatalf("ddl default decode = %+v, %v", ddl, err)
	}
	var empty Policy
	if err := json.Unmarshal([]byte(`{}`), &empty); err != nil || !empty.IsZero() {
		t.Fatalf("empty decode = %+v, %v", empty, err)
	}
	if b, _ := json.Marshal(Policy{}); string(b) != "{}" {
		t.Fatalf("zero marshal = %s", b)
	}
	if err := json.Unmarshal([]byte(`{"base":"soon"}`), &empty); err == nil {
		t.Fatal("bad duration must fail")
	}
}
