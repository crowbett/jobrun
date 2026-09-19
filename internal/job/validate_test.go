package job

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

func validNewJob() NewJob {
	return NewJob{Queue: "default", Type: "email.send", Payload: []byte(`{"to":"x"}`), Priority: 5}
}

func TestValidateName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		ok   bool
	}{
		{"simple", "default", true},
		{"dotted dashed underscored", "a.b-c_d9", true},
		{"single char", "x", true},
		{"max length", strings.Repeat("a", MaxNameLen), true},
		{"empty", "", false},
		{"too long", strings.Repeat("a", MaxNameLen+1), false},
		{"uppercase", "Default", false},
		{"space", "my queue", false},
		{"slash", "a/b", false},
		{"unicode", "queué", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateName("queue", tc.in)
			if tc.ok && err != nil {
				t.Fatalf("unexpected: %v", err)
			}
			if !tc.ok && !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("want ErrInvalidArgument, got %v", err)
			}
		})
	}
}

func TestNewJobValidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		mut  func(*NewJob)
		ok   bool
	}{
		{"valid", func(*NewJob) {}, true},
		{"empty payload", func(n *NewJob) { n.Payload = nil }, true},
		{"payload at cap", func(n *NewJob) {
			n.Payload = append([]byte(`"`), append(bytes.Repeat([]byte("a"), MaxPayloadBytes-2), '"')...)
		}, true},
		{"run_at only", func(n *NewJob) { n.RunAt = time.Now() }, true},
		{"delay only", func(n *NewJob) { n.Delay = time.Minute }, true},
		{"max_attempts default", func(n *NewJob) { n.MaxAttempts = 0 }, true},
		{"max_attempts at limit", func(n *NewJob) { n.MaxAttempts = MaxAttemptsLimit }, true},
		{"priority bounds", func(n *NewJob) { n.Priority = MinPriority }, true},
		{"custom backoff", func(n *NewJob) { n.Backoff = Policy{Base: time.Second, Max: time.Minute, Factor: 3} }, true},
		{"idempotency key at cap", func(n *NewJob) { n.IdempotencyKey = strings.Repeat("k", MaxIdempotencyKeyLen) }, true},

		{"bad queue", func(n *NewJob) { n.Queue = "Bad Queue" }, false},
		{"empty type", func(n *NewJob) { n.Type = "" }, false},
		{"payload over cap", func(n *NewJob) {
			n.Payload = append([]byte(`"`), append(bytes.Repeat([]byte("a"), MaxPayloadBytes-1), '"')...)
		}, false},
		{"payload not json", func(n *NewJob) { n.Payload = []byte(`{"to":`) }, false},
		{"priority too high", func(n *NewJob) { n.Priority = MaxPriority + 1 }, false},
		{"priority too low", func(n *NewJob) { n.Priority = MinPriority - 1 }, false},
		{"run_at and delay", func(n *NewJob) { n.RunAt, n.Delay = time.Now(), time.Second }, false},
		{"negative delay", func(n *NewJob) { n.Delay = -time.Second }, false},
		{"max_attempts negative", func(n *NewJob) { n.MaxAttempts = -1 }, false},
		{"max_attempts over limit", func(n *NewJob) { n.MaxAttempts = MaxAttemptsLimit + 1 }, false},
		{"bad backoff", func(n *NewJob) { n.Backoff = Policy{Base: time.Minute, Max: time.Second, Factor: 2} }, false},
		{"idempotency key too long", func(n *NewJob) { n.IdempotencyKey = strings.Repeat("k", MaxIdempotencyKeyLen+1) }, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n := validNewJob()
			tc.mut(&n)
			err := n.Validate()
			if tc.ok && err != nil {
				t.Fatalf("unexpected: %v", err)
			}
			if !tc.ok && !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("want ErrInvalidArgument, got %v", err)
			}
		})
	}
}

func TestNewAppliesDefaults(t *testing.T) {
	t.Parallel()
	n := validNewJob()
	n.Payload = nil
	j, err := New(n)
	if err != nil {
		t.Fatal(err)
	}
	if j.ID == (ID{}) || j.Status != StatusQueued || string(j.Payload) != "{}" ||
		j.MaxAttempts != DefaultMaxAttempts || j.Backoff != DefaultPolicy || j.Priority != 5 {
		t.Fatalf("defaults not applied: %+v", j)
	}
	a, b := NewID(), NewID()
	if a == b || a.Version() != 7 {
		t.Fatalf("NewID must be unique v7: %s %s", a, b)
	}
	n.Queue = ""
	if _, err := New(n); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("New must validate: %v", err)
	}
}

func TestListQueryValidate(t *testing.T) {
	t.Parallel()
	dead, unspec, bogus := StatusDead, StatusUnspecified, Status(42)
	tests := []struct {
		name      string
		q         ListQuery
		ok        bool
		wantLimit int32
	}{
		{"defaults", ListQuery{}, true, DefaultListLimit},
		{"filters", ListQuery{Queue: "q", Type: "t", Status: &dead, Limit: 10}, true, 10},
		{"max limit", ListQuery{Limit: MaxListLimit}, true, MaxListLimit},
		{"over limit", ListQuery{Limit: MaxListLimit + 1}, false, 0},
		{"negative limit", ListQuery{Limit: -1}, false, 0},
		{"bad queue", ListQuery{Queue: "Q"}, false, 0},
		{"bad type", ListQuery{Type: "a b"}, false, 0},
		{"unspecified status", ListQuery{Status: &unspec}, false, 0},
		{"bogus status", ListQuery{Status: &bogus}, false, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			q := tc.q
			err := q.Validate()
			if tc.ok && (err != nil || q.Limit != tc.wantLimit) {
				t.Fatalf("got err %v, limit %d", err, q.Limit)
			}
			if !tc.ok && !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("want ErrInvalidArgument, got %v", err)
			}
		})
	}
}

func TestPurgeQueryValidate(t *testing.T) {
	t.Parallel()
	q := PurgeQuery{}
	if err := q.Validate(); err != nil || len(q.Statuses) != 3 {
		t.Fatalf("defaults: %v %v", err, q.Statuses)
	}
	bad := PurgeQuery{Statuses: []Status{StatusLeased}}
	if err := bad.Validate(); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("live status must be rejected: %v", err)
	}
	neg := PurgeQuery{OlderThan: -time.Hour}
	if err := neg.Validate(); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative age: %v", err)
	}
}

func TestScheduleValidate(t *testing.T) {
	t.Parallel()
	base := Schedule{Name: "nightly", Cron: "0 2 * * *", Timezone: "America/New_York", Template: validNewJob()}
	tests := []struct {
		name string
		mut  func(*Schedule)
		ok   bool
	}{
		{"valid", func(*Schedule) {}, true},
		{"utc default", func(s *Schedule) { s.Timezone = "" }, true},
		{"template run_at ignored", func(s *Schedule) { s.Template.RunAt, s.Template.Delay = time.Now(), time.Second }, true},
		{"bad name", func(s *Schedule) { s.Name = "Nightly Job" }, false},
		{"empty cron", func(s *Schedule) { s.Cron = "" }, false},
		{"bad tz", func(s *Schedule) { s.Timezone = "Mars/Olympus" }, false},
		{"bad template", func(s *Schedule) { s.Template.Queue = "" }, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := base
			tc.mut(&s)
			err := s.Validate()
			if tc.ok && err != nil {
				t.Fatalf("unexpected: %v", err)
			}
			if !tc.ok && !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("want ErrInvalidArgument, got %v", err)
			}
		})
	}
}
