package job

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

// esc spells a JSON \u escape without writing one in the source (editor
// tooling decodes literal \uXXXX sequences).
func esc(hex string) string { return "\\" + "u" + hex }

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

		// Postgres jsonb / text compatibility: what encoding/json accepts but
		// the database rejects must be an invalid argument, not an internal error.
		{"payload escaped unicode", func(n *NewJob) {
			n.Payload = []byte(`{"a":"` + esc("00e9") + " " + esc("D83D") + esc("DE00") + ` \\u0000"}`)
		}, true},
		{"payload high surrogate then non-low escape", func(n *NewJob) {
			n.Payload = []byte(`{"a":"\ud83d` + esc("0041") + `"}`)
		}, false},
		{"payload utf-8 literal", func(n *NewJob) { n.Payload = []byte(`{"a":"é😀"}`) }, true},
		{"payload nul escape", func(n *NewJob) { n.Payload = []byte(`{"a":"\u0000"}`) }, false},
		{"payload nul escape upper", func(n *NewJob) { n.Payload = []byte(`["\U0000"]`) }, false},
		{"payload lone high surrogate", func(n *NewJob) { n.Payload = []byte(`{"a":"\ud800"}`) }, false},
		{"payload lone low surrogate", func(n *NewJob) { n.Payload = []byte(`{"a":"\udc00"}`) }, false},
		{"payload high surrogate then non-low", func(n *NewJob) { n.Payload = []byte(`{"a":"\ud83dA"}`) }, false},
		{"payload high surrogate at end", func(n *NewJob) { n.Payload = []byte(`"\ud83d"`) }, false},
		{"payload invalid utf-8", func(n *NewJob) { n.Payload = []byte("{\"a\":\"\xff\"}") }, false},
		{"idempotency key unicode", func(n *NewJob) { n.IdempotencyKey = "order-é-1" }, true},
		{"idempotency key nul", func(n *NewJob) { n.IdempotencyKey = "a\x00b" }, false},
		{"idempotency key newline", func(n *NewJob) { n.IdempotencyKey = "a\nb" }, false},
		{"idempotency key del", func(n *NewJob) { n.IdempotencyKey = "a\x7fb" }, false},
		{"idempotency key invalid utf-8", func(n *NewJob) { n.IdempotencyKey = "a\xffb" }, false},
		{"delay at cap", func(n *NewJob) { n.Delay = MaxDelay }, true},
		{"delay over cap", func(n *NewJob) { n.Delay = MaxDelay + time.Second }, false},
		{"run_at before epoch", func(n *NewJob) { n.RunAt = time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC) }, false},
		{"run_at year 10000", func(n *NewJob) { n.RunAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) }, false},
		{"run_at huge unix", func(n *NewJob) { n.RunAt = time.Unix(1e18, 0) }, false},
		{"run_at year 9999", func(n *NewJob) { n.RunAt = time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC) }, true},
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
	atCap := PurgeQuery{Limit: MaxPurgeLimit}
	if err := atCap.Validate(); err != nil {
		t.Fatalf("limit at cap: %v", err)
	}
	overCap := PurgeQuery{Limit: MaxPurgeLimit + 1}
	if err := overCap.Validate(); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("limit over cap must be rejected: %v", err)
	}
}

func TestSanitizeError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"short", "boom", "boom"},
		{"empty", "", ""},
		{"at cap", strings.Repeat("a", MaxErrorLen), strings.Repeat("a", MaxErrorLen)},
		{"over cap ascii", strings.Repeat("a", MaxErrorLen+10), strings.Repeat("a", MaxErrorLen)},
		// "é" is two bytes; 4095 'a' + "é" straddles the cap, the rune must go whole.
		{"over cap rune boundary", strings.Repeat("a", MaxErrorLen-1) + "éé", strings.Repeat("a", MaxErrorLen-1)},
		{"nul dropped", "a\x00b\x00", "ab"},
		{"invalid utf-8 replaced", "a\xffb", "a�b"},
		{"nul and invalid", "\x00\xfe", "�"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := SanitizeError(tc.in)
			if got != tc.want {
				t.Fatalf("SanitizeError(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if len(got) > MaxErrorLen {
				t.Fatalf("len %d > MaxErrorLen", len(got))
			}
		})
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
		{"cron too long", func(s *Schedule) { s.Cron = strings.Repeat("*", MaxCronLen+1) }, false},
		{"tz too long", func(s *Schedule) { s.Timezone = strings.Repeat("A", MaxTimezoneLen+1) }, false},
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
