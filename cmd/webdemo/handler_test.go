package main

import (
	"encoding/json"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/crowbett/jobrun/internal/job"
)

func locals(r CronResult) []string {
	out := make([]string, len(r.Runs))
	for i, run := range r.Runs {
		out[i] = run.Local + " " + run.Zone
	}
	return out
}

func TestCronDST(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		p    CronParams
		want []string
	}{
		{
			"spring-forward gap is skipped",
			CronParams{Expr: "30 2 * * *", Start: "2026-03-07T00:00:00Z", TZ: "America/New_York", Count: 3},
			[]string{"Sat 2026-03-07 02:30 EST", "Mon 2026-03-09 02:30 EDT", "Tue 2026-03-10 02:30 EDT"},
		},
		{
			"fall-back repeat fires once",
			CronParams{Expr: "30 1 * * *", Start: "2026-10-31T00:00:00Z", TZ: "America/New_York", Count: 3},
			[]string{"Sat 2026-10-31 01:30 EDT", "Sun 2026-11-01 01:30 EDT", "Mon 2026-11-02 01:30 EST"},
		},
		{
			"empty zone is UTC",
			CronParams{Expr: "@weekly", Start: "2026-01-01T00:00:00Z", Count: 2},
			[]string{"Sun 2026-01-04 00:00 UTC", "Sun 2026-01-11 00:00 UTC"},
		},
		{"nothing within the lookahead", CronParams{Expr: "0 0 30 2 *", Start: "2026-01-01T00:00:00Z", Count: 3}, []string{}},
		{
			"runs out within the lookahead",
			CronParams{Expr: "0 0 29 2 *", Start: "2090-01-01T00:00:00Z", Count: 10},
			[]string{"Fri 2092-02-29 00:00 UTC", "Wed 2096-02-29 00:00 UTC"},
		},
	}
	for _, c := range cases {
		r, err := Cron(c.p)
		if err != nil {
			t.Fatal(c.name, err)
		}
		if got := locals(r); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

func TestCronNormalizes(t *testing.T) {
	t.Parallel()
	r, err := Cron(CronParams{Expr: "@daily", Start: "2026-01-01T12:00:00Z", Count: 1})
	if err != nil {
		t.Fatal(err)
	}
	if r.Normalized != "0 0 * * *" || r.Runs[0].UTC != "2026-01-02T00:00:00Z" {
		t.Errorf("got %+v", r)
	}
}

func TestBackoffMatchesPolicy(t *testing.T) {
	t.Parallel()
	pol := job.Policy{Base: 500 * time.Millisecond, Max: 20 * time.Second, Factor: 3, Jitter: true}
	raw, err := json.Marshal(pol)
	if err != nil {
		t.Fatal(err)
	}
	r, err := Backoff(BackoffParams{Policy: raw, MaxAttempts: 8, Seed: 42, Samples: 4})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Retries) != 7 {
		t.Fatalf("got %d retries, want 7 (attempt 8 is final)", len(r.Retries))
	}
	for s := range 4 {
		rnd := rand.New(rand.NewPCG(42, uint64(s)))
		for _, rt := range r.Retries {
			if want := pol.Next(rt.Attempt, rnd).Milliseconds(); rt.DelaysMs[s] != want {
				t.Errorf("sample %d attempt %d: %d ms, want %d", s, rt.Attempt, rt.DelaysMs[s], want)
			}
			if rt.CapMs != ms(pol.Cap(rt.Attempt)) || float64(rt.DelaysMs[s]) > rt.CapMs {
				t.Errorf("attempt %d: cap %v, delay %d", rt.Attempt, rt.CapMs, rt.DelaysMs[s])
			}
		}
	}
}

func TestBackoffDefaultsAndNoJitter(t *testing.T) {
	t.Parallel()
	r, err := Backoff(BackoffParams{Policy: json.RawMessage(`{}`), MaxAttempts: 2, Samples: 1})
	if err != nil {
		t.Fatal(err)
	}
	if r.Policy != job.DefaultPolicy {
		t.Errorf("empty policy resolved to %+v, want DefaultPolicy", r.Policy)
	}
	r, err = Backoff(BackoffParams{Policy: json.RawMessage(`{"base":"1s","max":"5s","factor":2,"jitter":false}`), MaxAttempts: 6, Samples: 2})
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{1000, 2000, 4000, 5000, 5000}
	for i, rt := range r.Retries {
		if rt.DelaysMs[0] != want[i] || rt.DelaysMs[1] != want[i] {
			t.Errorf("attempt %d without jitter: %v, want %d", rt.Attempt, rt.DelaysMs, want[i])
		}
	}
}

func TestBackoffSingleAttemptHasNoRetries(t *testing.T) {
	t.Parallel()
	r, err := Backoff(BackoffParams{Policy: json.RawMessage(`{}`), MaxAttempts: 1, Samples: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Retries) != 0 {
		t.Errorf("got %d retries, want none (attempt 1 is final)", len(r.Retries))
	}
}

func TestHandleErrors(t *testing.T) {
	t.Parallel()
	cases := []struct{ method, params, want string }{
		{"nope", `{}`, `unknown method "nope"`},
		{"cron", `{`, "unexpected end"},
		{"cron", `{"expr":"61 * * * *","start":"2026-01-01T00:00:00Z","count":3}`, "cronx: invalid expression: minute: 61 out of range 0-59"},
		{"cron", `{"expr":"* * * *","start":"2026-01-01T00:00:00Z","count":3}`, "cronx: invalid expression"},
		{"cron", `{"expr":"@weekly","start":"2026-01-01T00:00:00Z","tz":"Mars/Base","count":3}`, `time zone "Mars/Base"`},
		{"cron", `{"expr":"@weekly","start":"yesterday","count":3}`, "start must be RFC 3339"},
		{"cron", `{"expr":"@weekly","start":"2026-01-01T00:00:00Z","count":0}`, "count must be 1..50"},
		{"cron", `{"expr":"@weekly","start":"2026-01-01T00:00:00Z","count":51}`, "count must be 1..50"},
		{"backoff", `{"policy":{"base":"x"},"maxAttempts":5,"samples":1}`, `backoff policy base: time: invalid duration "x"`},
		{"backoff", `{"policy":{"base":"1s","max":"0.5s","factor":2},"maxAttempts":5,"samples":1}`, "backoff max must be >= base"},
		{"backoff", `{"policy":{"base":"1s","max":"5s","factor":0.5},"maxAttempts":5,"samples":1}`, "backoff factor must be >= 1"},
		{"backoff", `{"policy":{},"maxAttempts":51,"samples":1}`, "max attempts must be 1..50"},
		{"backoff", `{"policy":{},"maxAttempts":5,"samples":0}`, "samples must be 1..50"},
	}
	for _, c := range cases {
		got, ok := Handle(c.method, []byte(c.params)).(map[string]string)
		if !ok || !strings.Contains(got["error"], c.want) {
			t.Errorf("%s %s: got %v, want an error containing %q", c.method, c.params, got, c.want)
		}
	}
}

func TestResultsHaveNoErrorKey(t *testing.T) {
	t.Parallel()
	for method, params := range map[string]string{
		"cron":    `{"expr":"*/15 9-17 * * MON-FRI","start":"2026-01-01T00:00:00Z","tz":"Europe/London","count":50}`,
		"backoff": `{"policy":{},"maxAttempts":50,"seed":7,"samples":50}`,
	} {
		raw, err := json.Marshal(Handle(method, []byte(params)))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), `"error"`) {
			t.Errorf("%s: unexpected error in %s", method, raw)
		}
	}
}
