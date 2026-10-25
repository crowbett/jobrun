// Command webdemo is jobrun's in-browser demo bridge, compiled to js/wasm by
// crowbett.github.io: it runs the real cron parser (internal/cronx) and the
// real retry backoff (job.Policy) on inputs from the page. Contract:
// https://github.com/crowbett/crowbett.github.io/blob/main/docs/demo-contract.md
package main

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"
	_ "time/tzdata" // browser Go has no zoneinfo files

	"github.com/crowbett/jobrun/internal/cronx"
	"github.com/crowbett/jobrun/internal/job"
)

const (
	maxRuns     = 50
	maxAttempts = 50
	maxSamples  = 50
)

// CronParams asks for the next Count firings of Expr after Start (RFC 3339)
// in time zone TZ (IANA name; empty means UTC).
type CronParams struct {
	Expr  string `json:"expr"`
	Start string `json:"start"`
	TZ    string `json:"tz"`
	Count int    `json:"count"`
}

// Run is one firing: the instant in UTC and the wall clock in the zone.
type Run struct {
	UTC   string `json:"utc"`
	Local string `json:"local"`
	Zone  string `json:"zone"`
}

// CronResult is the normalised expression and the runs found. Runs is
// shorter than asked when Next found nothing within cronx.MaxLookahead years.
type CronResult struct {
	Normalized string `json:"normalized"`
	Runs       []Run  `json:"runs"`
}

// BackoffParams is a policy in its stored jsonb shape
// ({"base":"1s","max":"5m","factor":2,"jitter":true}), the job's max
// attempts, and Samples seeded ladders starting at Seed.
type BackoffParams struct {
	Policy      json.RawMessage `json:"policy"`
	MaxAttempts int32           `json:"maxAttempts"`
	Seed        uint64          `json:"seed"`
	Samples     int             `json:"samples"`
}

// Retry is the delay after failed attempt Attempt: CapMs is the un-jittered
// ladder value and DelaysMs holds one jittered draw per sample.
type Retry struct {
	Attempt  int32   `json:"attempt"`
	CapMs    float64 `json:"capMs"`
	DelaysMs []int64 `json:"delaysMs"`
}

// BackoffResult is the effective policy (defaults filled in) and one Retry
// per attempt that is retried; the attempt after the last one is final.
type BackoffResult struct {
	Policy  job.Policy `json:"policy"`
	Retries []Retry    `json:"retries"`
}

// Handle answers one bridge call. Errors come back as {"error": "..."}.
func Handle(method string, params []byte) (res any) {
	defer func() {
		if v := recover(); v != nil {
			res = errorResult(fmt.Errorf("internal: %v", v))
		}
	}()
	var (
		out any
		err error
	)
	switch method {
	case "cron":
		var p CronParams
		if err = json.Unmarshal(params, &p); err == nil {
			out, err = Cron(p)
		}
	case "backoff":
		var p BackoffParams
		if err = json.Unmarshal(params, &p); err == nil {
			out, err = Backoff(p)
		}
	default:
		err = fmt.Errorf("unknown method %q", method)
	}
	if err != nil {
		return errorResult(err)
	}
	return out
}

func errorResult(err error) map[string]string {
	return map[string]string{"error": err.Error()}
}

// Cron parses p.Expr with cronx and walks Next from p.Start the way the
// scheduler does: each run is Next of the previous one.
func Cron(p CronParams) (CronResult, error) {
	if p.Count < 1 || p.Count > maxRuns {
		return CronResult{}, fmt.Errorf("count must be 1..%d, got %d", maxRuns, p.Count)
	}
	s, err := cronx.Parse(p.Expr)
	if err != nil {
		return CronResult{}, err
	}
	loc := time.UTC
	if p.TZ != "" {
		if loc, err = time.LoadLocation(p.TZ); err != nil {
			return CronResult{}, fmt.Errorf("time zone %q: %w", p.TZ, err)
		}
	}
	at, err := time.Parse(time.RFC3339, p.Start)
	if err != nil {
		return CronResult{}, fmt.Errorf("start must be RFC 3339, e.g. 2026-03-08T00:00:00Z: %w", err)
	}
	out := CronResult{Normalized: s.String(), Runs: []Run{}}
	for range p.Count {
		at = s.Next(at, loc)
		if at.IsZero() {
			break
		}
		local := at.In(loc)
		zone, _ := local.Zone()
		out.Runs = append(out.Runs, Run{
			UTC:   at.UTC().Format(time.RFC3339),
			Local: local.Format("Mon 2006-01-02 15:04"),
			Zone:  zone,
		})
	}
	return out, nil
}

// Backoff reads the policy through its own UnmarshalJSON and Validate, then
// lists Policy.Cap and Policy.Next for every attempt below MaxAttempts.
func Backoff(p BackoffParams) (BackoffResult, error) {
	if p.MaxAttempts < 1 || p.MaxAttempts > maxAttempts {
		return BackoffResult{}, fmt.Errorf("max attempts must be 1..%d, got %d", maxAttempts, p.MaxAttempts)
	}
	if p.Samples < 1 || p.Samples > maxSamples {
		return BackoffResult{}, fmt.Errorf("samples must be 1..%d, got %d", maxSamples, p.Samples)
	}
	var pol job.Policy
	if len(p.Policy) > 0 {
		if err := json.Unmarshal(p.Policy, &pol); err != nil {
			return BackoffResult{}, err
		}
	}
	if err := pol.Validate(); err != nil {
		return BackoffResult{}, err
	}
	out := BackoffResult{Policy: pol.OrDefault(), Retries: []Retry{}}
	rnds := make([]*rand.Rand, p.Samples)
	for i := range rnds {
		rnds[i] = rand.New(rand.NewPCG(p.Seed, uint64(i)))
	}
	for a := int32(1); a < p.MaxAttempts; a++ {
		r := Retry{Attempt: a, CapMs: ms(pol.Cap(a)), DelaysMs: make([]int64, len(rnds))}
		for i, rnd := range rnds {
			r.DelaysMs[i] = pol.Next(a, rnd).Milliseconds()
		}
		out.Retries = append(out.Retries, r)
	}
	return out, nil
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
