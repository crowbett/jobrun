package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/crowbett/jobrun/internal/job"
)

func readyCache(t *testing.T, rows []job.DepthRow) *DepthCache {
	t.Helper()
	c := NewDepthCache(func(context.Context) ([]job.DepthRow, error) { return rows, nil }, time.Hour, nil)
	if err := c.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.at = fixedNow.Add(-2500 * time.Millisecond)
	c.mu.Unlock()
	return c
}

func fullCounters() func() Counters {
	return func() Counters {
		return Counters{
			Jobs: map[JobKey]uint64{
				{"mail", OutcomeSucceeded}: 7, {"default", OutcomeDead}: 2, {"default", OutcomeSucceeded}: 120,
				{"default", OutcomeRetried}: 9, {"default", OutcomeCancelled}: 1,
			},
			Dead: 2, LeaseExpired: 4, SchedulerTicks: 33,
		}
	}
}

func fullHistogram() *Histogram {
	h := NewHistogram()
	h.ObserveDuration("default", "email", 3*time.Millisecond)
	h.ObserveDuration("default", "email", 40*time.Millisecond)
	h.ObserveDuration("default", "email", 40*time.Millisecond)
	h.ObserveDuration("default", "email", 2*time.Second)
	h.ObserveDuration("default", "email", 10*time.Minute) // +Inf
	h.ObserveDuration("bench", "noop", 0)
	return h
}

const golden = `# HELP jobrun_queue_depth Jobs by queue and status.
# TYPE jobrun_queue_depth gauge
jobrun_queue_depth{queue="default",status="queued"} 12
jobrun_queue_depth{queue="default",status="leased"} 3
jobrun_queue_depth{queue="default",status="dead"} 1
jobrun_queue_depth{queue="mail",status="queued"} 0
jobrun_queue_depth{queue="we\"ird\\\n",status="succeeded"} 5
# HELP jobrun_queue_oldest_age_seconds Age of the oldest queued job's run_at per queue (0 when empty or not yet due).
# TYPE jobrun_queue_oldest_age_seconds gauge
jobrun_queue_oldest_age_seconds{queue="default"} 61.5
jobrun_queue_oldest_age_seconds{queue="mail"} 0
# HELP jobrun_depth_age_seconds Seconds since the depth table was last refreshed.
# TYPE jobrun_depth_age_seconds gauge
jobrun_depth_age_seconds 2.5
# HELP jobrun_jobs_total Job outcomes by queue and status: succeeded, dead, cancelled, retried.
# TYPE jobrun_jobs_total counter
jobrun_jobs_total{queue="default",status="cancelled"} 1
jobrun_jobs_total{queue="default",status="dead"} 2
jobrun_jobs_total{queue="default",status="retried"} 9
jobrun_jobs_total{queue="default",status="succeeded"} 120
jobrun_jobs_total{queue="mail",status="succeeded"} 7
# HELP jobrun_dead_total Jobs that reached the dead state (attempts exhausted or permanent failure).
# TYPE jobrun_dead_total counter
jobrun_dead_total 2
# HELP jobrun_lease_expired_total Leases the reaper found expired and requeued or killed.
# TYPE jobrun_lease_expired_total counter
jobrun_lease_expired_total 4
# HELP jobrun_scheduler_ticks_total Scheduler ticks run by this process while it held the leader lock.
# TYPE jobrun_scheduler_ticks_total counter
jobrun_scheduler_ticks_total 33
# HELP jobrun_job_duration_seconds Handler run time per queue and type.
# TYPE jobrun_job_duration_seconds histogram
jobrun_job_duration_seconds_bucket{queue="bench",type="noop",le="0.005"} 1
jobrun_job_duration_seconds_bucket{queue="bench",type="noop",le="0.01"} 1
jobrun_job_duration_seconds_bucket{queue="bench",type="noop",le="0.025"} 1
jobrun_job_duration_seconds_bucket{queue="bench",type="noop",le="0.05"} 1
jobrun_job_duration_seconds_bucket{queue="bench",type="noop",le="0.1"} 1
jobrun_job_duration_seconds_bucket{queue="bench",type="noop",le="0.25"} 1
jobrun_job_duration_seconds_bucket{queue="bench",type="noop",le="0.5"} 1
jobrun_job_duration_seconds_bucket{queue="bench",type="noop",le="1"} 1
jobrun_job_duration_seconds_bucket{queue="bench",type="noop",le="2.5"} 1
jobrun_job_duration_seconds_bucket{queue="bench",type="noop",le="5"} 1
jobrun_job_duration_seconds_bucket{queue="bench",type="noop",le="10"} 1
jobrun_job_duration_seconds_bucket{queue="bench",type="noop",le="30"} 1
jobrun_job_duration_seconds_bucket{queue="bench",type="noop",le="60"} 1
jobrun_job_duration_seconds_bucket{queue="bench",type="noop",le="300"} 1
jobrun_job_duration_seconds_bucket{queue="bench",type="noop",le="+Inf"} 1
jobrun_job_duration_seconds_sum{queue="bench",type="noop"} 0
jobrun_job_duration_seconds_count{queue="bench",type="noop"} 1
jobrun_job_duration_seconds_bucket{queue="default",type="email",le="0.005"} 1
jobrun_job_duration_seconds_bucket{queue="default",type="email",le="0.01"} 1
jobrun_job_duration_seconds_bucket{queue="default",type="email",le="0.025"} 1
jobrun_job_duration_seconds_bucket{queue="default",type="email",le="0.05"} 3
jobrun_job_duration_seconds_bucket{queue="default",type="email",le="0.1"} 3
jobrun_job_duration_seconds_bucket{queue="default",type="email",le="0.25"} 3
jobrun_job_duration_seconds_bucket{queue="default",type="email",le="0.5"} 3
jobrun_job_duration_seconds_bucket{queue="default",type="email",le="1"} 3
jobrun_job_duration_seconds_bucket{queue="default",type="email",le="2.5"} 4
jobrun_job_duration_seconds_bucket{queue="default",type="email",le="5"} 4
jobrun_job_duration_seconds_bucket{queue="default",type="email",le="10"} 4
jobrun_job_duration_seconds_bucket{queue="default",type="email",le="30"} 4
jobrun_job_duration_seconds_bucket{queue="default",type="email",le="60"} 4
jobrun_job_duration_seconds_bucket{queue="default",type="email",le="300"} 4
jobrun_job_duration_seconds_bucket{queue="default",type="email",le="+Inf"} 5
jobrun_job_duration_seconds_sum{queue="default",type="email"} 602.083
jobrun_job_duration_seconds_count{queue="default",type="email"} 5
# HELP jobrun_build_info Build metadata; always 1.
# TYPE jobrun_build_info gauge
jobrun_build_info{go_version="go1.27.0",version="v1.2.3"} 1
`

func serve(t *testing.T, h http.Handler) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	return rr
}

func goldenRows() []job.DepthRow {
	return []job.DepthRow{
		{Queue: "we\"ird\\\n", Status: job.StatusSucceeded, N: 5},
		{Queue: "default", Status: job.StatusDead, N: 1, OldestRunAt: fixedNow.Add(-time.Hour)},
		{Queue: "default", Status: job.StatusQueued, N: 12, OldestRunAt: fixedNow.Add(-61500 * time.Millisecond)},
		{Queue: "mail", Status: job.StatusQueued, N: 0},
		{Queue: "default", Status: job.StatusLeased, N: 3, OldestRunAt: fixedNow},
	}
}

func TestMetricsGolden(t *testing.T) {
	t.Parallel()
	h := newMetrics(readyCache(t, goldenRows()), fullCounters(), fullHistogram(),
		map[string]string{"version": "v1.2.3", "go_version": "go1.27.0"}, func() time.Time { return fixedNow })

	rr := serve(t, h)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != ContentType {
		t.Fatalf("Content-Type = %q", ct)
	}
	if got := rr.Body.String(); got != golden {
		t.Fatalf("exposition mismatch\n--- got ---\n%s\n--- want ---\n%s", got, golden)
	}
}

// lineRE accepts `name value`, `name{k="v",...} value` or a comment.
var lineRE = regexp.MustCompile(`^(#.*|[a-zA-Z_:][a-zA-Z0-9_:]*(\{[a-zA-Z_][a-zA-Z0-9_]*="(?:[^"\\]|\\.)*"(,[a-zA-Z_][a-zA-Z0-9_]*="(?:[^"\\]|\\.)*")*\})? -?(?:[0-9]+(?:\.[0-9]+)?(?:e[+-]?[0-9]+)?|NaN|[+-]?Inf))$`)

func TestMetricsEveryLineParses(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		h    *metricsHandler
	}{
		{"full", newMetrics(readyCache(t, goldenRows()), fullCounters(), fullHistogram(), nil, func() time.Time { return fixedNow })},
		{"nothing wired", newMetrics(nil, nil, nil, nil, time.Now)},
		{"empty cache, empty counters", newMetrics(NewDepthCache(nil, 0, nil), func() Counters { return Counters{} }, NewHistogram(), map[string]string{"version": "x"}, time.Now)},
		{"future oldest run_at", newMetrics(readyCache(t, []job.DepthRow{{Queue: "q", Status: job.StatusQueued, N: 1, OldestRunAt: fixedNow.Add(time.Hour)}}), nil, nil, nil, func() time.Time { return fixedNow })},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body := serve(t, tc.h).Body.String()
			if !strings.HasSuffix(body, "\n") {
				t.Fatal("body does not end with newline")
			}
			for i, line := range strings.Split(strings.TrimSuffix(body, "\n"), "\n") {
				if !lineRE.MatchString(line) {
					t.Errorf("line %d does not parse: %q", i+1, line)
				}
			}
			if !strings.Contains(body, "jobrun_build_info{") {
				t.Error("build info missing")
			}
		})
	}
}

func TestMetricsUnrefreshedCacheOmitsDepth(t *testing.T) {
	t.Parallel()
	h := newMetrics(NewDepthCache(nil, 0, nil), nil, nil, nil, time.Now)
	body := serve(t, h).Body.String()
	if strings.Contains(body, "jobrun_queue_depth") || strings.Contains(body, "jobrun_depth_age_seconds") {
		t.Errorf("depth series reported before the first refresh:\n%s", body)
	}
}

func BenchmarkMetrics(b *testing.B) {
	h := newMetrics(readyCache(&testing.T{}, goldenRows()), fullCounters(), fullHistogram(), map[string]string{"version": "bench"}, time.Now)
	req := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	w := &nopWriter{h: make(http.Header)}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		h.ServeHTTP(w, req)
	}
}

// nopWriter avoids measuring httptest.ResponseRecorder's own allocations.
type nopWriter struct{ h http.Header }

func (w *nopWriter) Header() http.Header         { return w.h }
func (w *nopWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *nopWriter) WriteHeader(int)             {}
