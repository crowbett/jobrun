package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/crowbett/jobrun/internal/job"
)

func testDeps(t *testing.T) Deps {
	t.Helper()
	b := NewBroker(nil)
	b.Publish(job.Event{Queue: "default", From: job.StatusLeased, To: job.StatusDead})
	return Deps{
		Broker:    b,
		Depth:     readyCache(t, []job.DepthRow{{Queue: "default", Status: job.StatusQueued, N: 4, OldestRunAt: fixedNow}}),
		Counters:  func() Counters { c := b.Counters(); c.LeaseExpired, c.SchedulerTicks = 5, 6; return c },
		Durations: fullHistogram(),
		Build:     map[string]string{"version": "v9"},
		Version:   "v9",
		StartedAt: time.Now().Add(-time.Minute),
		Pool:      func() any { return map[string]int{"in_flight": 2} },
	}
}

func TestHandlerRoutes(t *testing.T) {
	t.Parallel()
	h := Handler(testDeps(t))
	tests := []struct {
		method, path string
		wantCode     int
		wantCT       string
		wantBody     string
	}{
		{http.MethodGet, "/admin", http.StatusOK, "text/html", "<title>jobrun</title>"},
		{http.MethodGet, "/admin/stats", http.StatusOK, "application/json", `"version":"v9"`},
		{http.MethodGet, "/metrics", http.StatusOK, ContentType, "jobrun_queue_depth{queue=\"default\",status=\"queued\"} 4\n"},
		{http.MethodGet, "/healthz", http.StatusOK, "text/plain", "ok\n"},
		{http.MethodPost, "/admin/stats", http.StatusMethodNotAllowed, "", ""},
		{http.MethodGet, "/nope", http.StatusNotFound, "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			t.Parallel()
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest(tt.method, tt.path, http.NoBody))
			if rr.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d", rr.Code, tt.wantCode)
			}
			if tt.wantCT != "" && !strings.HasPrefix(rr.Header().Get("Content-Type"), tt.wantCT) {
				t.Errorf("Content-Type = %q, want prefix %q", rr.Header().Get("Content-Type"), tt.wantCT)
			}
			if tt.wantBody != "" && !strings.Contains(rr.Body.String(), tt.wantBody) {
				t.Errorf("body lacks %q:\n%s", tt.wantBody, rr.Body.String())
			}
		})
	}
}

func TestStatsJSON(t *testing.T) {
	t.Parallel()
	rr := httptest.NewRecorder()
	Handler(testDeps(t)).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/admin/stats", http.NoBody))
	var got struct {
		Version       string  `json:"version"`
		UptimeSeconds float64 `json:"uptime_seconds"`
		DepthAsOf     string  `json:"depth_as_of"`
		Depth         []struct {
			Queue  string `json:"queue"`
			Status string `json:"status"`
			N      int64  `json:"n"`
		} `json:"depth"`
		Subscribers int `json:"sse_subscribers"`
		Counters    struct {
			Jobs           []jobCount `json:"jobs"`
			Dead           uint64     `json:"dead"`
			LeaseExpired   uint64     `json:"lease_expired"`
			SchedulerTicks uint64     `json:"scheduler_ticks"`
		} `json:"counters"`
		Pool map[string]int `json:"pool"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("json: %v\n%s", err, rr.Body.String())
	}
	if got.Version != "v9" || got.UptimeSeconds < 59 || got.DepthAsOf == "" || got.Subscribers != 0 {
		t.Errorf("stats = %+v", got)
	}
	if len(got.Depth) != 1 || got.Depth[0].Status != "queued" || got.Depth[0].N != 4 {
		t.Errorf("depth = %+v", got.Depth)
	}
	if got.Counters.Dead != 1 || got.Counters.LeaseExpired != 5 || got.Counters.SchedulerTicks != 6 ||
		len(got.Counters.Jobs) != 1 || got.Counters.Jobs[0] != (jobCount{"default", "dead", 1}) {
		t.Errorf("counters = %+v", got.Counters)
	}
	if got.Pool["in_flight"] != 2 {
		t.Errorf("pool = %v", got.Pool)
	}
}

func TestStatsWithNoDeps(t *testing.T) {
	t.Parallel()
	rr := httptest.NewRecorder()
	Handler(Deps{Broker: NewBroker(nil)}).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/admin/stats", http.NoBody))
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"depth":[]`) {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestNewServerHasNoWriteTimeout(t *testing.T) {
	t.Parallel()
	srv := NewServer("127.0.0.1:0", Deps{Broker: NewBroker(nil)})
	if srv.WriteTimeout != 0 {
		t.Errorf("WriteTimeout = %v; an SSE server must not have one", srv.WriteTimeout)
	}
	if srv.ReadHeaderTimeout == 0 || srv.Handler == nil || srv.ErrorLog == nil {
		t.Errorf("server = %+v", srv)
	}
}
