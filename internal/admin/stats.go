package admin

import (
	"encoding/json"
	"net/http"
	"time"
)

// StatsDeps is what /admin/stats reports.
type StatsDeps struct {
	Depth     *DepthCache
	Broker    *Broker
	Version   string
	StartedAt time.Time
	// Pool returns the worker pool's Stats() (any JSON-marshalable value);
	// nil omits the field. It is `any` so admin does not import internal/worker.
	Pool func() any
	// Counters is the same func /metrics reads; nil omits the field.
	Counters func() Counters
}

type statsJSON struct {
	Version       string      `json:"version"`
	StartedAt     time.Time   `json:"started_at"`
	UptimeSeconds float64     `json:"uptime_seconds"`
	DepthAsOf     *time.Time  `json:"depth_as_of,omitempty"`
	Depth         []depthJSON `json:"depth"`
	Subscribers   int         `json:"sse_subscribers"`
	Dropped       uint64      `json:"sse_dropped"`
	Counters      *counters   `json:"counters,omitempty"`
	Pool          any         `json:"pool,omitempty"`
}

type counters struct {
	Jobs           []jobCount `json:"jobs"`
	Dead           uint64     `json:"dead"`
	LeaseExpired   uint64     `json:"lease_expired"`
	SchedulerTicks uint64     `json:"scheduler_ticks"`
	LeaseFenced    uint64     `json:"lease_fenced"`
}

type jobCount struct {
	Queue  string `json:"queue"`
	Status string `json:"status"`
	N      uint64 `json:"n"`
}

// Stats serves a JSON summary: version, uptime, the cached depth table, SSE
// subscriber/drop counts, the outcome counters and the pool stats. It sends
// no CORS headers itself; Handler wraps it in the shared origin allowlist.
func Stats(d StatsDeps) http.Handler {
	return statsHandler{d: d, now: time.Now}
}

type statsHandler struct {
	d   StatsDeps
	now func() time.Time
}

func (h statsHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	now := h.now()
	out := statsJSON{
		Version:       h.d.Version,
		StartedAt:     h.d.StartedAt,
		UptimeSeconds: max(0, now.Sub(h.d.StartedAt).Seconds()),
		Depth:         []depthJSON{},
	}
	if h.d.Depth != nil {
		rows, at := h.d.Depth.Depth()
		out.Depth = depthsJSON(rows)
		if !at.IsZero() {
			out.DepthAsOf = &at
		}
	}
	if h.d.Broker != nil {
		out.Subscribers = h.d.Broker.Subscribers()
		out.Dropped = h.d.Broker.Dropped()
	}
	if h.d.Counters != nil {
		c := h.d.Counters()
		cj := &counters{Jobs: make([]jobCount, 0, len(c.Jobs)), Dead: c.Dead, LeaseExpired: c.LeaseExpired, SchedulerTicks: c.SchedulerTicks, LeaseFenced: c.LeaseFenced}
		for k, n := range c.Jobs {
			cj.Jobs = append(cj.Jobs, jobCount{Queue: k.Queue, Status: k.Status, N: n})
		}
		sortJobCounts(cj.Jobs)
		out.Counters = cj
	}
	if h.d.Pool != nil {
		out.Pool = h.d.Pool()
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(out)
}

func sortJobCounts(js []jobCount) {
	for i := 1; i < len(js); i++ { // insertion sort: a handful of rows
		for j := i; j > 0 && less(js[j], js[j-1]); j-- {
			js[j], js[j-1] = js[j-1], js[j]
		}
	}
}

func less(a, b jobCount) bool {
	if a.Queue != b.Queue {
		return a.Queue < b.Queue
	}
	return a.Status < b.Status
}
