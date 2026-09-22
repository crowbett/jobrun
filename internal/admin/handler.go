package admin

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/web"
)

// Deps is everything the admin surface reads. Only Broker is required; a nil
// field simply leaves its data out.
type Deps struct {
	Broker *Broker
	// Depth feeds /metrics, /admin/stats and the SSE snapshot. Run it yourself
	// (DepthCache.Run) in the process errgroup.
	Depth *DepthCache
	// Counters is read per scrape; wire it as
	// func() Counters { c := broker.Counters(); s := snap(); c.LeaseExpired = s.LeaseLost; c.SchedulerTicks = s.SchedulerTicks; return c }.
	Counters func() Counters
	// Durations is the histogram the worker feeds through DurationObserver.
	Durations *Histogram
	// Build labels jobrun_build_info (version, go_version, ...).
	Build     map[string]string
	Version   string
	StartedAt time.Time
	// Pool returns worker.Pool.Stats() for /admin/stats.
	Pool func() any
	// AllowedOrigins (ADMIN_CORS_ORIGINS) are the cross-origin pages allowed
	// to read /admin/stats and /admin/events. Empty (the default) sends no
	// Access-Control-Allow-Origin at all: only the same-origin /admin page
	// can read them.
	AllowedOrigins []string
	Log            *slog.Logger
}

// Handler mounts the admin surface on a fresh mux:
//
//	GET /admin         the embedded synthwave page
//	GET /admin/events  SSE feed (snapshot, then one event per transition)
//	GET /admin/stats   JSON summary
//	GET /metrics       Prometheus text
//	GET /healthz       liveness, "ok"
//
// Nothing here is wrapped in http.TimeoutHandler: /admin/events is a
// long-lived stream. If you add middleware around this handler, keep
// timeouts off the events route (or give the http.Server no WriteTimeout, as
// NewServer does).
func Handler(d Deps) http.Handler {
	if d.Log == nil {
		d.Log = slog.New(slog.DiscardHandler)
	}
	snapshot := func(context.Context) ([]job.DepthRow, error) {
		if d.Depth == nil {
			return nil, nil
		}
		rows, _ := d.Depth.Depth()
		return rows, nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		http.ServeFileFS(w, r, web.Admin, "admin.html")
	})
	mux.Handle("GET /admin/events", allowOrigins(d.AllowedOrigins, Events(d.Broker, snapshot)))
	mux.Handle("GET /admin/stats", allowOrigins(d.AllowedOrigins, Stats(StatsDeps{
		Depth: d.Depth, Broker: d.Broker, Version: d.Version, StartedAt: d.StartedAt, Pool: d.Pool, Counters: d.Counters,
	})))
	mux.Handle("GET /metrics", Metrics(d.Depth, d.Counters, d.Durations, d.Build))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}

// NewServer builds the admin http.Server on addr (docs/PLAN.md: 127.0.0.1:9110)
// around Handler(d). WriteTimeout is deliberately zero so the SSE stream is
// not cut; ReadHeaderTimeout and IdleTimeout bound the rest. Stop it with
// Server.Shutdown in teardown stage 1; SSE connections end with their
// request contexts.
func NewServer(addr string, d Deps) *http.Server {
	if d.Log == nil {
		d.Log = slog.New(slog.DiscardHandler)
	}
	return &http.Server{
		Addr:              addr,
		Handler:           Handler(d),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          slog.NewLogLogger(d.Log.Handler(), slog.LevelWarn),
	}
}
