// Package config loads the jobrund configuration from the environment. No
// globals: Load returns a value that cmd/jobrund threads into every
// constructor, and Validate reports every problem at once.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is everything cmd/jobrund needs.
type Config struct {
	ServiceName string // SERVICE_NAME, default "jobrun"
	Env         string // APP_ENV: dev | staging | prod
	GRPCAddr    string // GRPC_ADDR, default ":50052"
	AdminAddr   string // ADMIN_ADDR, default "127.0.0.1:9110"
	DatabaseURL string // DATABASE_URL (required)
	PGMaxConns  int    // PGX_MAX_CONNS, default Worker.Concurrency + 8

	LogLevel  slog.Level // LOG_LEVEL
	LogFormat string     // LOG_FORMAT: json | text

	StopTimeout time.Duration // SHUTDOWN_TIMEOUT: gRPC drain budget, default 15s

	Worker    Worker
	Scheduler Scheduler
	Reaper    Reaper
	OTel      OTel
}

// Worker tunes the in-process pool.
type Worker struct {
	Queues         []string      // WORKER_QUEUES, comma list, default "default"
	Concurrency    int           // WORKER_CONCURRENCY, default 16
	BatchSize      int           // WORKER_BATCH, default 32
	LeaseTTL       time.Duration // LEASE_TTL, default 30s
	HeartbeatEvery time.Duration // HEARTBEAT_EVERY, default 10s
	JobTimeout     time.Duration // JOB_TIMEOUT, default 10m
	PollFloor      time.Duration // POLL_FLOOR, default 200ms
	Events         string        // JOB_EVENTS: all | terminal, default all
}

// Scheduler tunes cron materialisation.
type Scheduler struct {
	Enabled bool          // SCHEDULER_ENABLED, default true
	Tick    time.Duration // SCHEDULER_TICK, default 5s
	LockKey int64         // SCHEDULER_LOCK_KEY, default 0x6a6f6272756e ("jobrun")
}

// Reaper tunes lease recovery and retention.
type Reaper struct {
	Retention time.Duration // JOB_RETENTION, default 168h
	Batch     int           // REAPER_BATCH, default 500
}

// OTel selects telemetry export.
type OTel struct {
	Enabled  bool   // OTEL_ENABLED
	Endpoint string // OTEL_EXPORTER_OTLP_ENDPOINT, default http://localhost:4317
}

// Load reads the environment and validates the result.
func Load() (Config, error) {
	cfg := Config{
		ServiceName: getenv("SERVICE_NAME", "jobrun"),
		Env:         getenv("APP_ENV", "dev"),
		GRPCAddr:    getenv("GRPC_ADDR", ":50052"),
		AdminAddr:   getenv("ADMIN_ADDR", "127.0.0.1:9110"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		LogFormat:   strings.ToLower(getenv("LOG_FORMAT", "json")),
		StopTimeout: 15 * time.Second,
		Worker: Worker{
			Queues: splitList(getenv("WORKER_QUEUES", "default")), Concurrency: 16, BatchSize: 32,
			LeaseTTL: 30 * time.Second, HeartbeatEvery: 10 * time.Second, JobTimeout: 10 * time.Minute,
			PollFloor: 200 * time.Millisecond, Events: strings.ToLower(getenv("JOB_EVENTS", "all")),
		},
		Scheduler: Scheduler{Enabled: true, Tick: 5 * time.Second, LockKey: 0x6a6f6272756e},
		Reaper:    Reaper{Retention: 168 * time.Hour, Batch: 500},
		OTel:      OTel{Endpoint: getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")},
	}

	var err error
	if err = cfg.LogLevel.UnmarshalText([]byte(getenv("LOG_LEVEL", "info"))); err != nil {
		return Config{}, fmt.Errorf("LOG_LEVEL: %w", err)
	}
	durs := []struct {
		key string
		dst *time.Duration
	}{
		{"SHUTDOWN_TIMEOUT", &cfg.StopTimeout},
		{"LEASE_TTL", &cfg.Worker.LeaseTTL},
		{"HEARTBEAT_EVERY", &cfg.Worker.HeartbeatEvery},
		{"JOB_TIMEOUT", &cfg.Worker.JobTimeout},
		{"POLL_FLOOR", &cfg.Worker.PollFloor},
		{"SCHEDULER_TICK", &cfg.Scheduler.Tick},
		{"JOB_RETENTION", &cfg.Reaper.Retention},
	}
	for _, d := range durs {
		if *d.dst, err = getDuration(d.key, *d.dst); err != nil {
			return Config{}, err
		}
	}
	ints := []struct {
		key string
		dst *int
	}{
		{"WORKER_CONCURRENCY", &cfg.Worker.Concurrency},
		{"WORKER_BATCH", &cfg.Worker.BatchSize},
		{"REAPER_BATCH", &cfg.Reaper.Batch},
		{"PGX_MAX_CONNS", &cfg.PGMaxConns},
	}
	for _, i := range ints {
		if *i.dst, err = getInt(i.key, *i.dst); err != nil {
			return Config{}, err
		}
	}
	if cfg.PGMaxConns == 0 {
		cfg.PGMaxConns = cfg.Worker.Concurrency + 8
	}
	var key int
	if key, err = getInt("SCHEDULER_LOCK_KEY", int(cfg.Scheduler.LockKey)); err != nil {
		return Config{}, err
	}
	cfg.Scheduler.LockKey = int64(key)
	if cfg.Scheduler.Enabled, err = getBool("SCHEDULER_ENABLED", true); err != nil {
		return Config{}, err
	}
	if cfg.OTel.Enabled, err = getBool("OTEL_ENABLED", false); err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate reports every problem at once.
func (c Config) Validate() error {
	var errs []error
	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	if c.GRPCAddr == "" {
		errs = append(errs, errors.New("GRPC_ADDR must not be empty"))
	}
	if c.AdminAddr == "" {
		errs = append(errs, errors.New("ADMIN_ADDR must not be empty"))
	}
	if c.LogFormat != "json" && c.LogFormat != "text" {
		errs = append(errs, fmt.Errorf("LOG_FORMAT must be json or text, got %q", c.LogFormat))
	}
	if c.StopTimeout <= 0 {
		errs = append(errs, errors.New("SHUTDOWN_TIMEOUT must be > 0"))
	}
	if len(c.Worker.Queues) == 0 {
		errs = append(errs, errors.New("WORKER_QUEUES must list at least one queue"))
	}
	if c.Worker.Concurrency < 1 || c.Worker.Concurrency > 1024 {
		errs = append(errs, fmt.Errorf("WORKER_CONCURRENCY must be 1..1024, got %d", c.Worker.Concurrency))
	}
	if c.Worker.BatchSize < 1 || c.Worker.BatchSize > 1000 {
		errs = append(errs, fmt.Errorf("WORKER_BATCH must be 1..1000, got %d", c.Worker.BatchSize))
	}
	if c.Worker.LeaseTTL < time.Second {
		errs = append(errs, errors.New("LEASE_TTL must be >= 1s"))
	}
	if c.Worker.HeartbeatEvery <= 0 || c.Worker.HeartbeatEvery >= c.Worker.LeaseTTL/2 {
		errs = append(errs, fmt.Errorf("HEARTBEAT_EVERY must be > 0 and < LEASE_TTL/2 (%s)", c.Worker.LeaseTTL/2))
	}
	if c.Worker.JobTimeout <= 0 {
		errs = append(errs, errors.New("JOB_TIMEOUT must be > 0"))
	}
	if c.Worker.PollFloor < 10*time.Millisecond {
		errs = append(errs, errors.New("POLL_FLOOR must be >= 10ms"))
	}
	if c.Worker.Events != "all" && c.Worker.Events != "terminal" {
		errs = append(errs, fmt.Errorf("JOB_EVENTS must be all or terminal, got %q", c.Worker.Events))
	}
	if c.Scheduler.Tick < 100*time.Millisecond {
		errs = append(errs, errors.New("SCHEDULER_TICK must be >= 100ms"))
	}
	if c.Reaper.Retention < time.Minute {
		errs = append(errs, errors.New("JOB_RETENTION must be >= 1m"))
	}
	if c.Reaper.Batch < 1 {
		errs = append(errs, errors.New("REAPER_BATCH must be >= 1"))
	}
	if c.PGMaxConns < c.Worker.Concurrency+2 {
		errs = append(errs, fmt.Errorf("PGX_MAX_CONNS must be >= WORKER_CONCURRENCY+2 (%d)", c.Worker.Concurrency+2))
	}
	if c.OTel.Enabled && c.OTel.Endpoint == "" {
		errs = append(errs, errors.New("OTEL_EXPORTER_OTLP_ENDPOINT is required when OTEL_ENABLED=true"))
	}
	return errors.Join(errs...)
}

func splitList(s string) []string {
	var out []string
	for p := range strings.SplitSeq(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

func getInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func getBool(key string, def bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s: %w", key, err)
	}
	return b, nil
}
