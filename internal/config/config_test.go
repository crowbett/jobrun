package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.GRPCAddr != ":50052" || cfg.AdminAddr != "127.0.0.1:9110" || cfg.StopTimeout != 15*time.Second {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
	if len(cfg.Worker.Queues) != 1 || cfg.Worker.Queues[0] != "default" || cfg.Worker.Concurrency != 16 {
		t.Errorf("worker defaults: %+v", cfg.Worker)
	}
	if cfg.PGMaxConns != 24 || !cfg.Scheduler.Enabled || cfg.Reaper.Retention != 168*time.Hour {
		t.Errorf("derived defaults: pg=%d sched=%+v reaper=%+v", cfg.PGMaxConns, cfg.Scheduler, cfg.Reaper)
	}
}

func TestLoadTable(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string // substring of the error; empty means success
		check   func(t *testing.T, c Config)
	}{
		{
			name: "overrides",
			env:  map[string]string{"WORKER_QUEUES": "a, b", "WORKER_CONCURRENCY": "4", "LEASE_TTL": "20s", "HEARTBEAT_EVERY": "5s", "JOB_EVENTS": "TERMINAL"},
			check: func(t *testing.T, c Config) {
				t.Helper()
				if len(c.Worker.Queues) != 2 || c.Worker.Concurrency != 4 || c.PGMaxConns != 12 || c.Worker.Events != "terminal" {
					t.Errorf("overrides not applied: %+v pg=%d", c.Worker, c.PGMaxConns)
				}
			},
		},
		{name: "missing db", env: map[string]string{"DATABASE_URL": ""}, wantErr: "DATABASE_URL is required"},
		{name: "heartbeat too slow", env: map[string]string{"LEASE_TTL": "10s", "HEARTBEAT_EVERY": "6s"}, wantErr: "HEARTBEAT_EVERY"},
		{name: "bad events mode", env: map[string]string{"JOB_EVENTS": "some"}, wantErr: "JOB_EVENTS"},
		{name: "bad duration", env: map[string]string{"JOB_TIMEOUT": "later"}, wantErr: "JOB_TIMEOUT"},
		{name: "bad int", env: map[string]string{"WORKER_BATCH": "many"}, wantErr: "WORKER_BATCH"},
		{name: "pool too small", env: map[string]string{"PGX_MAX_CONNS": "4"}, wantErr: "PGX_MAX_CONNS"},
		{name: "several at once", env: map[string]string{"DATABASE_URL": "", "LOG_FORMAT": "yaml", "POLL_FLOOR": "1ms"}, wantErr: "POLL_FLOOR"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://x")
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			cfg, err := Load()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Load: %v", err)
				}
				tt.check(t, cfg)
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}
