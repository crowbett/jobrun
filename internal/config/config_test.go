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
		{name: "admin wildcard refused", env: map[string]string{"ADMIN_ADDR": ":9110"}, wantErr: "ADMIN_ADDR \":9110\" is not a loopback address"},
		{name: "admin 0.0.0.0 refused", env: map[string]string{"ADMIN_ADDR": "0.0.0.0:9110"}, wantErr: "not a loopback address"},
		{name: "admin lan ip refused", env: map[string]string{"ADMIN_ADDR": "192.168.1.5:9110"}, wantErr: "not a loopback address"},
		{
			name: "admin wildcard with opt-in",
			env:  map[string]string{"ADMIN_ADDR": ":9110", "ADMIN_ALLOW_REMOTE": "true"},
			check: func(t *testing.T, c Config) {
				t.Helper()
				if !c.AdminAllowRemote || c.AdminAddr != ":9110" {
					t.Errorf("opt-in not applied: %+v", c)
				}
			},
		},
		{
			name: "admin ipv6 loopback",
			env:  map[string]string{"ADMIN_ADDR": "[::1]:9110"},
			check: func(t *testing.T, c Config) {
				t.Helper()
				if c.AdminAddr != "[::1]:9110" {
					t.Errorf("got %q", c.AdminAddr)
				}
			},
		},
		{name: "bad admin opt-in", env: map[string]string{"ADMIN_ALLOW_REMOTE": "maybe"}, wantErr: "ADMIN_ALLOW_REMOTE"},
		{name: "bad scheduler bool", env: map[string]string{"SCHEDULER_ENABLED": "sometimes"}, wantErr: "SCHEDULER_ENABLED"},
		{name: "bad log level", env: map[string]string{"LOG_LEVEL": "loud"}, wantErr: "LOG_LEVEL"},
		{name: "bad lock key", env: map[string]string{"SCHEDULER_LOCK_KEY": "abc"}, wantErr: "SCHEDULER_LOCK_KEY"},
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

func TestIsLoopbackAddr(t *testing.T) {
	t.Parallel()
	tests := []struct {
		addr string
		want bool
	}{
		{"127.0.0.1:9110", true},
		{"127.5.5.5:9110", true},
		{"[::1]:9110", true},
		{"localhost:9110", true},
		{":9110", false},
		{"0.0.0.0:9110", false},
		{"[::]:9110", false},
		{"192.168.1.5:9110", false},
		{"10.0.0.1:9110", false},
		{"example.com:9110", false},
		{"127.0.0.1", false}, // no port: not a listen address
		{"", false},
	}
	for _, tc := range tests {
		t.Run(tc.addr, func(t *testing.T) {
			t.Parallel()
			if got := IsLoopbackAddr(tc.addr); got != tc.want {
				t.Fatalf("IsLoopbackAddr(%q) = %v, want %v", tc.addr, got, tc.want)
			}
		})
	}
}

func TestValidateOTelEndpoint(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.OTel = OTel{Enabled: true}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "OTEL_EXPORTER_OTLP_ENDPOINT") {
		t.Fatalf("Validate: %v", err)
	}
}
