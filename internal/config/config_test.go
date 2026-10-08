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
	if len(cfg.AdminCORSOrigins) != 0 {
		t.Errorf("AdminCORSOrigins default = %q, want empty", cfg.AdminCORSOrigins)
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
		{name: "prod grpc wildcard without token refused", env: map[string]string{"APP_ENV": "prod", "GRPC_ADDR": ":50052"}, wantErr: "GRPC_TOKEN is empty"},
		{name: "short grpc token refused", env: map[string]string{"GRPC_TOKEN": "short"}, wantErr: "GRPC_TOKEN must be at least"},
		{
			name: "prod grpc loopback without token",
			env:  map[string]string{"APP_ENV": "prod", "GRPC_ADDR": "127.0.0.1:50052"},
			check: func(t *testing.T, c Config) {
				t.Helper()
				if c.GRPCOpen() {
					t.Error("loopback bind reported as open")
				}
			},
		},
		{
			name: "prod grpc wildcard with token",
			env:  map[string]string{"APP_ENV": "prod", "GRPC_ADDR": ":50052", "GRPC_TOKEN": "0123456789abcdef"},
			check: func(t *testing.T, c Config) {
				t.Helper()
				if c.GRPCToken != "0123456789abcdef" || c.GRPCOpen() {
					t.Errorf("token not applied: open=%v", c.GRPCOpen())
				}
			},
		},
		{
			name: "dev grpc wildcard without token is allowed but open",
			env:  map[string]string{"GRPC_ADDR": ":50052"},
			check: func(t *testing.T, c Config) {
				t.Helper()
				if !c.GRPCOpen() {
					t.Error("wildcard without token not reported as open")
				}
			},
		},
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
		{
			name: "admin cors allowlist",
			env:  map[string]string{"ADMIN_CORS_ORIGINS": "http://localhost:3000, https://ops.example.com"},
			check: func(t *testing.T, c Config) {
				t.Helper()
				if len(c.AdminCORSOrigins) != 2 || c.AdminCORSOrigins[0] != "http://localhost:3000" || c.AdminCORSOrigins[1] != "https://ops.example.com" {
					t.Errorf("AdminCORSOrigins = %q", c.AdminCORSOrigins)
				}
			},
		},
		{name: "admin cors wildcard refused", env: map[string]string{"ADMIN_CORS_ORIGINS": "*"}, wantErr: "ADMIN_CORS_ORIGINS"},
		{name: "admin cors null refused", env: map[string]string{"ADMIN_CORS_ORIGINS": "null"}, wantErr: "ADMIN_CORS_ORIGINS"},
		{name: "admin cors path refused", env: map[string]string{"ADMIN_CORS_ORIGINS": "http://localhost:3000/admin"}, wantErr: "ADMIN_CORS_ORIGINS"},
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

func TestIsOrigin(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   string
		want bool
	}{
		{"http://localhost:3000", true},
		{"https://ops.example.com", true},
		{"http://127.0.0.1:9110/", true},
		{"http://[::1]:8080", true},
		{"*", false},
		{"null", false},
		{"", false},
		{"localhost:3000", false},
		{"file:///tmp/x.html", false},
		{"ftp://example.com", false},
		{"http://example.com/path", false},
		{"http://example.com?q=1", false},
		{"http://example.com#f", false},
		{"http://user@example.com", false},
		{"http://:8080", false},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			if got := IsOrigin(tc.in); got != tc.want {
				t.Errorf("IsOrigin(%q) = %v, want %v", tc.in, got, tc.want)
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
