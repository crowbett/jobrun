package interceptor

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestValidRequestID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		id   string
		ok   bool
	}{
		{"hex", "0123456789abcdef01234567", true},
		{"uuid", "018f1c2e-6b3a-7c4d-9e5f-1a2b3c4d5e6f", true},
		{"punctuation", "req/1:a.b_c-d~!", true},
		{"at cap", strings.Repeat("x", MaxRequestIDLen), true},
		{"empty", "", false},
		{"over cap", strings.Repeat("x", MaxRequestIDLen+1), false},
		{"space", "a b", false},
		{"newline (log injection)", "ok\nlevel=ERROR msg=forged", false},
		{"tab", "a\tb", false},
		{"del", "a\x7fb", false},
		{"nul", "a\x00b", false},
		{"non-ascii", "requête", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := validRequestID(tc.id); got != tc.ok {
				t.Fatalf("validRequestID(%q) = %v, want %v", tc.id, got, tc.ok)
			}
		})
	}
}

func TestWithRequestID(t *testing.T) {
	t.Parallel()
	t.Run("reuses a valid incoming id", func(t *testing.T) {
		t.Parallel()
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(RequestIDHeader, "client-42"))
		if got := RequestIDFrom(withRequestID(ctx)); got != "client-42" {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("replaces a hostile incoming id", func(t *testing.T) {
		t.Parallel()
		hostile := strings.Repeat("A", MaxRequestIDLen+1)
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(RequestIDHeader, hostile))
		got := RequestIDFrom(withRequestID(ctx))
		if got == "" || got == hostile || len(got) > MaxRequestIDLen || !validRequestID(got) {
			t.Fatalf("hostile id not replaced: %q", got)
		}
	})
	t.Run("generates when absent", func(t *testing.T) {
		t.Parallel()
		a, b := RequestIDFrom(withRequestID(context.Background())), RequestIDFrom(withRequestID(context.Background()))
		if a == "" || a == b || !validRequestID(a) {
			t.Fatalf("generated ids %q %q", a, b)
		}
	})
	if RequestIDFrom(context.Background()) != "" {
		t.Fatal("no id must read as empty")
	}
}

func TestUnaryChain(t *testing.T) {
	// The subtests share buf, so they run sequentially on purpose.
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	info := &grpc.UnaryServerInfo{FullMethod: "/jobrun.v1.JobService/Enqueue"}
	chain := func(h grpc.UnaryHandler) grpc.UnaryHandler {
		rec, rid, lg := UnaryRecover(logger), UnaryRequestID(), UnaryLogging(logger)
		return func(ctx context.Context, req any) (any, error) {
			return rec(ctx, req, info, func(ctx context.Context, req any) (any, error) {
				return rid(ctx, req, info, func(ctx context.Context, req any) (any, error) {
					return lg(ctx, req, info, h)
				})
			})
		}
	}

	t.Run("ok is logged at info with the request id", func(t *testing.T) {
		buf.Reset()
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(RequestIDHeader, "rid-1"))
		var seen string
		resp, err := chain(func(ctx context.Context, _ any) (any, error) {
			seen = RequestIDFrom(ctx)
			return "resp", nil
		})(ctx, "req")
		if err != nil || resp != "resp" || seen != "rid-1" {
			t.Fatalf("resp %v err %v seen %q", resp, err, seen)
		}
		if out := buf.String(); !strings.Contains(out, "level=INFO") || !strings.Contains(out, "request_id=rid-1") || !strings.Contains(out, "code=OK") {
			t.Fatalf("log: %s", out)
		}
	})
	t.Run("status errors keep their code and log at warn", func(t *testing.T) {
		buf.Reset()
		_, err := chain(func(context.Context, any) (any, error) {
			return nil, status.Error(codes.InvalidArgument, "bad")
		})(context.Background(), nil)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("code %v", status.Code(err))
		}
		if out := buf.String(); !strings.Contains(out, "level=WARN") || !strings.Contains(out, "code=InvalidArgument") {
			t.Fatalf("log: %s", out)
		}
	})
	t.Run("internal errors log at error", func(t *testing.T) {
		buf.Reset()
		_, _ = chain(func(context.Context, any) (any, error) {
			return nil, status.Error(codes.Internal, "boom")
		})(context.Background(), nil)
		if out := buf.String(); !strings.Contains(out, "level=ERROR") {
			t.Fatalf("log: %s", out)
		}
	})
	t.Run("panic becomes Internal without leaking the value", func(t *testing.T) {
		buf.Reset()
		resp, err := chain(func(context.Context, any) (any, error) {
			panic("secret database password")
		})(context.Background(), nil)
		if resp != nil || status.Code(err) != codes.Internal {
			t.Fatalf("resp %v err %v", resp, err)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatalf("panic value leaked to the client: %v", err)
		}
		if out := buf.String(); !strings.Contains(out, "panic recovered") || !strings.Contains(out, "secret database password") || !strings.Contains(out, "stack=") {
			t.Fatalf("panic must be logged with its value and stack: %s", out)
		}
	})
}

type fakeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f fakeStream) Context() context.Context { return f.ctx }

func TestStreamChain(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	info := &grpc.StreamServerInfo{FullMethod: "/jobrun.v1.JobService/Watch"}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(RequestIDHeader, "s-1"))
	rec, rid, lg := StreamRecover(logger), StreamRequestID(), StreamLogging(logger)
	run := func(h grpc.StreamHandler) error {
		return rec(nil, fakeStream{ctx: ctx}, info, func(srv any, ss grpc.ServerStream) error {
			return rid(srv, ss, info, func(srv any, ss grpc.ServerStream) error {
				return lg(srv, ss, info, h)
			})
		})
	}

	var seen string
	if err := run(func(_ any, ss grpc.ServerStream) error {
		seen = RequestIDFrom(ss.Context())
		return nil
	}); err != nil || seen != "s-1" {
		t.Fatalf("err %v seen %q", err, seen)
	}
	if out := buf.String(); !strings.Contains(out, "kind=stream") || !strings.Contains(out, "request_id=s-1") {
		t.Fatalf("log: %s", out)
	}

	buf.Reset()
	err := run(func(any, grpc.ServerStream) error { panic(errors.New("kaboom")) })
	if status.Code(err) != codes.Internal || strings.Contains(err.Error(), "kaboom") {
		t.Fatalf("stream panic: %v", err)
	}
	if !strings.Contains(buf.String(), "panic recovered") {
		t.Fatalf("log: %s", buf.String())
	}
}
