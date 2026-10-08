package interceptor

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestUnaryAuth(t *testing.T) {
	t.Parallel()
	const tok = "s3cret-token-0123456"
	jobs := &grpc.UnaryServerInfo{FullMethod: "/jobrun.v1.JobService/Enqueue"}
	healthInfo := &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}
	tests := []struct {
		name   string
		token  string
		info   *grpc.UnaryServerInfo
		md     metadata.MD
		want   codes.Code
		called bool
	}{
		{name: "right token", token: tok, info: jobs, md: metadata.Pairs(AuthHeader, "Bearer "+tok), want: codes.OK, called: true},
		{name: "no metadata", token: tok, info: jobs, want: codes.Unauthenticated},
		{name: "no bearer prefix", token: tok, info: jobs, md: metadata.Pairs(AuthHeader, tok), want: codes.Unauthenticated},
		{name: "wrong token", token: tok, info: jobs, md: metadata.Pairs(AuthHeader, "Bearer nope"), want: codes.Unauthenticated},
		{name: "prefix of the token", token: tok, info: jobs, md: metadata.Pairs(AuthHeader, "Bearer "+tok[:8]), want: codes.Unauthenticated},
		{name: "two headers", token: tok, info: jobs, md: metadata.Pairs(AuthHeader, "Bearer "+tok, AuthHeader, "Bearer x"), want: codes.Unauthenticated},
		{name: "health is open", token: tok, info: healthInfo, want: codes.OK, called: true},
		{name: "empty token disables", token: "", info: jobs, want: codes.OK, called: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if tc.md != nil {
				ctx = metadata.NewIncomingContext(ctx, tc.md)
			}
			called := false
			_, err := UnaryAuth(tc.token)(ctx, nil, tc.info, func(context.Context, any) (any, error) {
				called = true
				return "ok", nil
			})
			if status.Code(err) != tc.want || called != tc.called {
				t.Fatalf("err %v called %v; want %v called %v", err, called, tc.want, tc.called)
			}
			if err != nil && strings.Contains(err.Error(), "nope") {
				t.Fatalf("error echoes the presented token: %v", err)
			}
		})
	}
}

func TestStreamAuth(t *testing.T) {
	t.Parallel()
	const tok = "s3cret-token-0123456"
	info := &grpc.StreamServerInfo{FullMethod: "/jobrun.v1.WorkerService/Claim"}
	h := func(any, grpc.ServerStream) error { return nil }
	ok := fakeStream{ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs(AuthHeader, "Bearer "+tok))}
	if err := StreamAuth(tok)(nil, ok, info, h); err != nil {
		t.Fatalf("right token: %v", err)
	}
	bad := fakeStream{ctx: context.Background()}
	if err := StreamAuth(tok)(nil, bad, info, h); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("no token: %v", err)
	}
}

func TestBearer(t *testing.T) {
	t.Parallel()
	md, err := Bearer("abc").GetRequestMetadata(context.Background())
	if err != nil || md[AuthHeader] != "Bearer abc" || Bearer("abc").RequireTransportSecurity() {
		t.Fatalf("md %v err %v", md, err)
	}
}
