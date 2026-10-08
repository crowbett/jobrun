package interceptor

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// AuthHeader is the metadata key carrying "Bearer <GRPC_TOKEN>".
const AuthHeader = "authorization"

// healthPrefix is the one service reachable without a token: liveness and
// readiness probes (kubelet `grpc:` probes, grpc_health_probe) cannot send one.
const healthPrefix = "/grpc.health.v1.Health/"

// UnaryAuth rejects every call except the health service unless it carries
// "authorization: Bearer <token>". An empty token disables the check (the
// config layer decides when that is allowed).
func UnaryAuth(token string) grpc.UnaryServerInterceptor {
	if token == "" {
		return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			return h(ctx, req)
		}
	}
	want := sha256.Sum256([]byte(token))
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if err := authorize(ctx, info.FullMethod, want); err != nil {
			return nil, err
		}
		return h(ctx, req)
	}
}

// StreamAuth is UnaryAuth for streaming RPCs (WorkerService, reflection).
func StreamAuth(token string) grpc.StreamServerInterceptor {
	if token == "" {
		return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			return h(srv, ss)
		}
	}
	want := sha256.Sum256([]byte(token))
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		if err := authorize(ss.Context(), info.FullMethod, want); err != nil {
			return err
		}
		return h(srv, ss)
	}
}

// authorize compares the presented bearer token with want in constant time.
// Both sides are hashed first, so the comparison time does not depend on the
// presented token's length. The error never echoes what was sent.
func authorize(ctx context.Context, method string, want [sha256.Size]byte) error {
	if strings.HasPrefix(method, healthPrefix) {
		return nil
	}
	md, _ := metadata.FromIncomingContext(ctx)
	vals := md.Get(AuthHeader)
	if len(vals) != 1 {
		return status.Error(codes.Unauthenticated, "missing bearer token")
	}
	tok, ok := strings.CutPrefix(vals[0], "Bearer ")
	if !ok {
		return status.Error(codes.Unauthenticated, "missing bearer token")
	}
	got := sha256.Sum256([]byte(tok))
	if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
		return status.Error(codes.Unauthenticated, "invalid bearer token")
	}
	return nil
}

// Bearer is the client side: per-RPC credentials that send
// "authorization: Bearer <token>". jobrund speaks plaintext gRPC, so the
// token crosses the wire in clear; put TLS (or a private network) in front
// when it leaves the host.
type Bearer string

var _ credentials.PerRPCCredentials = Bearer("")

// GetRequestMetadata implements credentials.PerRPCCredentials.
func (b Bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{AuthHeader: "Bearer " + string(b)}, nil
}

// RequireTransportSecurity implements credentials.PerRPCCredentials: false,
// because the transport is insecure by design (see the type comment).
func (Bearer) RequireTransportSecurity() bool { return false }
