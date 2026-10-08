package server_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	jobrunv1 "github.com/crowbett/jobrun/gen/jobrun/v1"
	"github.com/crowbett/jobrun/internal/interceptor"
	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/internal/jobtest"
	"github.com/crowbett/jobrun/internal/server"
)

const testToken = "0123456789abcdef-test"

// authServer is the production gRPC surface (JobService, WorkerService,
// health) behind GRPC_TOKEN; the store answers Get with NotFound so a call
// that passes auth is recognisable.
func authServer(t *testing.T, cliOpts ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	fake := &fakeStore{get: func(context.Context, job.ID) (job.Job, error) { return job.Job{}, job.ErrNotFound }}
	ws := server.NewWorkerServer(jobtest.NewLeases(), nil, server.WorkerConfig{LeaseTTL: time.Second, PollFloor: 5 * time.Millisecond})
	register := func(r grpc.ServiceRegistrar) {
		server.NewJobServer(fake, slog.New(slog.DiscardHandler)).Register(r)
		ws.Register(r)
		healthv1.RegisterHealthServer(r, health.NewServer())
	}
	srvOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(interceptor.UnaryAuth(testToken)),
		grpc.ChainStreamInterceptor(interceptor.StreamAuth(testToken)),
	}
	return dial(t, register, srvOpts, cliOpts...)
}

func TestAuthOnTheWire(t *testing.T) {
	t.Parallel()
	const anID = "0190c5a2-7d1b-7c3e-8f00-000000000001"
	tests := []struct {
		name     string
		opts     []grpc.DialOption
		wantJobs codes.Code
	}{
		{name: "no token", wantJobs: codes.Unauthenticated},
		{name: "wrong token", opts: []grpc.DialOption{grpc.WithPerRPCCredentials(interceptor.Bearer("wrong-token-0000000"))}, wantJobs: codes.Unauthenticated},
		{name: "right token", opts: []grpc.DialOption{grpc.WithPerRPCCredentials(interceptor.Bearer(testToken))}, wantJobs: codes.NotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			conn := authServer(t, tc.opts...)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := jobrunv1.NewJobServiceClient(conn).Get(ctx, &jobrunv1.GetRequest{Id: anID})
			if got := status.Code(err); got != tc.wantJobs {
				t.Fatalf("JobService.Get = %v (%v), want %v", got, err, tc.wantJobs)
			}

			// The WorkerService stream is guarded too: a stranger must not lease jobs.
			// With the token the stream is open and Recv waits for work, hence
			// its own short deadline.
			sctx, scancel := context.WithTimeout(ctx, 300*time.Millisecond)
			defer scancel()
			stream, err := jobrunv1.NewWorkerServiceClient(conn).Claim(sctx)
			if err == nil {
				_, err = stream.Recv()
			}
			if tc.wantJobs == codes.Unauthenticated && status.Code(err) != codes.Unauthenticated {
				t.Fatalf("WorkerService.Claim without a valid token = %v, want Unauthenticated", err)
			}
			if tc.wantJobs != codes.Unauthenticated && status.Code(err) == codes.Unauthenticated {
				t.Fatalf("WorkerService.Claim with the token = %v", err)
			}

			// Health stays open for probes.
			if _, err := healthv1.NewHealthClient(conn).Check(ctx, &healthv1.HealthCheckRequest{}); err != nil {
				t.Fatalf("health check without auth: %v", err)
			}
		})
	}
}
