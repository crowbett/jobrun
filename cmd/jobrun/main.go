// Command jobrun is the CLI for a running jobrund: enqueue|get|list|retry|cancel|schedule|bench.
//
// This is the minimal bring-up main (health check only); the integrator adds the
// subcommands on top of it.
//
//	go run ./cmd/jobrun -addr localhost:50052
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"

	jobrunv1 "github.com/crowbett/jobrun/gen/jobrun/v1"
)

func main() {
	addr := flag.String("addr", envOr("GRPC_ADDR", "localhost:50052"), "jobrund address")
	timeout := flag.Duration("timeout", 5*time.Second, "per-call timeout")
	flag.Parse()

	if err := run(*addr, *timeout); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(addr string, timeout time.Duration) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	hc, err := healthv1.NewHealthClient(conn).Check(ctx, &healthv1.HealthCheckRequest{
		Service: jobrunv1.JobService_ServiceDesc.ServiceName,
	})
	if err != nil {
		return fmt.Errorf("health check: %w", err)
	}
	fmt.Printf("health: %s\n", hc.GetStatus())
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
