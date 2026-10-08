// Command jobrun is the CLI for a running jobrund.
//
//	jobrun [-addr host:port] [-timeout 5s] <command> [flags]
//
//	health
//	enqueue  --type sleep [--queue default] [--payload '{"ms":100}'] [--key k] [--priority 0]
//	         [--delay 5s | --run-at 2026-09-19T12:00:00Z] [--max-attempts 5]
//	get      --id <uuid> [--events]
//	list     [--queue q] [--status queued] [--type t] [--size 50] [--token t]
//	cancel   --id <uuid>
//	retry    --id <uuid>
//	purge    [--queue q] [--status dead,cancelled] [--older-than 24h] [--limit n]
//	stats    [--queue q]
//	schedule create --name n --cron "*/1 * * * *" [--tz UTC] --queue q --type t [--payload '{}']
//	schedule list|get|update|delete
//	bench    [--rate 200] [--duration 60s] [--queue bench] [--type noop] [--direct]
//
// Single objects print as protojson; lists print as aligned tables.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"

	jobrunv1 "github.com/crowbett/jobrun/gen/jobrun/v1"
	"github.com/crowbett/jobrun/internal/interceptor"
)

const usage = `usage: jobrun [-addr host:port] [-timeout 5s] <command> [flags]

commands:
  health                     check the JobService health
  enqueue                    insert one job
  get                        one job by id (--events for its history)
  list                       page jobs newest first
  cancel | retry             steer one job
  purge                      delete finished jobs by queue/status/age
  stats                      cached queue depth by status
  schedule create|list|get|update|delete
  bench                      open-loop load test against the plan's criterion

run "jobrun <command> -h" for the flags of one command.`

// version is injected at build time: -ldflags="-X main.version=v1.2.3".
var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(2)
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// client bundles the connection and the per-call timeout every command uses.
type client struct {
	conn    *grpc.ClientConn
	jobs    jobrunv1.JobServiceClient
	sched   jobrunv1.ScheduleServiceClient
	timeout time.Duration
}

func (c *client) call() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), c.timeout)
}

func run(args []string) error {
	global := flag.NewFlagSet("jobrun", flag.ContinueOnError)
	global.Usage = func() { fmt.Fprintln(global.Output(), usage) }
	addr := global.String("addr", envOr("GRPC_ADDR", "localhost:50052"), "jobrund gRPC address")
	timeout := global.Duration("timeout", 5*time.Second, "per-call timeout")
	showVersion := global.Bool("version", false, "print the version and exit")
	if err := global.Parse(args); err != nil {
		return err
	}
	if *showVersion {
		fmt.Println("jobrun", version)
		return nil
	}
	if global.NArg() == 0 {
		global.Usage()
		return errors.New("a command is required")
	}
	sub, rest := global.Arg(0), global.Args()[1:]

	// The token comes from the environment only: a flag would show it in the process list.
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if tok := os.Getenv("GRPC_TOKEN"); tok != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(interceptor.Bearer(tok)))
	}
	conn, err := grpc.NewClient(*addr, opts...)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	c := &client{conn: conn, jobs: jobrunv1.NewJobServiceClient(conn), sched: jobrunv1.NewScheduleServiceClient(conn), timeout: *timeout}

	switch sub {
	case "health":
		return c.health()
	case "enqueue":
		return c.enqueue(rest)
	case "get":
		return c.get(rest)
	case "list":
		return c.list(rest)
	case "cancel":
		return c.cancel(rest)
	case "retry":
		return c.retry(rest)
	case "purge":
		return c.purge(rest)
	case "stats":
		return c.stats(rest)
	case "schedule":
		return c.schedule(rest)
	case "bench":
		return c.bench(rest)
	case "help", "-h", "--help":
		global.Usage()
		return nil
	}
	return fmt.Errorf("unknown command %q (try: jobrun help)", sub)
}

func (c *client) health() error {
	ctx, cancel := c.call()
	defer cancel()
	for _, svc := range []string{jobrunv1.JobService_ServiceDesc.ServiceName, jobrunv1.ScheduleService_ServiceDesc.ServiceName} {
		hc, err := healthv1.NewHealthClient(c.conn).Check(ctx, &healthv1.HealthCheckRequest{Service: svc})
		if err != nil {
			return fmt.Errorf("health check %s: %w", svc, err)
		}
		fmt.Printf("%-26s %s\n", svc, hc.GetStatus())
	}
	return nil
}

// parseStatus reads the database spelling ("dead") or the enum name.
func parseStatus(s string) (jobrunv1.JobStatus, error) {
	if s == "" {
		return jobrunv1.JobStatus_JOB_STATUS_UNSPECIFIED, nil
	}
	name := strings.ToUpper(strings.TrimSpace(s))
	if !strings.HasPrefix(name, "JOB_STATUS_") {
		name = "JOB_STATUS_" + name
	}
	v, ok := jobrunv1.JobStatus_value[name]
	if !ok || v == 0 {
		return 0, fmt.Errorf("unknown status %q (queued|leased|succeeded|failed|dead|cancelled)", s)
	}
	return jobrunv1.JobStatus(v), nil
}

// statusName strips the JOB_STATUS_ prefix for tables.
func statusName(s jobrunv1.JobStatus) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), "JOB_STATUS_"))
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
