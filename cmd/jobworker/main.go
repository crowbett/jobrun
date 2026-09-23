// Command jobworker runs jobrun handlers out of process: it leases jobs from a
// jobrund over jobrun.v1.WorkerService (pkg/jobworker) and runs the built-in
// demo handlers (noop, sleep, fail, echo).
//
// Remote workers get at-least-once effects; see pkg/jobworker. Stop with
// Ctrl-C / Ctrl-Break / SIGTERM: it stops claiming, drains in-flight handlers
// (-drain) and exits 0. A kill mid-job leaves the lease to expire; the reaper
// requeues the job for another worker.
//
//	jobworker -addr localhost:50052 -queues default,bench -concurrency 16
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/crowbett/jobrun/internal/handlers"
	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/internal/obs"
	"github.com/crowbett/jobrun/pkg/jobworker"
)

var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "jobworker:", err)
		os.Exit(1)
	}
}

type options struct {
	addr        string
	queues      string
	id          string
	concurrency int
	jobTimeout  time.Duration
	drain       time.Duration
	logFormat   string
	logLevel    string
}

func parse(args []string) (options, error) {
	var o options
	fs := flag.NewFlagSet("jobworker", flag.ContinueOnError)
	fs.StringVar(&o.addr, "addr", envOr("JOBRUN_ADDR", "localhost:50052"), "jobrund gRPC address (JOBRUN_ADDR)")
	fs.StringVar(&o.queues, "queues", envOr("WORKER_QUEUES", "default"), "comma-separated queues to claim from (WORKER_QUEUES)")
	fs.StringVar(&o.id, "id", os.Getenv("WORKER_ID"), "worker id in lease_owner (WORKER_ID); default host-pid-rand")
	fs.IntVar(&o.concurrency, "concurrency", jobworker.DefaultConcurrency, "max handlers running at once")
	fs.DurationVar(&o.jobTimeout, "job-timeout", jobworker.DefaultJobTimeout, "per-job handler timeout")
	fs.DurationVar(&o.drain, "drain", jobworker.DefaultDrainTimeout, "how long to wait for in-flight handlers on stop")
	fs.StringVar(&o.logFormat, "log-format", envOr("LOG_FORMAT", "json"), "json | text (LOG_FORMAT)")
	fs.StringVar(&o.logLevel, "log-level", envOr("LOG_LEVEL", "info"), "debug | info | warn | error (LOG_LEVEL)")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if o.concurrency < 1 || o.concurrency > 1024 {
		return o, errors.New("-concurrency must be 1..1024")
	}
	return o, nil
}

func run(args []string) error {
	o, err := parse(args)
	if err != nil {
		return err
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(o.logLevel)); err != nil {
		return fmt.Errorf("-log-level: %w", err)
	}
	hopts := &slog.HandlerOptions{Level: level}
	var h slog.Handler = slog.NewJSONHandler(os.Stdout, hopts)
	if o.logFormat == "text" {
		h = slog.NewTextHandler(os.Stdout, hopts)
	}
	logger := slog.New(obs.NewLogHandler(h)).With("service", "jobworker")
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	otelStop, err := obs.Setup(ctx, obs.Config{
		Enabled:        os.Getenv("OTEL_ENABLED") == "true",
		ServiceName:    "jobworker",
		ServiceVersion: version,
		Environment:    envOr("APP_ENV", "dev"),
		Endpoint:       envOr("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"),
		Logger:         logger,
	})
	if err != nil {
		return fmt.Errorf("otel: %w", err)
	}
	defer func() {
		fctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = otelStop(fctx)
	}()

	conn, err := grpc.NewClient(o.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return fmt.Errorf("dial %s: %w", o.addr, err)
	}
	defer func() { _ = conn.Close() }()

	w := jobworker.New(conn, jobworker.Config{
		WorkerID:     o.id,
		Queues:       splitList(o.queues),
		Concurrency:  o.concurrency,
		JobTimeout:   o.jobTimeout,
		DrainTimeout: o.drain,
		Logger:       logger,
	})
	if err := registerBuiltins(w, logger); err != nil {
		return err
	}
	logger.Info("jobworker starting", "addr", o.addr, "queues", o.queues, "concurrency", o.concurrency,
		"pid", os.Getpid(), "version", version)
	return w.Run(ctx)
}

// registerBuiltins exposes internal/handlers (the same noop/sleep/fail/echo
// jobrund runs in-process) through the remote Handler interface.
func registerBuiltins(w *jobworker.Worker, log *slog.Logger) error {
	reg := job.NewRegistry()
	if err := handlers.RegisterWith(reg, log); err != nil {
		return err
	}
	for _, typ := range reg.Types() {
		h, _ := reg.Lookup(typ)
		inner, ok := h.(job.Handler)
		if !ok {
			continue // TxHandlers are in-process only: their effect needs the completion tx
		}
		if err := w.Handle(typ, adapt(inner)); err != nil {
			return err
		}
	}
	return nil
}

// adapt runs an in-process job.Handler on a remote job; job.ErrPermanent maps
// to jobworker.ErrPermanent.
func adapt(h job.Handler) jobworker.Handler {
	return jobworker.HandlerFunc(func(ctx context.Context, rj jobworker.Job) error {
		id, err := uuid.Parse(rj.ID)
		if err != nil {
			return fmt.Errorf("%w: job id %q: %w", jobworker.ErrPermanent, rj.ID, err)
		}
		err = h.Handle(ctx, job.Job{
			ID: id, Queue: rj.Queue, Type: rj.Type, Payload: rj.Payload,
			Attempts: rj.Attempt, MaxAttempts: rj.MaxAttempts, Status: job.StatusLeased,
			LeaseToken: rj.LeaseToken, TraceParent: rj.TraceParent,
		})
		if errors.Is(err, job.ErrPermanent) {
			return fmt.Errorf("%w: %w", jobworker.ErrPermanent, err)
		}
		return err
	})
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

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
