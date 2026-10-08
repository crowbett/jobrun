// Command jobrund is the jobrun daemon: gRPC control plane, in-process worker
// pool, leader-elected scheduler, lease reaper and the /admin page, all in one
// process.
//
// Startup order: config -> logger -> OTel -> Postgres (pool, migrate, store,
// notifier, locker) -> registry + handlers -> broker/histogram/depth cache ->
// pool/scheduler/reaper -> gRPC -> admin http -> background goroutines.
// Teardown is staged in the reverse direction with independent budgets; see
// teardown and the "Concurrency and teardown" table in docs/PLAN.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
	_ "time/tzdata" // IANA zones for schedule timezones on hosts without a zoneinfo db (Windows, distroless)

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	jobrunv1 "github.com/crowbett/jobrun/gen/jobrun/v1"
	"github.com/crowbett/jobrun/internal/admin"
	"github.com/crowbett/jobrun/internal/config"
	"github.com/crowbett/jobrun/internal/handlers"
	"github.com/crowbett/jobrun/internal/interceptor"
	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/internal/obs"
	"github.com/crowbett/jobrun/internal/postgres"
	"github.com/crowbett/jobrun/internal/reaper"
	"github.com/crowbett/jobrun/internal/scheduler"
	"github.com/crowbett/jobrun/internal/server"
	"github.com/crowbett/jobrun/internal/worker"
)

// Fixed budgets for the teardown stages that are not user-configurable.
const (
	startupTimeout   = 30 * time.Second
	poolDrainTimeout = 30 * time.Second // stage 2: in-flight handlers; the rest keep their lease and get reaped
	bgStopTimeout    = 10 * time.Second // stage 3: scheduler releases the advisory lock, reaper finishes its tick
	infraStopTimeout = 5 * time.Second  // stage 4: notifier connection, depth cache
	otelFlushTimeout = 5 * time.Second  // stage 6
	clockSkewWarn    = 2 * time.Second  // docs/PLAN.md, Risks: clock skew on run_at

	maxRecvMsgSize       = 1 << 20 // gRPC request cap; payloads are capped at job.MaxPayloadBytes
	maxConcurrentStreams = 256     // per connection; unary RPCs finish in milliseconds
)

// version is injected at build time: -ldflags="-X main.version=v1.2.3".
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger := newLogger(cfg)
	slog.SetDefault(logger)
	startedAt := time.Now()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- Observability (stage 6 is the deferred flush) -------------------------
	otelStop, err := obs.Setup(ctx, obs.Config{
		Enabled:        cfg.OTel.Enabled,
		ServiceName:    cfg.ServiceName,
		ServiceVersion: version,
		Environment:    cfg.Env,
		Endpoint:       cfg.OTel.Endpoint,
		Logger:         logger,
	})
	if err != nil {
		return fmt.Errorf("otel setup: %w", err)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), otelFlushTimeout)
		defer cancel()
		if err := otelStop(flushCtx); err != nil {
			logger.Warn("teardown 6/6: otel flush", slog.Any("err", err))
			return
		}
		logger.Info("teardown 6/6: otel flushed")
	}()

	// --- Postgres (stage 5 is the deferred pool close) -------------------------
	startCtx, cancelStart := context.WithTimeout(ctx, startupTimeout)
	defer cancelStart()

	pool, err := postgres.Open(startCtx, cfg.DatabaseURL, int32(cfg.PGMaxConns), logger) //nolint:gosec // validated small int
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer func() {
		pool.Close()
		logger.Info("teardown 5/6: postgres pool closed")
	}()
	if err := postgres.Migrate(startCtx, pool); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	mode, err := postgres.ParseEventMode(cfg.Worker.Events)
	if err != nil {
		return fmt.Errorf("JOB_EVENTS: %w", err)
	}
	store := postgres.NewStore(pool, logger, postgres.WithEventMode(mode))
	warnClockSkew(startCtx, store, logger)
	notifier := postgres.NewNotifier(pool, logger, postgres.WithPollFloor(cfg.Worker.PollFloor))
	locker := postgres.NewLocker(pool)

	// --- Handlers, admin plumbing, runtime ---------------------------------------
	reg := job.NewRegistry()
	if err := handlers.RegisterWith(reg, logger); err != nil {
		return err
	}
	broker := admin.NewBroker(logger)
	hist := admin.NewHistogram()
	depth := admin.NewDepthCache(store.Depth, admin.DefaultDepthInterval, logger)

	wp := worker.NewPool(store, notifier, reg, broker, logger, worker.Config{
		Queues:         cfg.Worker.Queues,
		Concurrency:    cfg.Worker.Concurrency,
		BatchSize:      cfg.Worker.BatchSize,
		LeaseTTL:       cfg.Worker.LeaseTTL,
		HeartbeatEvery: cfg.Worker.HeartbeatEvery,
		JobTimeout:     cfg.Worker.JobTimeout,
		PollFloor:      cfg.Worker.PollFloor,
		Owner:          worker.DefaultOwner(),
		Durations:      hist,
	})
	var sched *scheduler.Scheduler
	if cfg.Scheduler.Enabled {
		sched = scheduler.NewScheduler(store, locker, logger, scheduler.Config{Tick: cfg.Scheduler.Tick, LockKey: cfg.Scheduler.LockKey})
	}
	rp := reaper.NewReaper(store, broker, logger, reaper.Config{
		LeaseTTL:  cfg.Worker.LeaseTTL,
		Retention: cfg.Reaper.Retention,
		Batch:     int32(cfg.Reaper.Batch), //nolint:gosec // validated >= 1
		Policy:    job.DefaultPolicy,
	})

	// Out-of-process workers lease through the same store, fenced by the same
	// tokens, woken by the same notifier; their transitions reach the broker.
	remote := server.NewWorkerServer(store, logger, server.WorkerConfig{
		LeaseTTL:  cfg.Worker.LeaseTTL,
		PollFloor: cfg.Worker.PollFloor,
		BatchSize: cfg.Worker.BatchSize,
	}, server.WithDispatcher(notifier), server.WithPublisher(broker))

	snap := func() obs.Snapshot {
		ws := wp.Stats()
		rs := remote.Stats()
		s := obs.Snapshot{
			Claimed:     uint64(ws.Claimed),                  //nolint:gosec // counters are non-negative
			Succeeded:   uint64(ws.Succeeded),                //nolint:gosec // counters are non-negative
			Failed:      uint64(ws.Failed),                   //nolint:gosec // counters are non-negative
			Retried:     uint64(ws.Retried),                  //nolint:gosec // counters are non-negative
			Dead:        uint64(ws.Dead),                     //nolint:gosec // counters are non-negative
			LeaseLost:   uint64(ws.LeaseLost + rs.LeaseLost), //nolint:gosec // counters are non-negative
			LeaseFenced: uint64(ws.Fenced + rs.Fenced),       //nolint:gosec // counters are non-negative
		}
		if sched != nil {
			s.SchedulerTicks = uint64(sched.Ticks()) //nolint:gosec // counter is non-negative
		}
		return s
	}
	if err := obs.RegisterSnapshot(snap); err != nil {
		return err
	}
	if err := obs.RegisterQueueDepth(func() []job.DepthRow { rows, _ := depth.Depth(); return rows }); err != nil {
		return err
	}
	counters := func() admin.Counters {
		c := broker.Counters()
		c.LeaseExpired = uint64(rp.Stats().Reaped) //nolint:gosec // counter is non-negative
		s := snap()
		c.SchedulerTicks = s.SchedulerTicks
		c.LeaseFenced = s.LeaseFenced
		return c
	}

	// --- gRPC --------------------------------------------------------------------
	if cfg.GRPCOpen() {
		logger.Warn("gRPC is reachable beyond loopback without GRPC_TOKEN: anyone who can connect can enqueue, lease and read jobs",
			slog.String("grpc_addr", cfg.GRPCAddr), slog.String("env", cfg.Env))
	}
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.GRPCAddr, err)
	}
	grpcSrv := grpc.NewServer(
		// job.MaxPayloadBytes (256 KiB) is the largest field a request carries;
		// 1 MiB leaves room for the envelope and stops a client from making the
		// server buffer the default 4 MiB per message.
		grpc.MaxRecvMsgSize(maxRecvMsgSize),
		grpc.MaxConcurrentStreams(maxConcurrentStreams),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			interceptor.UnaryRecover(logger), // outermost: catches panics from everything below
			interceptor.UnaryRequestID(),
			interceptor.UnaryLogging(logger), // logs rejected calls too
			interceptor.UnaryAuth(cfg.GRPCToken),
		),
		grpc.ChainStreamInterceptor(
			interceptor.StreamRecover(logger),
			interceptor.StreamRequestID(),
			interceptor.StreamLogging(logger),
			interceptor.StreamAuth(cfg.GRPCToken),
		),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
			Time:              2 * time.Minute,
			Timeout:           20 * time.Second,
		}),
	)
	server.NewJobServer(store, logger, server.WithDepthCache(depth)).Register(grpcSrv)
	server.NewScheduleServer(store, logger).Register(grpcSrv)
	remote.Register(grpcSrv)
	healthSrv := health.NewServer()
	healthv1.RegisterHealthServer(grpcSrv, healthSrv)
	healthSrv.SetServingStatus(jobrunv1.JobService_ServiceDesc.ServiceName, healthv1.HealthCheckResponse_SERVING)
	healthSrv.SetServingStatus(jobrunv1.ScheduleService_ServiceDesc.ServiceName, healthv1.HealthCheckResponse_SERVING)
	healthSrv.SetServingStatus(jobrunv1.WorkerService_ServiceDesc.ServiceName, healthv1.HealthCheckResponse_SERVING)
	reflection.Register(grpcSrv)

	// --- Admin http ---------------------------------------------------------------
	adminSrv := admin.NewServer(cfg.AdminAddr, admin.Deps{
		Broker:         broker,
		Depth:          depth,
		Counters:       counters,
		Durations:      hist,
		Build:          map[string]string{"version": version, "go_version": runtime.Version(), "env": cfg.Env},
		Version:        version,
		StartedAt:      startedAt,
		Pool:           func() any { return poolStats{Stats: wp.Stats(), Remote: remote.Stats()} },
		AllowedOrigins: cfg.AdminCORSOrigins,
		Log:            logger,
	})

	// --- Background goroutines ------------------------------------------------------
	// Each group gets its own context derived WITHOUT the signal's cancellation,
	// so a Ctrl-Break stops things in stage order rather than all at once.
	base := context.WithoutCancel(ctx)
	poolCtx, stopPool := context.WithCancel(base)
	defer stopPool()
	bgCtx, stopBG := context.WithCancel(base)
	defer stopBG()
	infraCtx, stopInfra := context.WithCancel(base)
	defer stopInfra()

	bg := background{
		pool: wp, sched: sched, reap: rp, remote: remote,
		stopPool: stopPool, stopBG: stopBG, stopInfra: stopInfra,
	}
	bg.notifierDone = spawn(infraCtx, logger, "notifier", notifier.Run)
	bg.depthDone = spawn(infraCtx, logger, "depth cache", depth.Run)
	bg.poolDone = spawn(poolCtx, logger, "worker pool", wp.Run)
	if sched != nil {
		bg.schedDone = spawn(bgCtx, logger, "scheduler", sched.Run)
	}
	bg.reaperDone = spawn(bgCtx, logger, "reaper", rp.Run)

	serveErr := make(chan error, 2)
	go func() {
		logger.Info("grpc server listening", "addr", lis.Addr().String(), "version", version,
			"queues", cfg.Worker.Queues, "concurrency", cfg.Worker.Concurrency, "batch", cfg.Worker.BatchSize,
			"poll_floor", cfg.Worker.PollFloor.String(), "owner", wp.Owner(), "scheduler", cfg.Scheduler.Enabled)
		if err := grpcSrv.Serve(lis); err != nil {
			serveErr <- fmt.Errorf("grpc serve: %w", err)
		}
	}()
	go func() {
		logger.Info("admin server listening", "addr", "http://"+cfg.AdminAddr+"/admin")
		if err := adminSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- fmt.Errorf("admin serve: %w", err)
		}
	}()

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("stop signal received; starting staged teardown")
	case runErr = <-serveErr:
		logger.Error("server failed; starting staged teardown", slog.Any("err", runErr))
	}
	teardown(cfg, logger, grpcSrv, healthSrv, adminSrv, bg)
	return runErr
}

// poolStats is the /admin/stats "pool" object: the in-process pool's counters
// plus the WorkerService's (remote workers) under "Remote".
type poolStats struct {
	worker.Stats
	Remote server.WorkerStats
}

// background holds the long-running goroutines and their stop handles, grouped
// by teardown stage.
type background struct {
	pool   *worker.Pool
	sched  *scheduler.Scheduler
	reap   *reaper.Reaper
	remote *server.WorkerServer

	stopPool, stopBG, stopInfra context.CancelFunc

	poolDone, schedDone, reaperDone, notifierDone, depthDone <-chan struct{}
}

// teardown runs stages 1-4 in order with independent budgets; stages 5-6
// (pool close, OTel flush) are the deferred calls in run.
//
//  1. health NOT_SERVING, gRPC GracefulStop and admin drain (SHUTDOWN_TIMEOUT).
//  2. cancel the pool: it stops claiming and drains in-flight handlers (30 s;
//     whatever does not finish keeps its lease and is reaped later).
//  3. cancel the scheduler (releases the advisory lock) and the reaper.
//  4. cancel the notifier connection and the depth cache.
func teardown(cfg config.Config, logger *slog.Logger, grpcSrv *grpc.Server, healthSrv *health.Server,
	adminSrv *http.Server, bg background,
) {
	// Stage 1. Claim streams are long-lived: end them first or GracefulStop
	// waits the whole budget for them.
	healthSrv.Shutdown()
	bg.remote.Drain()
	grpcDone := make(chan struct{})
	go func() { grpcSrv.GracefulStop(); close(grpcDone) }()
	drainCtx, cancel := context.WithTimeout(context.Background(), cfg.StopTimeout)
	defer cancel()
	adminErr := adminSrv.Shutdown(drainCtx)
	if !waitDone(grpcDone, cfg.StopTimeout) {
		logger.Warn("teardown 1/6: grpc drain timed out; forcing", "timeout", cfg.StopTimeout)
		grpcSrv.Stop()
	}
	if adminErr != nil {
		logger.Warn("teardown 1/6: admin drain incomplete", slog.Any("err", adminErr))
	}
	logger.Info("teardown 1/6: grpc + admin drained", "remote", fmt.Sprintf("%+v", bg.remote.Stats()))

	// Stage 2: Pool.Run returns once in-flight handlers have finished (or its
	// own drain budget ran out); we wait poolDrainTimeout at most.
	bg.stopPool()
	if !waitDone(bg.poolDone, poolDrainTimeout) {
		logger.Warn("teardown 2/6: worker pool did not drain in time; leases left to the reaper", "timeout", poolDrainTimeout)
	}
	logger.Info("teardown 2/6: worker pool stopped", "stats", fmt.Sprintf("%+v", bg.pool.Stats()))

	// Stage 3.
	bg.stopBG()
	ok := waitDone(bg.reaperDone, bgStopTimeout)
	if bg.schedDone != nil {
		ok = waitDone(bg.schedDone, bgStopTimeout) && ok
	}
	if !ok {
		logger.Warn("teardown 3/6: scheduler/reaper did not stop in time", "timeout", bgStopTimeout)
	}
	attrs := []any{"reaper", fmt.Sprintf("%+v", bg.reap.Stats())}
	if bg.sched != nil {
		attrs = append(attrs, "scheduler_ticks", bg.sched.Ticks(), "was_leader", bg.sched.Leader())
	}
	logger.Info("teardown 3/6: scheduler + reaper stopped", attrs...)

	// Stage 4.
	bg.stopInfra()
	if !waitDone(bg.notifierDone, infraStopTimeout) || !waitDone(bg.depthDone, infraStopTimeout) {
		logger.Warn("teardown 4/6: notifier/depth cache did not stop in time", "timeout", infraStopTimeout)
	}
	logger.Info("teardown 4/6: notifier + depth cache stopped")
}

// spawn runs fn in a goroutine and returns a channel closed when it returns.
// A non-nil, non-cancellation error is logged: every Run here is expected to
// end only when its context does.
func spawn(ctx context.Context, logger *slog.Logger, name string, fn func(context.Context) error) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := fn(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Warn(name+" stopped with error", slog.Any("err", err))
		}
	}()
	return done
}

// waitDone waits for ch to close within budget and reports whether it did.
// A nil channel (goroutine never started) counts as done.
func waitDone(ch <-chan struct{}, budget time.Duration) bool {
	if ch == nil {
		return true
	}
	select {
	case <-ch:
		return true
	case <-time.After(budget):
		return false
	}
}

// warnClockSkew logs when the app clock and the database clock disagree by
// more than clockSkewWarn: every scheduling comparison runs on the database
// clock, so a drifting app clock makes "delay 5s" look wrong to operators.
func warnClockSkew(ctx context.Context, store *postgres.Store, logger *slog.Logger) {
	dbNow, err := store.Now(ctx)
	if err != nil {
		logger.Warn("db clock check failed", slog.Any("err", err))
		return
	}
	skew := time.Until(dbNow)
	if skew < 0 {
		skew = -skew
	}
	if skew > clockSkewWarn {
		logger.Warn("database clock differs from the app clock; run_at/delay are on the database clock",
			"skew", skew.Round(time.Millisecond).String(), "db_now", dbNow.Format(time.RFC3339Nano))
		return
	}
	logger.Info("database clock ok", "skew", skew.Round(time.Millisecond).String())
}

func newLogger(cfg config.Config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	var h slog.Handler
	if cfg.LogFormat == "text" {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}
	return slog.New(obs.NewLogHandler(h)).With("service", cfg.ServiceName)
}
