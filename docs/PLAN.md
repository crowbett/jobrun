# ADR-0001: P9 "jobrun" - durable job queue (Postgres-backed, gRPC + CLI + /admin SSE)
Status: Proposed | Date: 2026-09-19 | Module: `github.com/crowbett/jobrun` | Go 1.27

## Context
Scaffold is `~/dev/_templates/go-grpc` (buf v2 `proto/` -> committed `gen/`, `internal/interceptor` Recover -> RequestID -> Logging, bufconn table tests, health + reflection, Taskfile, strict `.golangci.yml`, CI that diffs `buf generate`). Step 1 deletes the `echo` surface, as in P5.
Reuse from `~/dev/go/orders` (P5): `internal/postgres` (pgxpool + `withTx` + embedded goose + the sqlc/pgx-v5 `sqlc.yaml`), `internal/obs` (OTel providers + observable counters fed from a `Snapshot`), `internal/config` (env-only `Load()`/`Validate()`, no globals), the `cmd/server` staged teardown, the `processed_events` dedupe pattern. From `~/dev/go/sysmon`: `internal/stream` (SSE handler: `Subscribe(buf) (<-chan T, func())`, `http.NewResponseController` flush + lifted write deadline + keepalive), `internal/metrics` (hand-written Prometheus text exposer, zero deps), `web/demo.html` (the synthwave page).
Requirement (ROADMAP P9): enqueue/schedule (cron + one-shot), leases with heartbeat, retries with backoff, DLQ, worker pool, `/admin` SSE dashboard, gRPC + CLI, exactly-once effects, **10k jobs/min sustained (~170/s)**.
Ports must not collide: orders holds 5433/4223, sysmon 9100, aigw 8090. jobrun takes **pg 5434**, **gRPC :50052**, **admin http 127.0.0.1:9110**; 4225 is reserved for the NATS follow-up but there is no NATS in v1 (Decision 2).
New deps beyond the scaffold: `jackc/pgx/v5`, `pressly/goose/v3`, `google/uuid` (v7 = time-sortable ids), the same OTel set as orders. **No cron library, no Prometheus client, no NATS, no mocking framework** - each justified below.

## Decision
1. **Postgres is the only source of truth.** `jobs` holds state, `job_events` is append-only history, `schedules` holds cron/one-shot templates, `processed_effects` gives exactly-once side effects. Every timestamp used for scheduling comes from the database clock, never the app's.
2. **No NATS in v1.** JetStream would duplicate the queue: durable state, retry counter, lease and DLQ all have to live in Postgres anyway for `List`/`Retry`/`Cancel`/`/admin` to be truthful, so a stream would only be a wake-up bus - and an at-least-once one that can disagree with the table. `SELECT ... FOR UPDATE SKIP LOCKED` with a 200 ms poll floor plus `LISTEN/NOTIFY` wake-up gives sub-10 ms dispatch with far fewer moving parts, and 170/s is two orders of magnitude below what one Postgres does off a partial index. A `Dispatcher` port keeps the seam: a `natsx` dispatcher pushing claims to remote workers is the follow-up that pairs with out-of-process workers. `compose.yaml` is Postgres-only.
3. **Workers are in-process Go handlers** registered by job type in the same binary; the gRPC surface is a *control plane* (enqueue/inspect/schedule), not a worker protocol. That is what makes exactly-once effects possible at all: the handler's effect, the job completion and the `processed_effects` row commit in one transaction. A streaming `WorkerService` (Lease/Heartbeat/Complete/Fail) is sketched in proto comments but **not implemented in v1**; it degrades to effectively-once and needs the `Dispatcher` port from (2).
4. **Leases, not visibility timeouts.** A claim flips `queued -> leased`, stamps `lease_owner`/`lease_until` and **increments `attempts` at claim time**. A heartbeater extends `lease_until` for everything in flight; a heartbeat that updates fewer rows than it asked for means the lease was lost, and the worker cancels that job's context (fencing). A `Reaper` requeues expired leases with backoff. Deliberate deviation from the brief: attempts increments **at claim**, not in the reaper, so the counter has exactly one writer and a process that hard-crashes mid-job still burns exactly one attempt. Crash semantics: **at-least-once execution, at-most-`max_attempts` starts, exactly-once *effects* for handlers on the tx path.**
5. **Retries** are exponential with full jitter, capped; exhaustion sets `status='dead'` (a state, not a table). `Retry(id)` resets `attempts=0, status='queued', run_at=now()`; `Purge` deletes by queue/status/age.

## Package tree
```
cmd/jobrund/main.go          server wiring only: config -> obs -> pool+migrate -> registry -> worker/scheduler/reaper
                             -> grpc -> admin http -> staged teardown
cmd/jobrun/main.go           CLI: enqueue|get|list|retry|cancel|schedule|bench  (flag pkg, gRPC client)
proto/jobrun/v1/jobrun.proto JobService + ScheduleService + Job/Schedule/enums
gen/jobrun/v1/               GENERATED, committed, never hand-edited
internal/config/             Load()/Validate(); every knob in the tuning table
internal/job/                DOMAIN, no I/O: Job, Status, Policy, Cursor, Event, sentinels, Store/Dispatcher ports,
                             Handler + Registry. Imports nothing internal.
internal/cronx/              5-field cron parser + Next(after) in a named timezone. No deps.
internal/postgres/           ONLY SQL: pool.go, store.go (job.Store), effects.go, notify.go (LISTEN/NOTIFY),
                             lock.go (advisory lock), migrate.go, migrations/ (goose, embed), sqlc/ (GENERATED)
internal/worker/             Pool: claim loop -> bounded handler goroutines -> Complete/Fail; heartbeater
internal/scheduler/          leader-elected cron materialiser
internal/reaper/             expired-lease sweeper + retention pruner
internal/admin/              Broker (fan-out of transitions), SSE handler, /admin/stats, /metrics exposer, depth cache
internal/server/             gRPC adapters: jobs.go, schedules.go, convert.go, errors.go + bufconn tests
internal/obs/                OTel trace+metric providers, Snapshot counters (shared with /metrics)
internal/handlers/           demo handlers: noop (bench), sleep, fail, http_ping, prune
db/query/*.sql               sqlc source                  compose.yaml   postgres:17 on host 5434, healthchecked
deploy/grafana/jobrun.json   dashboard                     web/admin.html embedded synthwave SSE page
```
Imports: `cmd` -> everything; `server`/`worker`/`scheduler`/`reaper`/`postgres`/`admin` -> `job`; `job` -> stdlib + uuid only. `cronx` is imported by `scheduler` and `server` (validation) and imports nothing.

## Interfaces and key types
```go
// ---- internal/job (domain) ----
type ID = uuid.UUID
type Status int // Queued, Leased, Succeeded, Failed, Dead, Cancelled
type Job struct { ID ID; Queue, Type string; Payload []byte; Status Status; Priority int16; RunAt time.Time
    Attempts, MaxAttempts int32; Backoff Policy; LeaseOwner string; LeaseUntil *time.Time; LastError string
    IdempotencyKey string; CreatedAt, UpdatedAt time.Time; StartedAt, FinishedAt *time.Time }
type Policy struct{ Base, Max time.Duration; Factor float64; Jitter bool } // default 1s, 5m, 2.0, true
func (p Policy) Next(attempt int32, rnd *rand.Rand) time.Duration // full jitter: rand(0, min(base*f^(n-1), max))
type NewJob struct{ Queue, Type string; Payload []byte; Priority int16; RunAt time.Time; Delay time.Duration
    MaxAttempts int32; Backoff Policy; IdempotencyKey string }
type ListQuery struct{ Queue string; Status *Status; Type string; Cursor Cursor; Limit int32 } // Limit <= 200
type Cursor struct{ CreatedAt time.Time; ID ID } // keyset; opaque base64 on the wire
type Event struct{ JobID ID; Queue, Type string; From, To Status; Attempt int32; At time.Time; Err string }
type Schedule struct{ ID ID; Name, Cron, Timezone string; Template NewJob; Enabled bool
    NextRun time.Time; LastRun *time.Time }
var ( ErrNotFound=errors.New("job not found"); ErrIdempotencyConflict=errors.New("idempotency key reused")
      ErrNotRetryable=errors.New("job is not in a retryable state"); ErrNotCancellable=errors.New("...")
      ErrLeaseLost=errors.New("lease lost"); ErrNoHandler=errors.New("no handler registered for type")
      ErrPermanent=errors.New("permanent failure") ) // errors.Is -> straight to dead, no retry
// Port consumed by worker/scheduler/reaper/server; internal/postgres is the only implementation.
type Store interface {
    Enqueue(ctx context.Context, j Job) (Job, error)            // ON CONFLICT (queue,idem) -> the existing row
    EnqueueBatch(ctx context.Context, js []Job) (int64, error)  // pgx CopyFrom; bench + fan-out jobs
    Claim(ctx context.Context, q ClaimQuery) ([]Job, error)     // batch claim + lease, attempts++
    Heartbeat(ctx context.Context, owner string, ids []ID, until time.Time) (kept []ID, err error)
    Complete(ctx context.Context, id ID, owner string, fn Commit) error // fn runs in the SAME tx
    Fail(ctx context.Context, id ID, owner, cause string, perm bool, retryAt time.Time) (Status, error)
    ReapExpired(ctx context.Context, now time.Time, limit int32, p Policy) ([]Event, error)
    Get(ctx context.Context, id ID) (Job, error)
    List(ctx context.Context, q ListQuery) ([]Job, Cursor, error)
    Cancel(ctx context.Context, id ID) (Job, error)  // queued -> cancelled; leased -> cancel_requested
    Retry(ctx context.Context, id ID) (Job, error)   // dead|cancelled|failed -> queued, attempts=0
    Purge(ctx context.Context, q PurgeQuery) (int64, error)
    Depth(ctx context.Context) ([]DepthRow, error)   // cached by admin, never computed per scrape
    Schedules // embedded: Create/Update/Delete/List + DueSchedules + AdvanceSchedule
}
type ClaimQuery struct{ Queue, Owner string; Limit int32; LeaseTTL time.Duration }
type PurgeQuery struct{ Queue string; Statuses []Status; OlderThan time.Duration; Limit int32 }
type DepthRow struct{ Queue string; Status Status; N int64; OldestRunAt time.Time }
type Execer interface{ Exec(ctx context.Context, sql string, args ...any) (int64, error) }
type Commit func(ctx context.Context, ex Execer) error // nil for handlers with no durable effect
// ---- Dispatcher: the seam that keeps NATS addable without touching the worker ----
type Dispatcher interface {
    Ready(ctx context.Context, queue string) error          // producer: "work exists on this queue"
    Wait(ctx context.Context, queue string) <-chan struct{} // consumer; coalesced, always also ticks
} // v1 impls: postgres.Notifier (LISTEN/NOTIFY + poll floor) and job.PollOnly (ticker; tests).
// ---- Handlers + registry ----
type Handler interface{ Handle(ctx context.Context, j Job) error }
type TxHandler interface{ HandleTx(ctx context.Context, j Job) (Commit, error) } // opts into exactly-once
type HandlerFunc func(context.Context, Job) error
func NewRegistry() *Registry
func (r *Registry) Register(typ string, h Handler) error   // error on duplicate; called before Pool.Run
func (r *Registry) RegisterTx(typ string, h TxHandler) error
func (r *Registry) Lookup(typ string) (any, bool)
func (r *Registry) Types() []string
// ---- internal/worker ----
type Config struct{ Queues []string; Concurrency, BatchSize int; LeaseTTL, HeartbeatEvery, JobTimeout time.Duration
    PollFloor time.Duration; Owner string } // Owner = host+pid+rand, stable for the process lifetime
func NewPool(s job.Store, d job.Dispatcher, reg *job.Registry, pub EventPublisher, log *slog.Logger, c Config) *Pool
func (p *Pool) Run(ctx context.Context) error // errgroup: one claim loop per queue + one heartbeater; drains on ctx
func (p *Pool) Stats() Stats
type EventPublisher interface{ Publish(job.Event) } // admin.Broker; non-blocking, drops for slow subscribers
type heartbeater struct{ /* in-flight id -> cancel; Add/Remove/tick */ }
// ---- internal/cronx, internal/scheduler, internal/reaper ----
type Spec struct{ /* minute, hour, dom, month, dow bitsets; loc *time.Location */ }
func Parse(expr, tz string) (Spec, error)              // 5 fields, * , - / plus @hourly/@daily/@weekly/@monthly
func (s Spec) Next(after time.Time) (time.Time, error) // tz-aware; DST gap skips, DST repeat fires once
func NewScheduler(s job.Store, lk Locker, log *slog.Logger, cfg SchedConfig) *Scheduler
func (s *Scheduler) Run(ctx context.Context) error     // acquire -> tick(5s) -> materialise due -> advance next_run
type Locker interface {
    TryLock(ctx context.Context, key int64) (release func(context.Context) error, ok bool, err error) }
func NewReaper(s job.Store, pub EventPublisher, log *slog.Logger, cfg ReapConfig) *Reaper
func (r *Reaper) Run(ctx context.Context) error        // tick(LeaseTTL/3): ReapExpired, then retention Purge
// ---- internal/admin ----
func NewBroker(log *slog.Logger) *Broker
func (b *Broker) Publish(e job.Event)                          // never blocks; per-subscriber drop counter
func (b *Broker) Subscribe(buf int) (<-chan job.Event, func()) // sysmon/internal/stream contract
func NewServer(b *Broker, st Stats, d DepthCache, log *slog.Logger) http.Handler // /admin, /admin/stats,
                                                               // /admin/events (SSE), /metrics, /healthz
type DepthCache interface{ Depth() ([]job.DepthRow, time.Time) } // 5s ticker, not per request
```

## Proto sketch
```proto
// proto/jobrun/v1/jobrun.proto - package jobrun.v1; go_package .../gen/jobrun/v1;jobrunv1
service JobService {
  rpc Enqueue(EnqueueRequest) returns (EnqueueResponse);  // idempotency_key optional, unique per queue
  rpc Get(GetRequest) returns (GetResponse);
  rpc List(ListRequest) returns (ListResponse);           // keyset page_token
  rpc Cancel(CancelRequest) returns (CancelResponse);
  rpc Retry(RetryRequest) returns (RetryResponse);
  rpc Purge(PurgeRequest) returns (PurgeResponse);
  rpc Stats(StatsRequest) returns (StatsResponse);        // cached queue depth by status
}
service ScheduleService { rpc Create/Update/Delete/List(...); }  // cron + timezone + JobTemplate
// NOT in v1, reserved so the proto has room: service WorkerService {
//   rpc Lease(stream LeaseRequest) returns (stream LeaseResponse); rpc Heartbeat/Complete/Fail(...); }
enum JobStatus { JOB_STATUS_UNSPECIFIED=0; QUEUED=1; LEASED=2; SUCCEEDED=3; FAILED=4; DEAD=5; CANCELLED=6; }
message BackoffPolicy { google.protobuf.Duration base=1; google.protobuf.Duration max=2; double factor=3; bool jitter=4; }
message Job { string id=1; string queue=2; string type=3; bytes payload=4; JobStatus status=5; int32 priority=6;
  google.protobuf.Timestamp run_at=7; int32 attempts=8; int32 max_attempts=9; BackoffPolicy backoff=10;
  string lease_owner=11; google.protobuf.Timestamp lease_until=12; string last_error=13; string idempotency_key=14;
  google.protobuf.Timestamp created_at=15; updated_at=16; started_at=17; finished_at=18; }
message EnqueueRequest { string queue=1; string type=2; bytes payload=3; int32 priority=4;
  oneof when { google.protobuf.Timestamp run_at=5; google.protobuf.Duration delay=6; }
  int32 max_attempts=7; BackoffPolicy backoff=8; string idempotency_key=9; }
message Schedule { string id=1; string name=2; string cron=3; string timezone=4; JobTemplate template=5;
  bool enabled=6; google.protobuf.Timestamp next_run=7; google.protobuf.Timestamp last_run=8; }
```
Codes: InvalidArgument (validation, bad cron, bad cursor), NotFound, FailedPrecondition (`ErrNotCancellable`, `ErrNotRetryable`), AlreadyExists (`ErrIdempotencyConflict`), Internal otherwise, logged with the request id.

## Schema DDL (goose, `internal/postgres/migrations`)
```sql
-- 00001_jobs.sql
CREATE TYPE job_status AS ENUM ('queued','leased','succeeded','failed','dead','cancelled');
CREATE TABLE jobs (
  id uuid PRIMARY KEY, queue text NOT NULL, type text NOT NULL, payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  status job_status NOT NULL DEFAULT 'queued', priority smallint NOT NULL DEFAULT 0,
  run_at timestamptz NOT NULL DEFAULT now(), attempts int NOT NULL DEFAULT 0,
  max_attempts int NOT NULL DEFAULT 5 CHECK (max_attempts BETWEEN 1 AND 100),
  backoff jsonb NOT NULL DEFAULT '{"base":"1s","max":"5m","factor":2,"jitter":true}'::jsonb,
  lease_owner text, lease_until timestamptz, last_error text, idempotency_key text,
  cancel_requested bool NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz, finished_at timestamptz,
  CHECK ((status = 'leased') = (lease_owner IS NOT NULL)));
CREATE UNIQUE INDEX jobs_idem_uniq ON jobs (queue, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX jobs_ready  ON jobs (queue, priority DESC, run_at, id) WHERE status = 'queued'; -- the claim index
CREATE INDEX jobs_leases ON jobs (lease_until) WHERE status = 'leased';                      -- the reaper index
CREATE INDEX jobs_keyset ON jobs (queue, created_at DESC, id DESC);                          -- List
CREATE INDEX jobs_purge  ON jobs (finished_at) WHERE status IN ('succeeded','cancelled','dead');
-- 00002_job_events.sql  (append-only; written in the same tx as the transition)
CREATE TABLE job_events (id bigserial PRIMARY KEY, job_id uuid NOT NULL, queue text NOT NULL, type text NOT NULL,
  from_status job_status, to_status job_status NOT NULL, attempt int NOT NULL DEFAULT 0,
  error text, at timestamptz NOT NULL DEFAULT now());
CREATE INDEX job_events_job ON job_events (job_id, id); CREATE INDEX job_events_at ON job_events (at);
-- 00003_schedules.sql
CREATE TABLE schedules (id uuid PRIMARY KEY, name text NOT NULL UNIQUE, cron text NOT NULL,
  timezone text NOT NULL DEFAULT 'UTC', queue text NOT NULL, job_type text NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb, priority smallint NOT NULL DEFAULT 0,
  max_attempts int NOT NULL DEFAULT 5, backoff jsonb NOT NULL DEFAULT '{}'::jsonb,
  enabled bool NOT NULL DEFAULT true, next_run timestamptz NOT NULL, last_run timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX schedules_due ON schedules (next_run) WHERE enabled;
-- 00004_processed_effects.sql  (the P5 dedupe pattern, generalised)
CREATE TABLE processed_effects (scope text NOT NULL, key text NOT NULL, job_id uuid NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (scope, key));
```
sqlc queries (`db/query/`): **jobs.sql** - InsertJob, InsertJobsCopy, ClaimJobs, HeartbeatLeases, CompleteJob, FailJob, SelectExpiredLeases, RequeueExpired, GetJob, GetJobByIdem, ListJobs, CancelJob, RetryJob, PurgeJobs, QueueDepth; **events.sql** - InsertJobEvent, ListJobEvents, PruneJobEvents; **schedules.sql** - InsertSchedule, UpdateSchedule, DeleteSchedule, ListSchedules, DueSchedules, AdvanceSchedule; **effects.sql** - InsertProcessedEffect.

## The statements that matter
```sql
-- ClaimJobs :many - one round trip = batch claim + lease + attempts++. $1 queue, $2 db-now, $3 limit, $4 owner, $5 ttl.
WITH ready AS (
  SELECT id FROM jobs WHERE status = 'queued' AND queue = $1 AND run_at <= $2
   ORDER BY priority DESC, run_at, id LIMIT $3 FOR UPDATE SKIP LOCKED)
UPDATE jobs j SET status='leased', lease_owner=$4, lease_until=$2 + $5::interval,
       attempts=j.attempts+1, started_at=COALESCE(j.started_at,$2), updated_at=$2
  FROM ready WHERE j.id = ready.id RETURNING j.*;
-- HeartbeatLeases :many - returns the ids actually kept. len(kept) < len(ids) means those leases were reaped or
-- stolen and the worker cancels their contexts (fencing). $1 db-now, $2 ttl, $3 ids, $4 owner.
UPDATE jobs SET lease_until = $1 + $2::interval, updated_at = $1
 WHERE id = ANY($3::uuid[]) AND status='leased' AND lease_owner = $4 RETURNING id, cancel_requested;
-- CompleteJob :execrows - owner-guarded so a stale worker cannot resurrect a reaped job. The handler's Commit
-- closure and the job_events INSERT run in this same tx (opened by store.Complete). $1 id, $2 db-now, $3 owner.
UPDATE jobs SET status='succeeded', lease_owner=NULL, lease_until=NULL, last_error=NULL, finished_at=$2, updated_at=$2
 WHERE id=$1 AND status='leased' AND lease_owner=$3;
-- FailJob :one - $4 permanent, $5 retry_at computed in Go from job.Policy (the single source of backoff truth).
UPDATE jobs SET status = CASE WHEN $4::bool OR attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
       run_at = CASE WHEN $4::bool OR attempts >= max_attempts THEN run_at ELSE $5 END,
       lease_owner=NULL, lease_until=NULL, last_error=$6, updated_at=$3,
       finished_at = CASE WHEN $4::bool OR attempts >= max_attempts THEN $3 ELSE NULL END
 WHERE id=$1 AND status='leased' AND lease_owner=$7 RETURNING status, attempts;
-- SelectExpiredLeases (reaper tx step 1; step 2 writes run_at/status per id via unnest so Policy stays in Go):
SELECT id, queue, type, attempts, max_attempts FROM jobs
 WHERE status='leased' AND lease_until < $1 ORDER BY lease_until LIMIT $2 FOR UPDATE SKIP LOCKED;
```
A retry sits in `queued` with a future `run_at` (the partial claim index only reads `queued`); the `failed` enum value is reserved for "last attempt failed, not yet dead" and is **not** written by v1. Settle this in step 5 and put it in a comment on the enum - do not leave two spellings of "will retry".

## Data flow
1. `Enqueue` -> interceptors -> `server.JobServer` validates (queue/type 1..64 of `[a-z0-9._-]`, payload <= 256 KiB, priority -100..100, max_attempts 1..100, cron via `cronx.Parse`) -> `job.NewJob`.
2. `Store.Enqueue`, one tx: INSERT jobs `ON CONFLICT (queue, idempotency_key) DO NOTHING RETURNING`; empty result -> re-select (a replay returns the stored job); INSERT `job_events(nil -> queued)`; `SELECT pg_notify('jobrun_ready', queue)` in the same tx - Postgres delivers only on commit, so there is no notify-without-row window.
3. `postgres.Notifier` holds one dedicated pgx conn on `LISTEN jobrun_ready` and coalesces per queue into a 1-slot channel; `worker.Pool` selects on that channel or a `PollFloor` ticker (200 ms), whichever fires first.
4. Claim loop per queue: `Claim(limit=BatchSize)` -> per job `heartbeater.Add(id, cancel)`, run the handler in the bounded errgroup (`Concurrency`) under `context.WithTimeout(JobTimeout)`, publish `Event{queued->leased}`. Empty claim -> back to (3). A full batch -> claim again immediately, no sleep, until it comes back short.
5. Success: `Store.Complete(id, owner, commit)` - one tx: the handler's `Commit` (its durable effect plus `INSERT processed_effects` keyed `scope='job:'||type, key=idempotency_key|id`), the owner-guarded `jobs` UPDATE, the `job_events` row. Zero rows updated -> `ErrLeaseLost`, the effect rolls back, the job is redelivered later.
6. Failure: `Store.Fail(..., perm=errors.Is(err, job.ErrPermanent), retryAt=now+Policy.Next(attempts))` returns the resulting status; `dead` publishes `Event{-> dead}`, which is what the DLQ panel and the SSE feed show.
7. Scheduler (leader only): tick 5 s -> `DueSchedules(now)` with `FOR UPDATE SKIP LOCKED` -> `Enqueue` with `idempotency_key = "sched:"||schedule_id||":"||next_run.UTC().Format(RFC3339)`, so a double-elected leader or a retried tick cannot double-fire -> `AdvanceSchedule(cronx.Next(next_run))`, at most one materialisation per schedule per tick (no thundering herd after downtime).
8. Reaper: tick `LeaseTTL/3` -> `ReapExpired` (requeue with backoff, or dead at max_attempts) -> events; then purge succeeded/cancelled/dead older than `JOB_RETENTION` (default 72 h) and `PruneJobEvents` to match.
9. Every transition also goes to `admin.Broker` -> `/admin/events` SSE (`event: job`, JSON `job.Event`) and bumps the `obs` counters that both the OTel meter and the `/metrics` text exposer read. `/admin` serves the embedded synthwave page (`web/admin.html` via `embed.FS`) that subscribes to the SSE and draws depth + a transition log.

## Concurrency and teardown
| goroutine | started by | stopped by | on failure |
|---|---|---|---|
| gRPC `Serve` | `run()` | stage 1 `GracefulStop` (budget `SHUTDOWN_TIMEOUT`, else hard stop) | errCh -> run returns |
| admin `http.Server` | `run()` | stage 1 graceful stop; SSE conns die with the request ctx | errCh |
| claim loop x len(Queues) | `Pool.Run` errgroup | stage 2 pool ctx cancel | log, back off 1 s, retry |
| handler goroutines (<= `Concurrency`) | claim loop's inner errgroup | parent ctx; drain budget 30 s | `Fail` -> retry |
| heartbeater (1) | `Pool.Run` | stage 2 with the pool | kept-set shrink -> cancel that job's ctx |
| `Notifier.Run` (1 pgx conn) | `run()` | stage 3 | reconnect with backoff; the poll floor covers the gap |
| `Scheduler.Run` (1) | `run()` | stage 2, before the pool drains -> releases the advisory lock | lock lost -> re-elect |
| `Reaper.Run` (1) | `run()` | stage 2 | log, continue |
| depth cache ticker (1) | `admin.NewServer` | stage 1 with the http server | serve the last good snapshot |
| OTel batch exporters | `obs.Setup` | stage 5 `otelStop(5s)` | logged, non-fatal |

Staged teardown (orders' ladder; each stage gets its own `context.WithTimeout`, never the signal ctx): **1** gRPC + admin stop accepting -> **2** cancel scheduler/reaper/pool ctx and wait for in-flight handlers (30 s; whatever does not finish keeps its lease and is reaped later - that is the crash path, exercised on purpose) -> **3** stop the notifier -> **4** `pool.Close()` -> **5** `otelStop`. Ctrl-Break on Windows must log all five in order.

## Load target and benchmark
10k jobs/min = **167/s sustained**. `jobrun bench --rate 200 --duration 60s --queue bench --type noop --payload 256`: open-loop `time.Ticker` + N enqueuer goroutines over gRPC (`--direct` switches to `EnqueueBatch` on the pool to take the RPC out of the picture), a `noop` handler, and a sampler reading `job_events` for enqueue->succeeded latency. Reports achieved enqueue rate, completion rate, p50/p95/p99/max latency, final depth, plus `pg_stat_database` (xact_commit delta, cache hit ratio) and `pg_stat_statements` top-5 by total_exec_time around the run.
Knobs: `WORKER_BATCH_SIZE` (32) x `WORKER_CONCURRENCY` (16) x `POLL_FLOOR` (200 ms) x `PGX_MAX_CONNS` (24) x `LEASE_TTL` (30 s) x `JOB_EVENTS` (all|terminal|off). One claim per 200 ms at batch 32 is already 160/s before NOTIFY helps, so the pass criterion is 60 s at >= 167/s with p99 < 1 s and flat depth. The winning row goes in `README.md` (the ROADMAP's "one measured number").

## Observability
Spans `jobrun.enqueue`, `jobrun.claim` (attribute `jobs.claimed`), `jobrun.run` (linked to the enqueue span via a `traceparent` stored with the job at write time - the P5 lesson: inject at write time or the trace silently breaks), `jobrun.complete`. Metrics come from one `obs.Snapshot` of atomics exposed **both** as OTel observable instruments and as the hand-written `/metrics` text, so the two cannot disagree: `jobrun_jobs_total{queue,status}`, `jobrun_queue_depth{queue,status}` (from the 5 s depth cache), `jobrun_job_duration_seconds{queue,type}` (histogram; the text exposer writes explicit buckets), `jobrun_lease_expired_total`, `jobrun_scheduler_ticks_total`, `jobrun_claim_batch_size`, `jobrun_enqueue_total{result}`, `jobrun_dead_total{queue}`. `deploy/grafana/jobrun.json`: depth by queue, throughput, latency heatmap, dead + lease-expired rate, scheduler liveness. Push to the shared devstack collector on :4317 when `OTEL_ENABLED=true`, as in orders.

## Alternatives considered
- **JetStream as the queue** (work-queue stream, per-job ack): retries/DLQ for free, but the state of truth splits in two and `List`/`Cancel`/`Retry`/priority/`run_at` all become second-class; exactly-once still needs the DB.
- **JetStream as a wake-up bus only** (Postgres keeps truth): a second daemon in compose for what one `NOTIFY` does. Rejected for v1, kept reachable behind `job.Dispatcher`.
- **Out-of-process workers over a streaming `WorkerService`** in v1: doubles the surface (lease protocol, worker registry, auth) and gives up the single-tx exactly-once story. Follow-up; proto space reserved.
- **`robfig/cron/v3`** vs an in-house parser: see Risks - in-house wins, narrowly.
- **`SELECT FOR UPDATE` without `SKIP LOCKED`**, or an advisory lock per job: convoys and lock-table pressure at 170/s.
- **A separate `dead_letters` table**: a second write path and a second id space for the same row; `status='dead'` keeps the history and `job_events` + `Retry` stay trivially correct.
- **testcontainers-go**: not a dep in this house and slow on WSL2; compose + `-tags integration` + `TEST_DATABASE_URL` mirrors how orders is actually run.

## Implementation plan
Every step ends with `task fmt; go build ./...; go vet ./...` green plus the check named.
1. **Strip echo, stand up infra.** Delete `proto/echo`, `gen/echo`, `internal/server/echo*.go` and the echo wiring; add `compose.yaml` (postgres:17, host port 5434, healthcheck), `.env.example`, `sqlc.yaml`; rename `cmd/server` -> `cmd/jobrund`, `cmd/client` -> `cmd/jobrun`. -> `docker compose up -d --wait; docker compose ps`.
2. **Proto.** `proto/jobrun/v1/jobrun.proto` (JobService, ScheduleService, commented WorkerService). -> `buf lint; task gen`; commit `gen/`.
3. **Domain.** `internal/job/{job,status,policy,cursor,errors,ports,registry}.go` + tests: backoff ladder with a seeded `*rand.Rand`, the status transition table, cursor round-trip, registry duplicate/lookup. -> `go test -race ./internal/job/...`.
4. **cronx.** `internal/cronx/{parse,next}.go` + table tests (every field form, `@daily`, DST spring-forward and fall-back in `America/New_York`, leap day, dom/dow OR semantics) + `FuzzParse`. -> `go test -race ./internal/cronx/...`.
5. **Migrations + sqlc.** `internal/postgres/migrations/0000{1..4}_*.sql`, `db/query/*.sql`; settle the retry-state spelling above. -> `goose -dir ... up`; `sqlc generate`; `sqlc vet`.
6. **Store.** `internal/postgres/{pool,store,effects,notify,lock,migrate}.go` implementing `job.Store`, `job.Dispatcher`, `scheduler.Locker`. -> `go test -race -tags integration ./internal/postgres/...` with the stack up: claims are exclusive under 8 concurrent claimers, heartbeat fencing, complete-after-reap is a no-op, idempotent enqueue, keyset paging, depth.
7. **Worker pool.** `internal/worker/{pool,heartbeat,stats}.go` + tests against an in-memory `job.Store` fake: lease loss cancels the handler ctx, `ErrPermanent` skips retries, JobTimeout fires, drain on cancel. -> `go test -race ./internal/worker/...`.
8. **Scheduler + reaper.** `internal/scheduler/scheduler.go`, `internal/reaper/reaper.go` + tests: two schedulers and exactly one leader; one tick materialises exactly one job per due schedule; expired leases requeue with backoff and go dead at max_attempts. -> `go test -race ./internal/scheduler/... ./internal/reaper/...` plus an integration test for the advisory lock.
9. **gRPC adapter.** `internal/server/{jobs,schedules,convert,errors}.go` + a bufconn table test per RPC through the production interceptor chain. -> `go test -race ./internal/server/...`.
10. **Admin + metrics + page.** `internal/admin/{broker,sse,stats,metrics,depth}.go`, `web/admin.html` (embedded, synthwave, subscribes to `/admin/events`). Tests: the broker drops for a slow subscriber without blocking, SSE snapshot-then-stream, `/metrics` golden file. -> `go test -race ./internal/admin/...`; open `http://127.0.0.1:9110/admin` and watch transitions land live.
11. **Obs + wiring + handlers.** `internal/obs/*`, `internal/config/*` (+ a Validate table test), `internal/handlers/*`, `cmd/jobrund/main.go` (staged teardown), `cmd/jobrun/main.go` (all subcommands). -> `task run`; `jobrun enqueue --type sleep`, `get`, `list`, `cancel`, `retry`, `schedule create --cron "*/1 * * * *"`; Ctrl-Break logs five stages and exits 0; with the devstack obs profile up, enqueue->run is one trace in Tempo.
12. **Bench + CI + docs.** `cmd/jobrun/bench.go`, `deploy/grafana/jobrun.json`, the CI integration job, Taskfile (`db:up`, `migrate`, `sqlc`, `test:integration`, `bench`), `README.md` + `CLAUDE.md`, this file at `docs/PLAN.md`. -> `jobrun bench --rate 200 --duration 60s` meets the pass criterion; `task check`, `go test -race -count=1 ./...`, `golangci-lint run ./...` green.

## Fanout: 5 workstreams + integrator, exclusive file ownership
| WS | Owns (exclusive) | Depends on | Steps |
|---|---|---|---|
| A - contracts & infra | `proto/**`, `gen/**`, `compose.yaml`, `.env.example`, `sqlc.yaml`, `Taskfile.yml`, `.github/**`, `Dockerfile`, echo deletions | none | 1, 2, 12(CI) |
| B - domain & cron | `internal/job/**`, `internal/cronx/**` | none (ports frozen by this file) | 3, 4 |
| C - persistence | `internal/postgres/**`, `db/query/**` | A(1,2), B(3) | 5, 6 |
| D - runtime | `internal/worker/**`, `internal/scheduler/**`, `internal/reaper/**`, `internal/handlers/**` | B(3) | 7, 8 |
| E - edges | `internal/server/**`, `internal/admin/**`, `internal/obs/**`, `web/**`, `deploy/grafana/**` | A(2), B(3) | 9, 10 |
| INT - integrator | `cmd/**`, `internal/config/**`, `go.mod`, `go.sum`, `README.md`, `CLAUDE.md`, `docs/**` | all | 11, 12 |

**A lands first and alone** (protos + compose + sqlc.yaml frozen), then **B alone** (everything else compiles against `internal/job`), then **C, D, E in parallel** against the signatures in this document - changing one means amending this file and every fake. The integrator owns all wiring, resolves the expected `go mod tidy` conflicts, runs the benchmark and writes the numbers into the README. One Conventional commit per workstream (`feat(proto):`, `feat(job):`, `feat(postgres):`, `feat(worker):`, `feat(admin):`, `feat(cmd):`).

## Risks
- **Clock skew on `run_at`.** Mixing the app clock and the DB clock makes jobs fire early or hide forever. Rule: every comparison and default uses the *database* clock (`now()`, or a `SELECT now()` threaded in as `$2`); `Delay` becomes `now() + interval` in SQL; only an absolute client-supplied `run_at` crosses the wire, and the CLI sends RFC3339 with an offset. At startup, log a warning when `abs(db_now - time.Now()) > 2s`.
- **NOTIFY storms.** 200 enqueues/s is 200 notifications, and a naive listener wakes the claim loop 200 times for one batch of work. Mitigation: coalesce into a 1-slot per-queue channel, never sleep while batches come back full, and keep the poll floor so that dropping notifications is always safe - a disconnected listener silently misses them, which is a correctness trap, not a perf one. Payload is the queue name only (8000-byte limit).
- **Long transactions.** A handler's `Commit` runs inside the completion tx: a slow effect holds a row lock and a connection, and at `Concurrency=16` it can starve the claim loop. Mitigation: `Commit` closures are documented "writes only, no network", the completion tx sets its own `statement_timeout` (5 s), the pool is sized `Concurrency + 8`, and `QueueDepth` is cached rather than computed per `/metrics` scrape.
- **Windows `-race` needs a C compiler.** `task test` assumes gcc on PATH; if it is missing, run `task test:fast` and **say so explicitly** instead of claiming a race-clean run.
- **Cron parsing: in-house vs `robfig/cron/v3`.** Recommend **in-house `internal/cronx`**: we need exactly one function, `Next(after) time.Time` in a named timezone. robfig ships a whole scheduler/runner whose lifecycle half is dead weight here, and its parser is more permissive than what `ScheduleService.Create` should accept. Five bitsets plus a next-time search is ~200 lines, fuzzable, and makes DST behaviour *our* documented decision (gap -> skip, repeat -> fire once) instead of a library's. Cost: DST and the dom/dow OR rule are genuinely fiddly, so step 4 is table-tested against fixed vectors and fuzzed before anything depends on it. If step 4 overruns, fall back to `robfig/cron/v3` using only `cron.ParseStandard` and record the swap in `docs/PLAN.md`.
- **Priority inversion.** `ORDER BY priority DESC, run_at` lets a flood of high-priority jobs starve low ones. v1 accepts and documents it, the depth panel makes it visible, weighted claims are a follow-up.
- **`job_events` write amplification.** ~4 rows per job is ~700 rows/s at target. `JOB_EVENTS=terminal` roughly halves it and the reaper prunes; measure it during the bench before blaming the queue itself.
- **Dead jobs are silent** unless someone looks: `jobrun_dead_total` goes on the dashboard with an alert rule, the same discipline as orders' `orders_dlq_total`.

## Open questions for the owner
1. Cancel on a *leased* job: v1 sets `cancel_requested` and the heartbeat cancels the handler's context on the next tick, so the job ends `cancelled` rather than `dead`. Confirm, or make Cancel queued-only.
2. Payload as `jsonb` (queryable, validated, ~2x storage) vs `bytea` (opaque, proto-friendly). The plan assumes `jsonb` with a `bytea` escape hatch later; the proto field is `bytes` either way.
3. Multi-tenant queues: is `queue` enough, or is a `tenant` column needed now? It is far cheaper before the indexes exist than after.
4. Should the `jobrun` CLI also be able to embed the worker (`jobrun run --queues default`) so one binary covers both roles, or stay a pure client with `jobrund` as the only long-running process? The plan assumes the latter.
