# jobrun

A durable job queue on Postgres, in one Go binary: gRPC control plane, in-process worker pool
with fenced leases, cron scheduler, retries with backoff, a dead-letter state you can `retry`,
exactly-once side effects for handlers on the transaction path, and a live `/admin` page.
No broker, no cron library, no Prometheus client: `SELECT ... FOR UPDATE SKIP LOCKED` plus
`LISTEN/NOTIFY` does the dispatching and one Postgres does 1000 jobs/s off a partial index.
Design record: [`docs/PLAN.md`](docs/PLAN.md) (ADR-0001).

## Architecture

```
 jobrun CLI / any gRPC client
        |  Enqueue / Get / List / Cancel / Retry / Purge / Stats   (JobService)
        |  Create / Update / Delete / Get / List                   (ScheduleService)
        v
 +-- jobrund ---------------------------------------------------------------------------------+
 |  gRPC :50052  -> interceptors (Recover > RequestID > Logging) -> server.JobServer            |
 |                                                                       |                     |
 |        Store.Enqueue (one tx): INSERT jobs ON CONFLICT (queue, idempotency_key)             |
 |                                INSERT job_events, pg_notify('jobrun_ready', queue)          |
 |                                                                       |                     |
 |  Notifier (LISTEN, 1 conn) --wake--> Pool claim loop per queue <--tick-- poll floor 200 ms   |
 |                                       |  Claim: FOR UPDATE SKIP LOCKED, attempts++, lease   |
 |                                       v                                                     |
 |                        bounded handler goroutines (Concurrency) <-- heartbeater extends     |
 |                            |                     |                  leases, fences lost ones |
 |                     ok: Store.Complete (one tx)   err: Store.Fail                             |
 |                         handler Commit + processed_effects        queued@run_at+backoff       |
 |                         + owner-guarded UPDATE + job_events       or dead / cancelled         |
 |                                                                                             |
 |  Scheduler (advisory-lock leader, tick 5 s): due schedules -> Enqueue(key sched:<id>:<ts>)  |
 |  Reaper (tick LEASE_TTL/3): expired leases -> requeue with backoff or dead; retention purge  |
 |  Broker: every transition -> /admin/events (SSE), /admin/stats, /metrics, OTel counters     |
 +---------------------------------------------------------------------------------------------+
        |
   Postgres 17 (:5434): jobs, job_events, schedules, processed_effects
```

## Guarantees

- **At-least-once execution, at-most-`max_attempts` starts.** `attempts` increments at claim
  time, so a process that dies mid-job burns exactly one attempt; the reaper requeues the lease.
- **Exactly-once effects** for handlers that implement `job.TxHandler`: the returned `Commit`
  runs inside the completion transaction together with a `processed_effects` row keyed
  `("job:"+type, idempotency_key|id)` and the owner-guarded `jobs` update. A lost lease rolls
  the effect back; a `Retry` of a job whose effect already committed skips the closure.
- **Lease fencing.** Heartbeats return the ids still held; a missing id cancels that handler's
  context. `Complete`/`Fail` are owner-guarded, so a stale worker cannot resurrect a reaped job.
- **Cooperative cancel.** `Cancel` on a queued job is immediate; on a leased job it sets
  `cancel_requested`, the next heartbeat cancels the handler and the job ends `cancelled`.
- **Retry ladder.** Exponential backoff with full jitter (`1s`, x2, cap `5m`); a retry waits as
  `queued` with a future `run_at`. `job.ErrPermanent` skips the ladder. Exhaustion is
  `status='dead'`, a state not a table, and `retry` resets it to `queued` with `attempts=0`.
- **Idempotency keys** are unique per queue: a replay returns the stored job (`created: false`),
  a different body is `ALREADY_EXISTS`. The scheduler stamps `sched:<schedule_id>:<next_run>`.
- **The clock rule.** Every comparison and default (`run_at`, leases, `finished_at`, purge age)
  uses the database clock; `jobrund` warns at startup when the app clock is off by more than 2 s.

## Quick start

```powershell
task db:up                                   # postgres:17 on :5434, healthchecked
task run                                     # migrates, then serves gRPC :50052 + admin 127.0.0.1:9110
task build; .\bin\jobrun.exe health          # in another shell
```

Trimmed from the smoke run (`jobrun get --id ... --events` after `enqueue --type fail --payload '{"times":2}'`):

```
AT                        FROM         TO         ATTEMPT  ERROR
2026-09-19T11:39:31.762Z  unspecified  queued     0
2026-09-19T11:39:31.770Z  queued       leased     1
2026-09-19T11:39:31.773Z  leased       queued     1        fail: attempt 1 of 2 requested failures
2026-09-19T11:39:32.445Z  queued       leased     2
2026-09-19T11:39:32.449Z  leased       queued     2        fail: attempt 2 of 2 requested failures
2026-09-19T11:39:33.845Z  queued       leased     3
2026-09-19T11:39:33.849Z  leased       succeeded  3
```

A `{"permanent":true}` job goes `leased -> dead` on attempt 1 (`permanent failure: fail: permanent
requested`); `jobrun retry --id` adds `dead -> queued` with attempt 0. A `*/1 * * * *` schedule in
`America/New_York` materialised jobs with `runAt` on the exact minute and
`idempotencyKey: sched:01a0b978-...:2026-09-19T11:42:00Z`, claimed 6 ms after enqueue.

## CLI

`jobrun [-addr localhost:50052] [-timeout 5s] <command>`; single objects print as protojson, lists as tables.

| Command | Flags |
| --- | --- |
| `health` | |
| `enqueue` | `--type` `[--queue default] [--payload '{}'] [--key] [--priority] [--delay 5s \| --run-at RFC3339] [--max-attempts]` |
| `get` | `--id [--events]` |
| `list` | `[--queue] [--status queued\|leased\|succeeded\|dead\|cancelled] [--type] [--size 50] [--token]` |
| `cancel`, `retry` | `--id` |
| `purge` | `[--queue] [--status dead,cancelled] [--older-than 24h] [--limit]` |
| `stats` | `[--queue]` (cached depth by status) |
| `schedule create` | `--name --cron "*/1 * * * *" [--tz UTC] --queue --type [--payload] [--priority] [--max-attempts] [--disabled]` |
| `schedule list\|get\|update\|delete` | `--id`, `update` takes `[--cron] [--tz] [--enabled true\|false] [--type + template flags]` |
| `bench` | `[--rate 200] [--duration 60s] [--queue bench] [--type noop] [--direct] [--workers 8] [--wait 60s]` |

Built-in handlers: `noop`, `sleep` (`{"ms":N}`), `fail` (`{"times":N}` or `{"permanent":true}`), `echo`.
Reflection is on, so `grpcurl -plaintext localhost:50052 list` works too.

## Configuration (`internal/config`)

| Var | Default | Notes |
| --- | --- | --- |
| `DATABASE_URL` | required | `postgres://jobrun:jobrun@localhost:5434/jobrun?sslmode=disable` locally |
| `GRPC_ADDR` / `ADMIN_ADDR` | `:50052` / `127.0.0.1:9110` | ports frozen in docs/PLAN.md |
| `PGX_MAX_CONNS` | `WORKER_CONCURRENCY + 8` | must be >= concurrency + 2 |
| `WORKER_QUEUES` | `default` | comma list; `task run` uses `default,bench` |
| `WORKER_CONCURRENCY` / `WORKER_BATCH` | `16` / `32` | handler goroutines / jobs per claim |
| `LEASE_TTL` / `HEARTBEAT_EVERY` | `30s` / `10s` | heartbeat must be < TTL/2; reaper ticks TTL/3 |
| `JOB_TIMEOUT` | `10m` | per handler, also the pool's own drain budget |
| `POLL_FLOOR` | `200ms` | claim tick when NOTIFY is silent |
| `JOB_EVENTS` | `all` | `terminal` skips enqueue/claim/retry rows (~half the writes) |
| `SCHEDULER_ENABLED` / `SCHEDULER_TICK` / `SCHEDULER_LOCK_KEY` | `true` / `5s` / `0x6a6f6272756e` | leader election is an advisory lock |
| `JOB_RETENTION` / `REAPER_BATCH` | `168h` / `500` | finished jobs and their events are purged after retention |
| `SHUTDOWN_TIMEOUT` | `15s` | stage 1 budget (gRPC + admin drain) |
| `LOG_LEVEL` / `LOG_FORMAT` | `info` / `json` | |
| `OTEL_ENABLED` / `OTEL_EXPORTER_OTLP_ENDPOINT` | `false` / `http://localhost:4317` | traces + metrics to the devstack collector |

## Admin, metrics, SSE

`http://127.0.0.1:9110/admin` is the embedded page (depth table + live transition log).
`/admin/stats` is JSON (version, uptime, depth, pool counters, outcome tally); `/admin/events` is SSE
(`event: snapshot` with the depth table on connect, then `event: job` per transition, keepalive
comments while idle); `/metrics` is hand-written Prometheus text: `jobrun_queue_depth{queue,status}`
(5 s cache, never computed per scrape), `jobrun_jobs_total{queue,status}`,
`jobrun_job_duration_seconds{queue,type}` histogram, `jobrun_dead_total`, `jobrun_lease_expired_total`,
`jobrun_scheduler_ticks_total`, `jobrun_build_info`. The same `obs.Snapshot` feeds the OTel
observable instruments when `OTEL_ENABLED=true`; `deploy/grafana/jobrun.json` is the dashboard.

## Benchmark

`jobrun bench` is an open-loop enqueuer (ticker at `1/rate`, lagged ticks are counted, never queued),
then a drain watch on the cached depth and a `List` sweep computing `finished_at - created_at`
(both database-clock). Pass criterion from the plan: >= 167/s sustained, p99 < 1 s, flat depth
(backlog never above 2 s of the target rate). Measured on this box (Windows 11, Postgres 17 in
Docker Desktop/WSL2, defaults `WORKER_CONCURRENCY=16 WORKER_BATCH=32 POLL_FLOOR=200ms JOB_EVENTS=all`):

| run | enqueued | achieved | completed | p50 | p95 | p99 | max | max depth | verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `--rate 200 --duration 60s` (gRPC) | 11999 | 200.0/s | **200.3/s** | 6 ms | 7 ms | **9 ms** | 45 ms | 2 | PASS |
| `--rate 500 --duration 30s` (gRPC) | 14996 | 499.8/s | 500.0/s | 8 ms | 11 ms | 23 ms | 40 ms | 6 | PASS |
| `--rate 1000 --duration 30s --direct` | 30000 | 996.7/s | 996.8/s | 131 ms | 170 ms | 179 ms | 206 ms | 140 | PASS |

No knob tuning was needed; the target passes with 12x headroom on the direct path. The direct row's
latency is the 100 ms COPY batch tick plus the poll floor, not the queue. Knobs that matter after
that: `WORKER_BATCH` x `WORKER_CONCURRENCY` (claims per poll), `POLL_FLOOR`, `PGX_MAX_CONNS`,
`JOB_EVENTS=terminal` to halve the `job_events` writes.

## Teardown

Ctrl-Break / SIGTERM runs six stages, each with its own budget, logged as `teardown N/6`:
(1) health `NOT_SERVING`, gRPC `GracefulStop` and admin drain (`SHUTDOWN_TIMEOUT`);
(2) cancel the pool: stop claiming, wait up to 30 s for in-flight handlers, whatever remains keeps
its lease and is reaped later; (3) cancel scheduler (releases the advisory lock) and reaper;
(4) cancel the notifier connection and depth cache; (5) close the pgx pool; (6) flush OTel.

```
teardown 1/6: grpc + admin drained
teardown 2/6: worker pool stopped  stats="{Claimed:56999 Succeeded:56999 Failed:0 Retried:0 Dead:0 LeaseLost:0 InFlight:0}"
teardown 3/6: scheduler + reaper stopped  reaper="{Ticks:21 Reaped:0 Purged:0}" scheduler_ticks=44
teardown 4/6: notifier + depth cache stopped
teardown 5/6: postgres pool closed
teardown 6/6: otel flushed
```

## Testing and definition of done

Unit tests run against in-memory fakes (`internal/worker`, `internal/scheduler`, `internal/reaper`,
`internal/server` over bufconn, `internal/admin`, `internal/cronx` with DST vectors and a fuzzer).
The store tests need the stack: `task test:integration` (`-tags integration`, `TEST_DATABASE_URL`).
Done means `go build ./...`, `go vet ./...`, `golangci-lint run ./...`, `go test -race -count=1 ./...`,
the integration tests, `buf lint`, and `buf generate` leaving `gen/` unchanged.

## Not done yet

- Out-of-process workers over the reserved `WorkerService` (needs the `Dispatcher` seam and degrades
  exactly-once effects to effectively-once).
- `traceparent` stored on the job at enqueue so `jobrun.run` links to `jobrun.enqueue` across processes.
- Lost-lock detection in the scheduler beyond re-election on the next tick.
- Priority starvation: `ORDER BY priority DESC` lets a flood of high-priority jobs starve low ones.
- Per-queue concurrency limits (one `WORKER_CONCURRENCY` covers every queue).
