# jobrun

[![ci](https://github.com/crowbett/jobrun/actions/workflows/ci.yml/badge.svg)](https://github.com/crowbett/jobrun/actions/workflows/ci.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/crowbett/jobrun.svg)](https://pkg.go.dev/github.com/crowbett/jobrun) [![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A durable job queue on Postgres, in one Go binary: gRPC control plane, in-process worker pool
with fenced leases, out-of-process workers over `WorkerService` (`pkg/jobworker`, `cmd/jobworker`),
cron scheduler, retries with backoff, a dead-letter state you can `retry`, exactly-once side
effects for in-process handlers on the transaction path, W3C trace context carried from enqueue to
run, and a live `/admin` page.
No broker, no cron library, no Prometheus client: `SELECT ... FOR UPDATE SKIP LOCKED` plus
`LISTEN/NOTIFY` does the dispatching and one Postgres does 1000 jobs/s off a partial index.
Design record: [`docs/PLAN.md`](docs/PLAN.md) (ADR-0001; ADR-0002 for remote workers, fencing tokens, traceparent).

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
- **Lease fencing with tokens.** Every claim bumps `jobs.lease_token`; heartbeat, `Complete` and
  `Fail` must present `(owner, token)`, so a stale attempt cannot extend or finish a job even when
  the same worker reclaimed it after a reap.
- **Lost-lock detection.** A lease a heartbeat reports lost - or one that could not be renewed
  before it may have expired (claim/last good heartbeat send time + TTL - one heartbeat period,
  monotonic clock) - is *fenced*: the handler's ctx is cancelled within one heartbeat period and
  the worker never calls `Complete` or `Fail` for it, even if the handler returns success.
  Counted as `jobrun_lease_fenced_total` (in-process pool and remote workers alike).
- **Cooperative cancel.** `Cancel` on a queued job is immediate; on a leased job it sets
  `cancel_requested`, the next heartbeat cancels the handler and the job ends `cancelled`.
- **Retry ladder.** Exponential backoff with full jitter (`1s`, x2, cap `5m`); a retry waits as
  `queued` with a future `run_at`. `job.ErrPermanent` skips the ladder. Exhaustion is
  `status='dead'`, a state not a table, and `retry` resets it to `queued` with `attempts=0`.
- **Idempotency keys** are unique per queue: a replay returns the stored job (`created: false`),
  a different body is `ALREADY_EXISTS`. The scheduler stamps `sched:<schedule_id>:<next_run>`.
- **The clock rule.** Every comparison and default (`run_at`, leases, `finished_at`, purge age)
  uses the database clock; `jobrund` warns at startup when the app clock is off by more than 2 s.

## Remote workers

`jobrund` also serves `jobrun.v1.WorkerService`: a worker opens one `Claim` stream (worker id,
queues, credits = free slots), receives leased jobs with their fencing token, lease TTL and
traceparent, and reports with unary `Heartbeat` / `Complete` / `Fail`. The server never pushes more
jobs than credits granted; a returned credit only triggers a claim when the stream was
credit-bound. Leases are owned by `remote:<worker_id>`, so `jobrun list` tells them apart.

```go
w := jobworker.New(conn, jobworker.Config{Queues: []string{"emails"}, Concurrency: 16})
_ = w.Handle("email.send", jobworker.HandlerFunc(func(ctx context.Context, j jobworker.Job) error {
	return send(ctx, j.ID, j.Payload) // idempotent on j.ID: effects are at-least-once
}))
err := w.Run(ctx) // reconnects with backoff; drains in-flight handlers on ctx.Done()
```

`jobworker -addr localhost:50052 -queues default,bench -concurrency 16` (or `task worker -- ...`)
runs the built-in handlers remotely.

**Guarantee: at-least-once effects.** A remote worker killed mid-job leaves its lease to expire;
the reaper requeues the job and another worker runs it again (the integration test kills a real
`jobworker` process by PID and watches exactly that, ~2.3 s end to end with a 2 s TTL).
`job.TxHandler` exactly-once stays in-process: its `Commit` must run inside the completion
transaction, which a remote process cannot join. Key remote effects on the job id.

## Tracing

`Enqueue` opens `jobrun.enqueue` under the caller's trace (otelgrpc) and stores its W3C
`traceparent` on the job (`Job.traceparent` in the API). The run span (`jobrun.run`, in-process or
in `pkg/jobworker`) is its child, and `jobrun.complete` is a child of the run span - for remote
workers the run span's context comes back on `Complete`/`Fail`. Retries are sibling run spans in
the same trace. Nothing is stored when the producer is untraced.

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
| `GRPC_TOKEN` | empty | bearer token for every gRPC call except `grpc.health.v1.Health` (>= 16 chars); the `jobrun` CLI and `jobworker` read the same variable. Empty is refused in `APP_ENV=prod` unless `GRPC_ADDR` is loopback; elsewhere `jobrund` warns at startup |
| `ADMIN_ALLOW_REMOTE` | `false` | the admin surface has no auth: a non-loopback `ADMIN_ADDR` is refused unless this is `true` (the Dockerfile sets it) |
| `ADMIN_CORS_ORIGINS` | empty | comma list of exact origins (`http://localhost:3000`) allowed to read `/admin/stats` and `/admin/events` cross-origin; empty sends no CORS header |
| `PGX_MAX_CONNS` | `WORKER_CONCURRENCY + 8` | must be >= concurrency + 2 |
| `WORKER_QUEUES` | `default` | comma list; `task run` uses `default,bench` |
| `WORKER_CONCURRENCY` / `WORKER_BATCH` | `16` / `32` | handler goroutines / jobs per claim |
| `CLAIM_FIFO_EVERY` | `10` | every n-th claim per queue takes the oldest ready jobs regardless of priority (anti-starvation; index `jobs_ready_fifo`); `0` = strict priority |
| `LEASE_TTL` / `HEARTBEAT_EVERY` | `30s` / `10s` | heartbeat must be < TTL/2; reaper ticks TTL/3 |
| `JOB_TIMEOUT` | `10m` | per handler, also the pool's own drain budget |
| `POLL_FLOOR` | `200ms` | claim tick when NOTIFY is silent |
| `JOB_EVENTS` | `all` | `terminal` skips enqueue/claim/retry rows (~half the writes) |
| `SCHEDULER_ENABLED` / `SCHEDULER_TICK` / `SCHEDULER_LOCK_KEY` | `true` / `5s` / `0x6a6f6272756e` | leader election is an advisory lock |
| `JOB_RETENTION` / `REAPER_BATCH` | `168h` / `500` | finished jobs and their events are purged after retention |
| `SHUTDOWN_TIMEOUT` | `15s` | stage 1 budget (gRPC + admin drain) |
| `LOG_LEVEL` / `LOG_FORMAT` | `info` / `json` | |
| `OTEL_ENABLED` / `OTEL_EXPORTER_OTLP_ENDPOINT` | `false` / `http://localhost:4317` | traces + metrics to the devstack collector |

## gRPC authentication

With `GRPC_TOKEN` set, every gRPC call (JobService, ScheduleService, WorkerService, reflection) must carry
`authorization: Bearer <token>`; anything else gets `Unauthenticated`. Only `grpc.health.v1.Health` stays open, for
probes. The comparison is constant-time over sha256 of both sides. It is one shared token, not per-client identity,
and the transport is plaintext, so the token crosses the wire in clear: put TLS (or a private network) in front when
jobrund is reachable off-host. Without a token (the dev default, `GRPC_ADDR=:50052` on every interface) anyone who
can reach the port can enqueue, lease and read jobs; `APP_ENV=prod` refuses to start that way.

## Admin, metrics, SSE

`http://127.0.0.1:9110/admin` is the embedded page (depth table + live transition log).
There is no authentication on any of these routes: `jobrund` refuses to start with an
`ADMIN_ADDR` that is not loopback unless `ADMIN_ALLOW_REMOTE=true`, and the container image
(which sets it) should publish 9110 to loopback only. `/admin/stats` and `/admin/events` send
no `Access-Control-Allow-Origin` by default, so only same-origin pages (the embedded `/admin` page)
can read them and no other site open in the operator's browser can. To let a separate dashboard read
them cross-origin, list its exact origin in `ADMIN_CORS_ORIGINS`; a matching `Origin` is echoed back,
anything else gets no header (`*` and `null` are refused at startup).
`/admin/stats` is JSON (version, uptime, depth, pool counters, outcome tally); `/admin/events` is SSE
(`event: snapshot` with the depth table on connect, then `event: job` per transition, keepalive
comments while idle); `/metrics` is hand-written Prometheus text: `jobrun_queue_depth{queue,status}`
(5 s cache, never computed per scrape), `jobrun_jobs_total{queue,status}`,
`jobrun_job_duration_seconds{queue,type}` histogram, `jobrun_dead_total`, `jobrun_lease_expired_total`,
`jobrun_lease_fenced_total`,
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

**Remote workers vs in-process at 500/s (2026-09-22, loaded box).** Six interleaved rounds
(A B C D, 30 s each, `--rate 500 --type noop`) against fresh databases on the same Postgres, with
other agents' workloads running alongside. A = the pre-change `jobrund` (e59b670), in-process pool;
B = this version, in-process pool; C = this version, `WORKER_QUEUES=default` plus two
`jobworker -concurrency 8` processes on `bench`; D = C with the server from before f11495e (every
returned credit triggered a claim).

| mode | completed/s median (range) | p50 | p99 median (range) | verdict |
| --- | --- | --- | --- | --- |
| A before, in-process | 477.9 (360.6-497.1) | 16 ms | 54 ms (37-83) | 6/6 PASS |
| B after, in-process | 450.9 (290.6-483.6) | 22 ms | 67 ms (51-130) | 6/6 PASS |
| C after, 2 remote workers | 354.8 (227.2-469.3) | 26 ms | 100 ms (52-141) | 6/6 PASS |
| D 2 remote workers, server before f11495e | 387.8 (283.0-453.4) | 25 ms | 82 ms (56-180) | 6/6 PASS |

Read this as a latency comparison, not a throughput one. In every run the completion rate equals the
*achieved enqueue rate* and depth stayed flat, so the workers kept up. The throughput spread is the
open-loop enqueuer on a loaded box dropping ticks: 486-3092 lagged ticks median, worse in later
rounds for every mode. The remote path costs roughly +10 ms p50 and +30-45 ms p99 over in-process at
this rate: one stream push plus two extra RPCs (Complete, credit) per job. The fencing token and
traceparent columns cost B vs A about +6 ms p50 / +13 ms p99, which is inside the spread of the
loaded rounds. C vs D is within noise at this load. f11495e removes an empty claim transaction per
completed job, which matters for Postgres load more than for latency.

No knob tuning was needed; the target passes with 12x headroom on the direct path. The direct row's
latency is the 100 ms COPY batch tick plus the poll floor, not the queue. Knobs that matter after
that: `WORKER_BATCH` x `WORKER_CONCURRENCY` (claims per poll), `POLL_FLOOR`, `PGX_MAX_CONNS`,
`JOB_EVENTS=terminal` to halve the `job_events` writes.

## Teardown

Ctrl-Break / SIGTERM runs six stages, each with its own budget, logged as `teardown N/6`:
(1) health `NOT_SERVING`, WorkerService `Drain` (ends open `Claim` streams; remote workers keep
reporting what they hold), gRPC `GracefulStop` and admin drain (`SHUTDOWN_TIMEOUT`);
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
`internal/server` and `pkg/jobworker` over bufconn, `internal/admin`, `internal/cronx` with DST vectors and a fuzzer);
tracing tests use the OTel in-memory exporter. The store tests and the remote-worker crash tests
(`cmd/jobworker`: kill a worker process by PID mid-job; a reaped lease reclaimed by the same worker)
need the stack: `task test:integration` (`-tags integration`, `TEST_DATABASE_URL`).
Done means `go build ./...`, `go vet ./...`, `golangci-lint run ./...`, `go test -race -count=1 ./...`,
the integration tests, `buf lint`, and `buf generate` leaving `gen/` unchanged.

## Not done yet

- Per-client identities on gRPC: `GRPC_TOKEN` is one shared bearer token, and there is no TLS (see
  "gRPC authentication").
- Lost-lock detection in the scheduler beyond re-election on the next tick.
- Priority is bounded, not weighted: with `CLAIM_FIFO_EVERY=n` a low-priority job is never starved, but it only
  advances through the 1-in-n age-ordered claims (no per-priority quotas or aging of the priority itself).
- Per-queue concurrency limits (one `WORKER_CONCURRENCY` covers every queue).

## License

MIT, see [LICENSE](LICENSE).
