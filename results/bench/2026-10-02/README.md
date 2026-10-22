# Benchmark rerun, 2026-10-02

This rerun checks the numbers in the main README's Benchmark table, which came from 2026-09-19/22 runs whose
raw output was never committed. Every number below comes from a file in [`raw/`](raw/).

## Machine

| | |
| --- | --- |
| CPU | AMD Ryzen 7 3700X, 8 cores / 16 threads, 4.05 GHz max |
| RAM | 32 GB |
| OS | Windows 11 Home 10.0.26200 |
| Docker | Docker Desktop 29.8.0, WSL2 engine (kernel 6.18.40.1-microsoft-standard-WSL2), 16 CPUs / 15.6 GiB allotted |
| Postgres | `postgres:17-alpine` from `compose.yaml` (17.11), port 5434, default config |
| Go | go1.27.0 windows/amd64, binaries built with `CGO_ENABLED=0 go build -trimpath -ldflags="-s -w"` at 63a0472 |

Host CPU was 8-10% before the runs (`\Processor(_Total)\% Processor Time`, 3 x 2 s samples) and no go/cargo/lint
builds were running. Twelve unrelated, idle compose containers (Grafana/Prometheus/Loki/Tempo stacks, Jellyfin,
Redis, another Postgres, NATS) stayed up in the same Docker VM. Runs were from 2026-10-03 02:46Z to 03:08Z (UTC),
which was the evening of 2026-10-02 locally.

## Method

- `jobrund` uses the repo defaults: `WORKER_CONCURRENCY=16 WORKER_BATCH=32 POLL_FLOOR=200ms JOB_EVENTS=all`,
  `PGX_MAX_CONNS` = default, `WORKER_QUEUES=default,bench` and `LOG_LEVEL=warn`. It logged nothing at warn in any run.
- Each run gets a **fresh database** (`create database jb_<run>` on the compose Postgres) and a **fresh `jobrund`**.
  `jobrund` migrates on start. After the run the process is killed and the database dropped. No run sees another run's rows.
- The load generator is `jobrun bench` with its defaults (`--type noop`, `--workers 8` gRPC enqueuers, `--wait 60s`,
  `--sample 5s`). The rate and duration match the README rows: `--rate 200 --duration 60s`, `--rate 500 --duration 30s`
  (gRPC), and `--rate 1000 --duration 30s --direct` (COPY batches on `DATABASE_URL`, bypassing the RPC).
- **Interleaved**: each round runs 200 gRPC, 500 gRPC, 1000 direct and 1000 gRPC back to back, three rounds in all.
  The saturation sweep (1500/2000/3000 direct) runs three interleaved rounds after that, and a 750 direct point runs
  three times last.
- Driver: [`run.sh`](run.sh). This copy is repo-neutral; on Windows the binaries were `.exe` and the script also ran
  `taskkill /F /IM jobrund.exe` after `kill`. A bug in the first script version stopped it after round 1. Round 1's
  files are unaffected, and rounds 2-3 were then started with the fixed script.
- Latency is `finished_at - created_at` on the database clock, for every job. "completed/s" is jobs over the span
  from the first creation to the last finish.

## Results (median of 3, with the range)

| run | achieved enqueue/s | completed/s | p50 | p99 median (range) | max depth | errors | verdict |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `--rate 200 --duration 60s` (gRPC) | 200.0 | **200.0** | 6 ms | **12 ms** (11-14) | 2-3 | 0 | 3/3 PASS |
| `--rate 500 --duration 30s` (gRPC) | 499.8 (491.7-499.8) | **499.9** (492.0-500.0) | 9 ms | **38 ms** (21-42) | 7-9 | 0 | 3/3 PASS |
| `--rate 750 --duration 30s --direct` | 747.5 | **750.8** (750.7-751.0) | 66 ms | **137 ms** (114-149) | 61-75 | 0 | 3/3 PASS |
| `--rate 1000 --duration 30s --direct` | 996.7 | **990.4** (979.4-993.8) | 188 ms | **1.042 s** (0.634-1.155) | 119-865 | 0 | 1/3 PASS (p99 >= 1 s) |
| `--rate 1000 --duration 30s` (gRPC) | 677.7 (654.8-681.7) | 677.9 (655.1-681.9) | 18 ms | 29 ms (28-37) | 11-14 | 0 | 3/3 PASS* |
| `--rate 1500 --duration 30s --direct` | 1495.0 | 1033.4 (1026.8-1075.3) | 5.1 s | 13.5 s | 10-13k | 0 | FAIL, saturated |
| `--rate 2000 --duration 30s --direct` | 1944.3 (1919.5-1945.8) | 1052.1 (1008.2-1059.9) | 13.1 s | 26.0 s | 25-28k | 0 | FAIL, saturated |
| `--rate 3000 --duration 30s --direct` | 2785.7 (2750.1-2813.5) | 1031.4 (1012.7-1033.7) | 25.3 s | 52.3 s | ~52k | 0 | FAIL, saturated |

"errors" means enqueue errors. In every run every enqueued job reached `succeeded`, with no dead, cancelled or missing jobs.

\* With 8 enqueuer goroutines the gRPC client tops out at ~680 enqueues/s (9-10k lagged ticks per run). This row measures
the client, not the queue: the server kept up with whatever arrived, and depth stayed at 14 or less.

## Reading

- **Saturation: ~1,030 jobs/s completed** (median of the nine 1500-3000/s runs, range 1,008-1,075/s) with the default
  16 in-process workers on one Postgres. Above that, the backlog grows linearly and latency is queueing time.
- **1000/s offered sits right at that knee.** Throughput holds (~990/s), but the backlog reaches ~860 in two of
  three runs and p99 is ~1 s (median 1.04 s, best 634 ms). The previous README row (996.8/s at a **179 ms p99**)
  **did not reproduce** on this run set.
- At **750/s** (direct) the queue is comfortably below the knee: p99 137 ms median.
- **500/s and 200/s over gRPC** roughly reproduce. The medians are 38 ms and 12 ms p99, against the earlier
  single-run 23 ms and 9 ms; the best runs here were 21 ms and 11 ms.
