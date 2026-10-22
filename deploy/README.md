# Deploying jobrun

| Path | What | How far it was taken |
| --- | --- | --- |
| [`helm/jobrun`](helm/jobrun) | Helm chart: jobrund server + jobworker Deployments, Service, HPA, PDB, NetworkPolicy, ServiceMonitor, helm test | **Run on a local kind cluster** (below), plus `helm lint --strict`, `helm template`, `kubeconform` in CI |
| [`local/`](local) | kind cluster config, throwaway Postgres StatefulSet, demo values | **Run** (below), cluster torn down afterwards |
| [`terraform/aws`](terraform/aws) | EKS + RDS Postgres 17 + VPC + IRSA + ECR | **Validated only** (`fmt`, `validate`, `tflint`); never planned or applied. Cost estimate in its README |
| [`grafana/jobrun.json`](grafana/jobrun.json) | Grafana dashboard for the `/metrics` series | existing |
| [`../.github/workflows/deploy.yml`](../.github/workflows/deploy.yml) | CI for all of the above | runs on PRs touching `deploy/` |

## How the chart maps to jobrun's behaviour

- **Two workloads, one image.** `server` runs `jobrund` (gRPC :50052, in-process pool, scheduler,
  reaper, admin :9110). `worker` runs `/jobworker` against the server Service over `WorkerService`;
  it has no database access and its leases are owned by `remote:<pod name>` (`WORKER_ID` from the
  downward API). The image ships `jobrund`, `jobworker` and the `jobrun` CLI.
- **Probes.** Startup and readiness use the native gRPC probe against `grpc.health.v1.Health`;
  readiness asks for `jobrun.v1.JobService`, which jobrund flips to `NOT_SERVING` in teardown
  stage 1, so a stopping pod leaves the endpoints before its listener closes. Liveness is
  `/healthz` on the admin port, which does not touch Postgres: a database outage makes pods
  unready, never restarts them. The startup probe allows 90 s because migrations run before the
  gRPC listener opens. `jobworker` opens no port, so it has no probes (stated in the template).
- **Termination is sized from the code.** jobrund's teardown is staged with fixed budgets
  (`SHUTDOWN_TIMEOUT` + 30 s pool drain + 10 s + 5 s + 5 s); the chart adds a native `preStop`
  sleep (distroless has no shell) and refuses to render if `terminationGracePeriodSeconds` is
  below the sum. Same for `jobworker -drain`. Whatever is still running when the grace period
  ends is SIGKILLed, its lease expires after `LEASE_TTL`, and the reaper (every `LEASE_TTL/3`)
  requeues it - the demo below shows exactly that path.
- **Migrations with replicas > 1.** jobrund migrates on start. This branch adds a Postgres
  session advisory lock around goose (`internal/postgres/migrate.go`) so replicas starting at once
  serialise; the demo shows the second replica waiting for the first.
- **NetworkPolicy is the access control.** gRPC and admin have no auth (README, Limitations), so
  the server accepts :50052 only from this release's worker pods (plus `networkPolicy.grpcFrom`)
  and :9110 from nobody but kubelet unless `networkPolicy.adminFrom` is set. Workers accept
  nothing and may only reach DNS and the server. Egress from the server is DNS + Postgres
  (+ OTLP when enabled).
- **Hardening.** uid 65532, `runAsNonRoot`, read-only root FS, all capabilities dropped,
  `RuntimeDefault` seccomp, no ServiceAccount token mounted (jobrund never calls the API),
  `GOMEMLIMIT` from the memory limit, `maxUnavailable: 0` rollouts, soft zone/node spread.
- **Autoscaling.** CPU HPAs are optional. For workers the right signal is backlog,
  `jobrun_queue_depth{status="queued"}` from `/metrics`, via prometheus-adapter or KEDA;
  `worker.autoscaling.extraMetrics` takes the metric block (example in `values.yaml`).

## Local proof on kind (run 2026-10-01)

Windows 11, Docker Desktop 29.8.0 (WSL2), kind v0.33.0 (node image Kubernetes v1.37.0, kindnetd
enforces NetworkPolicy), Helm v4.3.0. Outputs below are copied from that run, trimmed only where
marked. Commands are bash (Git Bash on Windows, `MSYS_NO_PATHCONV=1`).

### 1. Cluster, image, Postgres, chart

```bash
docker build -t jobrun:dev --build-arg VERSION=deploy-k8s .
kind create cluster --config deploy/local/kind.yaml        # 1 control plane + 2 workers
kind load docker-image jobrun:dev --name jobrun
kubectl create namespace jobrun
kubectl -n jobrun apply -f deploy/local/postgres.yaml
kubectl -n jobrun rollout status statefulset/postgres
helm install jobrun deploy/helm/jobrun -n jobrun -f deploy/local/values-kind.yaml --wait
```

```
$ kubectl -n jobrun get pods -o wide
NAME                             READY   STATUS    RESTARTS   AGE   IP           NODE
jobrun-server-6d8c459cdb-pfr25   1/1     Running   0          2s    10.244.1.4   jobrun-worker
jobrun-server-6d8c459cdb-wxqnb   1/1     Running   0          2s    10.244.1.2   jobrun-worker
jobrun-worker-6bd649fb55-dc8ct   1/1     Running   0          2s    10.244.1.3   jobrun-worker
jobrun-worker-6bd649fb55-jvlk6   1/1     Running   0          2s    10.244.1.5   jobrun-worker
postgres-0                       1/1     Running   0          33s   10.244.2.3   jobrun-worker2
```

Both servers started together. `wxqnb` took the migration lock and applied every version once
(30.93-31.07 s); `pfr25` opened its pool at 30.98 s, waited on the lock (1 s poll) and carried on
at 32.0 s with nothing left to do. One of them won the scheduler's advisory lock, both workers
retried until a server was listening and then opened their Claim streams:

```
$ kubectl -n jobrun exec postgres-0 -- psql -U jobrun -d jobrun -c "select version_id, tstamp from goose_db_version order by id"
 version_id |           tstamp
------------+----------------------------
          0 | 2026-10-02 03:44:30.93007
          1 | 2026-10-02 03:44:30.95681
          2 | 2026-10-02 03:44:31.011063
          3 | 2026-10-02 03:44:31.043837
          4 | 2026-10-02 03:44:31.059784
          5 | 2026-10-02 03:44:31.073013

$ kubectl -n jobrun logs -l app.kubernetes.io/component=server --prefix      # trimmed
[pod/jobrun-server-...-pfr25] 03:44:30.980Z msg="postgres pool open" host=postgres.jobrun.svc max_conns=12
[pod/jobrun-server-...-pfr25] 03:44:32.015Z msg="database clock ok" skew=0s
[pod/jobrun-server-...-pfr25] 03:44:32.036Z msg="scheduler: following, another instance holds the lock"
[pod/jobrun-server-...-wxqnb] 03:44:30.911Z msg="postgres pool open" host=postgres.jobrun.svc max_conns=12
[pod/jobrun-server-...-wxqnb] 03:44:31.080Z msg="database clock ok" skew=0s
[pod/jobrun-server-...-wxqnb] 03:44:31.095Z msg="scheduler: became leader"
[pod/jobrun-server-...-wxqnb] 03:44:34.138Z msg="worker stream opened" owner=remote:jobrun-worker-6bd649fb55-jvlk6 queues=[remote]
[pod/jobrun-server-...-wxqnb] 03:44:34.138Z msg="worker stream opened" owner=remote:jobrun-worker-6bd649fb55-dc8ct queues=[remote]
```

### 2. helm test, health, NetworkPolicy

```
$ helm test jobrun -n jobrun
TEST SUITE:     jobrun-test-health
Phase:          Succeeded

$ kubectl -n jobrun exec deploy/jobrun-server -- /jobrun health
jobrun.v1.JobService       SERVING
jobrun.v1.ScheduleService  SERVING
```

The helm test pod carries the label the server's NetworkPolicy admits. An unlabelled pod in the
same namespace running the same command is dropped:

```
$ kubectl -n jobrun run np-probe --image=jobrun:dev --image-pull-policy=Never --restart=Never \
    --command -- /jobrun -addr jobrun-server:50052 -timeout 3s health
$ kubectl -n jobrun logs np-probe
error: health check jobrun.v1.JobService: rpc error: code = DeadlineExceeded desc = context deadline exceeded while waiting for connections to become ready
```

### 3. Jobs on both paths

In-process (`default` queue, handled inside the server pods): `fail {"times":1}` fails once,
retries with backoff, succeeds on attempt 2.

```
$ for i in 1 2 3; do kubectl -n jobrun exec deploy/jobrun-server -- /jobrun enqueue --type fail --payload '{"times":1}'; done
$ kubectl -n jobrun exec deploy/jobrun-server -- /jobrun list --queue default
ID                                    STATUS     QUEUE    TYPE  ATT  RUN_AT                    CREATED                   FINISHED
01a0fab7-a3ed-7ce3-bf4b-63df03763686  succeeded  default  fail  2/5  2026-10-02T03:45:34.870Z  2026-10-02T03:45:34.190Z  2026-10-02T03:45:34.885Z
01a0fab7-a1eb-7ef5-9884-68c85f41508b  succeeded  default  fail  2/5  2026-10-02T03:45:33.796Z  2026-10-02T03:45:33.676Z  2026-10-02T03:45:33.821Z
01a0fab7-a032-7e23-b135-988d05c36d64  succeeded  default  fail  2/5  2026-10-02T03:45:33.472Z  2026-10-02T03:45:33.235Z  2026-10-02T03:45:33.494Z
```

Remote (`remote` queue, handled by the jobworker pods): eight 15 s sleeps spread over the two
worker pods, four each (`-concurrency 4`):

```
$ for i in $(seq 1 8); do kubectl -n jobrun exec deploy/jobrun-server -- /jobrun enqueue --queue remote --type sleep --payload '{"ms":15000}'; done
$ kubectl -n jobrun exec postgres-0 -- psql -U jobrun -d jobrun -c \
    "select left(id::text,13) id, status, lease_owner, attempts, lease_token from jobs where queue='remote' order by lease_owner, id"
      id       | status |              lease_owner              | attempts | lease_token
---------------+--------+---------------------------------------+----------+-------------
 01a0fab7-dc2c | leased | remote:jobrun-worker-6bd649fb55-dc8ct |        1 |           1
 01a0fab7-dddc | leased | remote:jobrun-worker-6bd649fb55-dc8ct |        1 |           1
 01a0fab7-dfc9 | leased | remote:jobrun-worker-6bd649fb55-dc8ct |        1 |           1
 01a0fab7-e2a8 | leased | remote:jobrun-worker-6bd649fb55-dc8ct |        1 |           1
 01a0fab7-d8c9 | leased | remote:jobrun-worker-6bd649fb55-jvlk6 |        1 |           1
 01a0fab7-da66 | leased | remote:jobrun-worker-6bd649fb55-jvlk6 |        1 |           1
 01a0fab7-e159 | leased | remote:jobrun-worker-6bd649fb55-jvlk6 |        1 |           1
 01a0fab7-e3a7 | leased | remote:jobrun-worker-6bd649fb55-jvlk6 |        1 |           1
```

### 4. Kill a worker pod mid-job: lease recovery

`values-kind.yaml` sets `LEASE_TTL=6s`, `HEARTBEAT_EVERY=2s` (reaper tick 2 s) so this takes
seconds; the production default is 30 s. The worker holding four leases is SIGKILLed with no
grace period, so it cannot drain or report anything:

```
$ kubectl -n jobrun delete pod jobrun-worker-6bd649fb55-dc8ct --grace-period=0 --force     # at 03:46:00
$ # every 2 s: status | lease_owner | attempts | lease_token | count
--- 03:46:02
leased|remote:jobrun-worker-6bd649fb55-dc8ct|1|1|4      <- dead pod still holds 4 leases
leased|remote:jobrun-worker-6bd649fb55-jvlk6|1|1|3
succeeded|-|1|1|1
--- 03:46:04
leased|remote:jobrun-worker-6bd649fb55-dc8ct|1|1|4
leased|remote:jobrun-worker-6bd649fb55-jvlk6|1|1|1
succeeded|-|1|1|3
--- 03:46:07
queued|-|1|1|3                                          <- reaper requeued the expired leases
leased|remote:jobrun-worker-6bd649fb55-dssnz|2|2|1      <- the replacement pod claims them: attempt 2, new fencing token
succeeded|-|1|1|4
--- 03:46:09
leased|remote:jobrun-worker-6bd649fb55-dssnz|2|2|4
succeeded|-|1|1|4
  ... (still running their 15 s sleep)
--- 03:46:23
succeeded|-|1|1|4
succeeded|-|2|2|4                                       <- all 8 done; the 4 orphans on attempt 2
```

```
$ kubectl -n jobrun logs -l app.kubernetes.io/component=server --prefix | grep reaper
[pod/jobrun-server-6d8c459cdb-wxqnb/jobrund] 03:46:07.079Z level=WARN msg="reaper: expired leases requeued" n=4 dead=0

$ kubectl -n jobrun exec deploy/jobrun-server -- /jobrun get --id 01a0fab7-e2a8-7943-a7b1-096addedc04f --events
AT                        FROM         TO         ATTEMPT  ERROR
2026-10-02T03:45:50.248Z  unspecified  queued     0
2026-10-02T03:45:50.252Z  queued       leased     1
2026-10-02T03:46:07.076Z  leased       queued     1        lease expired
2026-10-02T03:46:07.327Z  queued       leased     2
2026-10-02T03:46:22.335Z  leased       succeeded  2

$ kubectl -n jobrun exec deploy/jobrun-server -- /jobrun stats
QUEUE    STATUS     COUNT  OLDEST_RUN_AT
default  succeeded  3      2026-10-02T03:45:33.472Z
remote   succeeded  8      2026-10-02T03:45:47.721Z
```

Kill to requeue: 7 s (last heartbeat + 6 s TTL, caught by the next 2 s reaper tick). Nothing
lost, nothing finished twice: the killed attempts never reported, and the bumped `lease_token`
would fence them if they had. This is at-least-once - the four handlers ran twice - which is
the documented guarantee for remote workers.

### 5. Graceful stop of a server pod mid-job: no lease lost

The contrast: a normal `kubectl delete` (SIGTERM after the 5 s `preStop` sleep) on a server pod
running four in-process 20 s jobs. Teardown stage 2 waits for them; they finish on attempt 1.

```
$ for i in 1 2 3 4 5 6; do kubectl -n jobrun exec deploy/jobrun-server -- /jobrun enqueue --type sleep --payload '{"ms":20000}'; done
              lease_owner              | count
---------------------------------------+-------
 jobrun-server-6d8c459cdb-wxqnb-1-650b |     4
 jobrun-server-6d8c459cdb-dxqqb-1-f683 |     2

$ time kubectl -n jobrun delete pod jobrun-server-6d8c459cdb-wxqnb               # at 03:47:21
real    0m16.341s

$ # kubectl logs -f captured before the delete, trimmed:
03:47:26.954Z msg="stop signal received; starting staged teardown"           <- after the 5 s preStop sleep
03:47:26.956Z msg="teardown 1/6: grpc + admin drained"
03:47:26.956Z msg="worker: stopped claiming, draining" in_flight=4
03:47:37.680Z msg="teardown 2/6: worker pool stopped" stats="{Claimed:11 Succeeded:10 Failed:1 Retried:1 Dead:0 LeaseLost:0 Fenced:0 InFlight:0}"
03:47:37.682Z msg="teardown 3/6: scheduler + reaper stopped"
03:47:37.682Z msg="teardown 4/6: notifier + depth cache stopped"
03:47:37.683Z msg="teardown 5/6: postgres pool closed"
03:47:37.683Z msg="teardown 6/6: otel flushed"

$ kubectl -n jobrun exec postgres-0 -- psql -U jobrun -d jobrun -c "select status, attempts, count(*) from jobs where queue='default' group by 1,2"
  status   | attempts | count
-----------+----------+-------
 succeeded |        1 |     6
```

### 6. API-server acceptance of the optional pieces, then teardown

kind has no metrics-server, so the HPAs were checked with a server-side dry run rather than
exercised:

```
$ helm template jobrun deploy/helm/jobrun -n jobrun -f deploy/local/values-kind.yaml \
    --set server.autoscaling.enabled=true --set worker.autoscaling.enabled=true \
  | kubectl -n jobrun apply --dry-run=server -f -
...
horizontalpodautoscaler.autoscaling/jobrun-server created (server dry run)
horizontalpodautoscaler.autoscaling/jobrun-worker created (server dry run)

$ kind delete cluster --name jobrun
Deleted nodes: ["jobrun-worker" "jobrun-control-plane" "jobrun-worker2"]
```

## Not exercised locally

- HPA scaling behaviour (no metrics-server in kind; manifests accepted by the API server only).
- ServiceMonitor (no prometheus-operator CRDs; the template refuses to render without them).
- OTLP egress, `adminFrom` scraping through the NetworkPolicy, multi-zone spread.
- Anything on AWS: see [`terraform/aws/README.md`](terraform/aws/README.md) - validated, never applied.
