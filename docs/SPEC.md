# taskq: Design Spec

> Status: **Planned** · Owner: Moin Palekar · Target: M3 (resume-complete) · Resume dates: Sep to Oct 2026
> Purpose of this doc: everything needed to build taskq from zero without re-deriving decisions.

---

## 1. Problem

Services constantly need work done *later* or *elsewhere*: send an email, resize an image, retrain a model, call a flaky third-party API. Doing it inline makes requests slow and fragile. A job queue decouples "this needs doing" from "doing it", and must guarantee that **no accepted job is ever lost**, even when workers crash mid-job, the network partitions, or pods get rescheduled.

taskq is a distributed job queue in Go that provides durable, at-least-once job execution across a horizontally scaled worker fleet, with retries, leases, dead-lettering, and observability, deployed on Kubernetes.

**Why this project exists (portfolio):** closes the concurrency and distributed-systems gap on the SDE and ML Infra resumes. Every interesting question in it (claiming without double-dispatch, crash recovery, stale workers, backpressure, graceful shutdown) is a standard systems-interview topic, answered with running code and numbers.

## 2. Goals and non-goals

### Goals
1. **Durability:** an enqueue that returns 2xx survives any single process crash. PostgreSQL is the source of truth.
2. **At-least-once execution:** every job eventually reaches a terminal state (`succeeded`, `dead`, `cancelled`).
3. **No double-dispatch under normal operation:** two live workers never hold the same job at once.
4. **Crash recovery:** a job held by a dead worker is re-dispatched after its lease expires.
5. **Stale-worker safety:** a worker that comes back after losing its lease cannot overwrite the result of the new holder (fencing).
6. **Horizontal scale:** throughput grows with worker replicas until Postgres becomes the bottleneck; that ceiling is measured, not guessed.
7. **Observability:** queue depth, latency, and outcome metrics in Prometheus with a Grafana dashboard.
8. **Correct without Redis:** Redis improves latency; if it is down, the system degrades to polling and stays correct.

### Non-goals
- Exactly-once execution (impossible for arbitrary side effects; handlers must be idempotent, see §6.6).
- Multi-region replication, Kafka-style ordered streams, or log retention.
- Workers in languages other than Go (an HTTP webhook handler type is an M5 option).

## 3. Architecture

```
            ┌──────────────┐  POST /v1/queues/{q}/jobs
 clients ──▶│  taskq-api   │───────────────┐
            └──────┬───────┘               │ INSERT (tx)
                   │ PUBLISH q.wake        ▼
            ┌──────▼───────┐        ┌──────────────┐
            │    Redis     │        │  PostgreSQL  │  ◀── source of truth
            │ wake + rate  │        │  jobs table  │
            └──────┬───────┘        └──────▲───────┘
                   │ SUBSCRIBE             │ claim: UPDATE ... FOR UPDATE SKIP LOCKED
            ┌──────▼─────────────────────────┴──────┐
            │  taskq-worker × N (goroutine pools)    │
            │  claim → heartbeat → run → ack/nack    │
            └──────────────────┬─────────────────────┘
                               │ (one replica holds pg advisory lock)
                        ┌──────▼───────┐
                        │   reaper     │ expired leases → requeue
                        │  (leader)    │ exhausted → dead
                        └──────────────┘
         Prometheus scrapes /metrics on api + workers → Grafana
```

### Components
| Binary | Role |
|---|---|
| `taskq-api` | HTTP/JSON API: enqueue, get, cancel, queue stats. Stateless, N replicas. |
| `taskq-worker` | Claims jobs, runs registered handlers in a bounded goroutine pool, heartbeats leases, acks/nacks. Also runs the reaper loop, active only on the replica holding the leader lock. |
| `taskqctl` | CLI: enqueue, inspect, requeue from DLQ, stats. |
| `taskq-bench` | Load generator and correctness checker (§9). |
| `pkg/client` | Go client SDK used by `taskqctl`, `taskq-bench`, and tests. |

### Key design decisions (write each up as an ADR in `docs/adr/`)
| # | Decision | Why | Rejected alternative |
|---|---|---|---|
| ADR-1 | Postgres is the source of truth | Transactional enqueue, durable, one system to reason about; `SKIP LOCKED` gives contention-free claiming | Redis as the queue: durability depends on AOF/fsync config, harder to reason about after failover |
| ADR-2 | Claim with `FOR UPDATE SKIP LOCKED` | Concurrent workers skip rows others are claiming instead of blocking | Advisory locks per job (lock leaks, more round-trips); `SELECT` then `UPDATE` (race) |
| ADR-3 | Leases + heartbeats, not "running forever" | Crash detection without a coordinator | Session-level locks (tie job ownership to a DB connection; breaks with poolers) |
| ADR-4 | Fencing token = `attempts` | A stale worker's ack fails its `WHERE` clause | Trusting the worker's clock or ID alone |
| ADR-5 | Redis pub/sub for wake-ups | Sub-poll-interval latency without holding a dedicated PG connection per worker | `LISTEN/NOTIFY` (needs a long-lived session connection; incompatible with transaction-mode PgBouncer) |
| ADR-6 | Reaper leader via `pg_try_advisory_lock` | Reuses Postgres, no extra coordinator (etcd/ZK) | K8s Lease API (ties correctness to the deployment platform) |
| ADR-7 | At-least-once + idempotency keys | Honest guarantee; dedupe at enqueue, idempotency at handler | Claiming exactly-once |

## 4. Data model

```sql
CREATE TYPE job_state AS ENUM ('queued','running','succeeded','dead','cancelled');

CREATE TABLE jobs (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  queue            TEXT        NOT NULL,
  type             TEXT        NOT NULL,           -- handler name
  payload          JSONB       NOT NULL,
  priority         SMALLINT    NOT NULL DEFAULT 0, -- higher runs first
  state            job_state   NOT NULL DEFAULT 'queued',
  attempts         INT         NOT NULL DEFAULT 0, -- doubles as fencing token
  max_attempts     INT         NOT NULL DEFAULT 5,
  run_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_expires_at TIMESTAMPTZ,
  locked_by        TEXT,                           -- worker id (pod name + pid)
  idempotency_key  TEXT,
  last_error       TEXT,
  result           JSONB,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at       TIMESTAMPTZ,
  finished_at      TIMESTAMPTZ,
  UNIQUE (queue, idempotency_key)
);

-- hot path: only queued rows, in dispatch order
CREATE INDEX jobs_dispatch_idx ON jobs (queue, priority DESC, run_at)
  WHERE state = 'queued';
-- reaper path
CREATE INDEX jobs_lease_idx ON jobs (lease_expires_at)
  WHERE state = 'running';
```

Migrations live in `migrations/` and run with `golang-migrate`.

### State machine
```
           enqueue
              │
              ▼
 ┌──────▶ queued ──── cancel ───▶ cancelled
 │            │ claim (attempts++)
 │            ▼
 │  nack /  running ── ack ──▶ succeeded
 │  lease       │
 │  expired     │ nack or lease expired AND attempts >= max_attempts
 └──(attempts   ▼
    < max)     dead  ── taskqctl requeue ──▶ queued (attempts reset)
```

## 5. API (HTTP/JSON)

| Method | Path | Body / notes | Response |
|---|---|---|---|
| POST | `/v1/queues/{queue}/jobs` | `{type, payload, priority?, run_at?, max_attempts?, idempotency_key?}` | `201 {id}`; `200 {id}` if idempotency key already exists |
| GET | `/v1/jobs/{id}` | | job record |
| POST | `/v1/jobs/{id}/cancel` | only from `queued` | `200` or `409` |
| POST | `/v1/jobs/{id}/requeue` | only from `dead` | `200` or `409` |
| GET | `/v1/queues/{queue}/stats` | counts per state, oldest queued age | stats |
| GET | `/healthz`, `/readyz`, `/metrics` | readyz checks PG ping | |

Errors: `{"error": {"code": "...", "message": "..."}}`. Payload cap 256 KiB (`413` above).

## 6. Core algorithms

### 6.1 Claim (batch)
```sql
UPDATE jobs SET state='running', attempts=attempts+1, locked_by=$1,
       lease_expires_at = now() + $2::interval, started_at = now()
WHERE id IN (
  SELECT id FROM jobs
  WHERE queue=$3 AND state='queued' AND run_at <= now()
  ORDER BY priority DESC, run_at
  LIMIT $4
  FOR UPDATE SKIP LOCKED)
RETURNING id, type, payload, attempts;
```
`$4` = free slots in the worker pool, which gives natural backpressure: a saturated worker claims nothing.

### 6.2 Ack / nack (fenced)
```sql
-- ack
UPDATE jobs SET state='succeeded', result=$4, finished_at=now(), locked_by=NULL
WHERE id=$1 AND locked_by=$2 AND attempts=$3 AND state='running';
```
0 rows affected ⇒ this worker lost the lease; log `taskq_stale_ack_total` and drop the result.

Nack: if `attempts < max_attempts` set `state='queued', run_at = now() + backoff(attempts)`, else `state='dead'`. Same fencing `WHERE`.

### 6.3 Backoff
`delay = min(base · 2^(attempt-1), cap) · U(0.5, 1.0)`; defaults `base=1s, cap=5m`. Jitter prevents synchronized retry storms.

### 6.4 Lease and heartbeat
- Lease default 30s; heartbeat every lease/3 extends `lease_expires_at` (fenced like ack).
- Handler receives a `context.Context` cancelled if a heartbeat fails, so it can stop early.

### 6.5 Reaper (leader only)
Every 5s on the replica holding `pg_try_advisory_lock(taskq_reaper)`:
1. `running` with `lease_expires_at < now()`: requeue with backoff, or `dead` if attempts exhausted.
2. Emit `queue_depth` and `oldest_queued_age_seconds` gauges.

The lock is session-scoped on a dedicated connection; if the leader dies, the connection drops, the lock frees, and another replica takes over within one tick.

### 6.6 Delivery guarantee (state it in the README exactly)
At-least-once. A job can run twice if a worker finishes the side effect but crashes before ack. Mitigations: idempotency keys dedupe enqueues; handlers get `(job_id, attempt)` to key their own side effects.

### 6.7 Worker runtime
- Fixed pool of `concurrency` goroutines fed by a channel; a claimer loop fills free slots.
- Wake-ups: Redis `SUBSCRIBE taskq:wake:{queue}` triggers an immediate claim; fallback poll every 1s (jittered). Redis down ⇒ poll only.
- Graceful shutdown on SIGTERM: stop claiming, wait for in-flight up to `shutdown_grace` (< K8s `terminationGracePeriodSeconds`), then release remaining jobs (`state='queued'`, `attempts=attempts-1`, fenced).
- Handlers registered by type: `w.Handle("resize", func(ctx, job) (result, error))`.
- Built-in test handlers: `sleep{ms}`, `fail{times}`, `cpu{iters}`, `crash{p}` (exits process with probability p, for chaos tests).

### 6.8 Rate limiting (M4)
Per-queue token bucket in Redis via a Lua script, checked before claim. Lets a queue protect a downstream API (e.g. 50 jobs/s across all workers).

## 7. Observability

| Metric | Type | Labels |
|---|---|---|
| `taskq_jobs_enqueued_total` | counter | queue, type |
| `taskq_jobs_completed_total` | counter | queue, type, outcome (succeeded/retried/dead) |
| `taskq_enqueue_to_start_seconds` | histogram | queue |
| `taskq_job_duration_seconds` | histogram | queue, type |
| `taskq_queue_depth` | gauge | queue |
| `taskq_oldest_queued_age_seconds` | gauge | queue |
| `taskq_stale_ack_total` | counter | queue |
| `taskq_claim_batch_size` | histogram | queue |

Structured JSON logs (`log/slog`) with `job_id`, `attempt`, `worker_id`. Grafana dashboard JSON committed in `deploy/grafana/`.

## 8. Repo layout
```
cmd/{taskq-api,taskq-worker,taskqctl,taskq-bench}/main.go
internal/store/      # Postgres queries (pgx), migrations runner
internal/notify/     # Redis wake-ups + rate limiter
internal/worker/     # pool, claimer, heartbeater, shutdown
internal/reaper/
internal/api/        # handlers, validation, errors
internal/metrics/
pkg/client/          # public Go SDK
migrations/
deploy/compose/docker-compose.yml
deploy/k8s/          # kustomize base + overlays/kind
deploy/grafana/
test/integration/    # testcontainers-go
test/chaos/          # pod-kill scripts
docs/SPEC.md  docs/adr/  docs/bench/
```

**Stack:** Go 1.23, `pgx/v5`, `go-redis/v9`, `chi` router, `prometheus/client_golang`, `log/slog`, `golang-migrate`, `testcontainers-go`, Docker, kind, kustomize, KEDA (M4), GitHub Actions.

## 9. Testing and benchmarks

### Correctness
- Unit tests for backoff, state transitions, API validation.
- Integration tests against real Postgres + Redis (testcontainers), always with `go test -race`.
- **Invariant test (the headline):** enqueue N=100k jobs whose handler records `(job_id, attempt)` to a table; run W workers; randomly `SIGKILL` workers throughout. Assert: every job terminal, **0 lost jobs**, duplicate executions counted and reported.
- Stale-worker test: pause a worker (SIGSTOP) past its lease, let another claim, resume; assert the stale ack is rejected.
- Leader failover test: kill the reaper leader; assert a new leader within one tick.

### Benchmarks (`taskq-bench`, results in `docs/bench/`)
| Measurement | Setup |
|---|---|
| Throughput (jobs/s) vs worker replicas 1, 2, 4, 8 | no-op handler, kind cluster, fixed PG resources |
| Enqueue-to-start latency p50/p95/p99 | with vs without Redis wake-ups |
| Lost jobs under chaos | pod kills every 10s during sustained load |
| Saturation point | where PG CPU or lock waits flatten throughput |

Results fill the resume placeholders: XX jobs/s, YY ms p99, 0 lost jobs across ZZ forced failures, NN× throughput scaling.

## 10. Milestones

| M | Scope | Done when |
|---|---|---|
| **M1: Core** | Schema + migrations, enqueue/get API, worker claim with `SKIP LOCKED`, ack/nack, retries with backoff, docker-compose, CI (lint, `test -race`) | Jobs flow end to end locally; integration tests green. **Repo goes public, README v1.** |
| **M2: Reliability** | Leases, heartbeats, fencing, reaper with advisory-lock leader, DLQ + requeue, idempotency keys, graceful shutdown, `taskqctl` | Invariant test passes with random SIGKILLs; stale-worker test passes |
| **M3: Distributed + observable** *(resume target)* | Redis wake-ups, Prometheus metrics, Grafana dashboard, K8s manifests on kind (api, workers, PG, Redis), `taskq-bench`, first benchmark report | Throughput-vs-replicas and latency numbers published in `docs/bench/`; README has the diagram and results table |
| **M4: Production polish** | KEDA autoscaling on queue depth, Redis rate limiting, chaos test against pods, PgBouncer | Chaos run report with 0 lost jobs; autoscale demo GIF |
| **M5: Extensions** *(optional)* | Cron/recurring jobs, job dependencies (DAG), HTTP webhook handler type, small web dashboard | Any one shipped with tests |

## 11. Resume mapping

Paste the SDE / ML Infra resume bullets for taskq here. Each must point to a milestone and to the artifact that proves it.

| Bullet (paste) | Milestone | Evidence in repo |
|---|---|---|
| | | |

Typical claim → evidence pairs:
- concurrent claiming without double-dispatch → §6.1 + invariant test
- crash recovery / zero lost jobs → chaos report in `docs/bench/`
- throughput and latency figures → `docs/bench/` tables
- Kubernetes deployment and scaling → `deploy/k8s/`, scaling chart

## 12. Interview talking points
- Why `SKIP LOCKED` and what happens without it (lock queueing, convoy).
- Why at-least-once and how idempotency closes the gap.
- Fencing tokens: the paused-worker scenario and why clocks are not enough.
- Why Postgres over Redis as the source of truth; where Postgres stops scaling and what comes next (partitioning by queue, sharding).
- Backpressure: claiming only free slots vs unbounded buffering.
- Graceful shutdown under Kubernetes termination semantics.

## 13. Open questions
- Partition `jobs` by state or time once finished rows dominate? (Retention job to delete finished rows older than N days is the M3 default.)
- gRPC alongside HTTP? Deferred to M5.
- Per-job timeouts separate from lease length? Default: handler timeout = `max_runtime` field, lease renewed independently.
