# Distributed Job Scheduler

![CI](https://github.com/Shivanim042003/Job-scheduler/actions/workflows/ci.yml/badge.svg)

Asynchronous job submission, persistent job tracking, and execution across containerized workers.
**Java 17 · Spring Boot 3.5 · PostgreSQL · RabbitMQ · Docker**

```
                        ┌────────────┐   INSERT job (QUEUED)   ┌────────────┐
 client ── POST /jobs ─▶│  API (REST)│────────────────────────▶│ PostgreSQL │◀──────────┐
                        └─────┬──────┘                         └────────────┘           │
                              │ publish after commit (confirmed)                        │ claim / result
                              ▼                                                         │ (conditional UPDATEs)
                       jobs.exchange ──▶ jobs.queue ──▶ worker ×N (manual ack) ─────────┘
                              ▲              │   │
         TTL expiry           │              │   └── retries left ──▶ jobs.retry.<attempt> (TTL = backoff)
      (broker re-routes)      └──────────────┼───────────────────────────────┘
                                             └── exhausted / non-retryable ──▶ jobs.dlx ──▶ jobs.dlq
```

One codebase, two roles chosen by Spring profile: `api` (REST + sweeper) and `worker` (RabbitMQ consumer).
Workers are stateless; scale them with `docker compose up --scale worker=N`.

## Run it

```bash
docker compose up --build        # postgres, rabbitmq, 1 api, 3 workers
```

RabbitMQ management UI: http://localhost:15672 (guest / guest)

```bash
# submit a job (202 Accepted + Location header)
curl -s -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"type":"echo","payload":{"msg":"hello"}}'

# track it
curl -s localhost:8080/api/jobs/<id>
curl -s 'localhost:8080/api/jobs?status=DEAD'
```

Built-in job types for poking at the failure paths:

| type    | payload                        | behaviour                                                       |
|---------|--------------------------------|-----------------------------------------------------------------|
| `echo`  | any                            | logs and succeeds                                               |
| `sleep` | `{"millis": 5000}`             | sleeps, then succeeds (kill a worker mid-sleep to see redelivery) |
| `flaky` | `{"failUntilAttempt": 2}`      | fails the first N attempts, then succeeds                       |
| `fail`  | any                            | always fails: retries, then dead-letters                        |
| other   | any                            | no handler: dead-lettered immediately (non-retryable)           |

Add your own by registering a `JobHandler` bean.

## Job lifecycle

```
QUEUED ──claim──▶ RUNNING ──success──▶ SUCCEEDED            (terminal)
                    │  ▲
          failure,  │  └── delay queue TTL expires, redelivered
       attempts left▼
                  RETRYING
RUNNING ──retries exhausted / non-retryable──▶ DEAD         (terminal, message in jobs.dlq)
```

## How the reliability pieces work

**Manual acknowledgements.** The worker acks a message only after the outcome is recorded in PostgreSQL.
If a worker dies mid-job the message is still unacked, so the broker redelivers it to another worker. Delivery is
at-least-once; the claim logic below makes that safe.

**Safe redelivery.** Job state changes are conditional `UPDATE ... WHERE status IN (...)` statements, so two workers
can never both claim a job. On a delivery the worker decides:
- job already `SUCCEEDED`, or `RUNNING` on a first delivery (someone else owns it) → ack and ignore;
- job `RUNNING` on a *redelivery* → the previous owner died, take it over;
- unknown job id → dead-letter.

**Bounded retries.** Every claim increments `attempts`, including attempts that ended in a worker crash. A job that
keeps killing its worker therefore still reaches `max_attempts` and is dead-lettered instead of looping forever.
`max_attempts` is configurable globally and per job (`maxAttempts` in the request, capped by the global value).

**Exponential backoff.** Delay after attempt *n* is `initial-delay × 2^(n-1)`, capped at `max-delay`. Implemented
with one RabbitMQ delay queue per attempt (`jobs.retry.1`, `jobs.retry.2`, ...), each with a fixed TTL that
dead-letters back onto the main exchange. Fixed TTL per queue avoids the head-of-line blocking you get with
per-message TTLs in a single queue.

**Dead-letter routing.** Exhausted and non-retryable jobs are rejected with `requeue=false`; the main queue's
dead-letter exchange routes them to `jobs.dlq` (with `x-death` headers) for inspection or replay.

**No lost submissions.** The job row is committed first and published after commit with publisher confirms. If the
API dies or the broker is down in between, the `StaleJobSweeper` republishes jobs stuck in `QUEUED`.

## Tests

```bash
mvn verify        # needs Docker: Testcontainers starts real PostgreSQL and RabbitMQ
```

`JobRecoveryIntegrationTest` runs the full app against real containers and covers:

- successful job → `SUCCEEDED` in one attempt
- transient failures retried with exponential backoff until success
- retry limit reached → `DEAD`, message present in the DLQ with a `rejected` x-death entry
- per-job `maxAttempts` and non-retryable failures (no retries)
- **worker dies mid-job** → message redelivered, job completes, executed exactly once
- **job that keeps crashing workers** → dead-lettered at the attempt limit without being executed again
- duplicate messages for `SUCCEEDED` / `DEAD` jobs → acked and ignored, state unchanged
- message for an unknown job → dead-lettered
- lost publish → sweeper recovers the job

`JobClaimConcurrencyIntegrationTest` races 8 workers on one job (repeated 5x) and checks exactly one wins, and that a
worker acting on a stale read cannot claim a job another worker already took.
`StaleWorkerFencingIntegrationTest` covers a worker that was presumed dead reporting late: its result is rejected and
cannot overwrite the worker that took over, or reopen a finished job.
`IdempotentExecutionIntegrationTest` covers the effect ledger, including a job re-executed after its side effect was
applied (handler runs twice, effect once).
`JobApiIntegrationTest` covers submission (202), tracking, 404 and validation errors; `MetricsIntegrationTest` checks
the Prometheus endpoint; `BackoffPolicyTest` covers the delay math.

## What the conditional updates guarantee (and what they don't)

- **Claim:** `UPDATE ... WHERE status IN (claimable) AND attempts = <attempts I read>`. Two workers racing on the
  same job cannot both win, including two racing to take over a crashed worker's job.
- **Result (fencing):** success, retry and dead-letter writes carry `WHERE status = RUNNING AND worker_id = <me> AND
  attempts = <my attempt>`. A worker that was presumed dead and replaced gets zero rows updated, discards its
  result, and schedules nothing. It also cannot reopen a finished job.
- **Not guaranteed:** exactly-once *execution*. A crash after a handler's side effect but before the result is
  recorded re-runs the handler. Use `IdempotentEffects.runOnce(jobId, key, effect)` so the effect is applied once:
  exactly once for effects in this database (ledger row and effect commit in one transaction), and for external
  systems by passing `jobId + key` as their idempotency key.
- **Delivery:** consumer side is manual acks (a crashed worker's message is redelivered). Producer side is a
  committed row, publisher confirms, and a sweeper that republishes jobs stuck in `QUEUED`.

## Observability

The API exposes Prometheus metrics at `http://localhost:8081/actuator/prometheus`:

| metric | meaning |
|---|---|
| `jobs_by_status{status}` | jobs in each lifecycle state (from PostgreSQL) |
| `jobs_queue_depth{queue}` | messages in `main`, `retry` and `dead_letter` queues (from RabbitMQ) |
| `jobs_execution_seconds{outcome}` | execution time per attempt, recorded by workers |
| `jobs_retries_total`, `jobs_dead_total{reason}`, `jobs_stale_results_total` | retries scheduled, jobs dead-lettered, discarded stale results (workers) |

Worker-side metrics live in each worker's registry; the compose file runs workers without a web server, so to scrape
them remove `SPRING_MAIN_WEBAPPLICATIONTYPE: none` from the worker service.

## Benchmarking

`scripts/loadtest.py` (Python 3.8+, no dependencies) submits a burst of jobs, waits for them to finish, and reports
throughput and p50/p95/p99 latency from the server's own timestamps. To measure scaling:

```bash
docker compose up -d --scale worker=1 && python scripts/loadtest.py --jobs 300 --work-ms 100
docker compose up -d --scale worker=3 && python scripts/loadtest.py --jobs 300 --work-ms 100
docker compose up -d --scale worker=6 && python scripts/loadtest.py --jobs 300 --work-ms 100
```

The ceiling is roughly `workers x consumers-per-worker / work-ms` (2 consumers per worker by default). Add the
results here once measured, together with the machine they were measured on.

## Known limitations / next steps

- At-least-once, not exactly-once: handlers should be idempotent, since a crash after a handler's side effect but
  before the result is recorded causes a re-run.
- Changing `initial-delay`, `max-delay` or `max-attempts` changes retry-queue arguments, which RabbitMQ refuses to
  re-declare; delete the `jobs.retry.*` queues when tuning.
- No job cancellation, priorities, scheduled (future) start times, or idempotency keys on submission.
- No authentication on the REST API.
