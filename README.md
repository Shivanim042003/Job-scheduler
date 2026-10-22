# Distributed Job Scheduler

![CI](https://github.com/Shivanim042003/Job-scheduler/actions/workflows/ci.yml/badge.svg)

Asynchronous job submission, persistent job tracking, and execution across containerized workers, built to keep
working when workers crash, hang, or report late.
**Java 17, Spring Boot 3.5, PostgreSQL, RabbitMQ, Docker, Testcontainers**

## Results

- **Scales with workers:** 14.7 -> 78.0 jobs/s (5.3x) going from 1 to 6 worker containers on one machine, with no
  lost jobs in 3,000 benchmarked jobs (simulated 100 ms tasks). See [Benchmarks](#benchmarks).
- **Survives failures:** worker crashes, hung workers, late results from replaced workers, duplicate messages, lost
  publishes and client retries are each handled and each covered by an automated test. See
  [Failure modes handled](#failure-modes-handled).
- **48 automated tests:** 45 integration tests against real PostgreSQL and RabbitMQ (Testcontainers) and 3 unit
  tests. CI runs them on every push.

## Architecture

```
 client
   |  POST /api/jobs  (optional Idempotency-Key header)
   v
 +-----------------------------+   ONE transaction: job row + outbox row
 | API                         |-------------------------------------------+
 |  - REST + job tracking      |                                           v
 |  - outbox relay             |<---- reads pending outbox rows -----+  +------------+
 |  - lease reaper             |      (FOR UPDATE SKIP LOCKED)       +--| PostgreSQL |
 +--------------+--------------+                                        +------+-----+
                | publish batch + broker confirm                               ^
                v                                                              | conditional UPDATEs:
          jobs.exchange --> jobs.queue --> worker x N (manual ack) ------------+ claim, renew lease,
                                ^              |            |                   record result
                                |              |            +-- retries left --> jobs.retry.<n>
                                |              |                                 (TTL = backoff)
                                +--------------+-------------------------------------+
                                               |
                                               +-- exhausted / non-retryable --> jobs.dlx --> jobs.dlq
```

One codebase, two roles chosen by Spring profile: `api` (REST, outbox relay, lease reaper, sweeper) and `worker`
(RabbitMQ consumer). Workers are stateless; scale them with `docker compose up -d --scale worker=N`.

### Job lifecycle

```
QUEUED --claim--> RUNNING --success--> SUCCEEDED                 (terminal)
                   |   ^
        failure,   |   +-- backoff delay queue expires, or lease expired and reaper requeues
     attempts left v
                RETRYING
RUNNING --retries exhausted / non-retryable / lease expired with no attempts left--> DEAD   (terminal, in jobs.dlq)
```

## Run it

```bash
docker compose up --build        # postgres, rabbitmq, 1 api (host port 8081), 3 workers
```

Allow about 45 seconds for the JVMs to start. RabbitMQ management UI: http://localhost:15672 (guest / guest).

```bash
# submit a job (202 Accepted + Location header); repeat the call with the same key and you get the same job (200)
curl -s -X POST localhost:8081/api/jobs -H 'Content-Type: application/json' -H 'Idempotency-Key: order-42' \
  -d '{"type":"echo","payload":{"msg":"hello"}}'

# track it
curl -s localhost:8081/api/jobs/<id>
curl -s 'localhost:8081/api/jobs?status=DEAD'
```

Built-in job types for exercising the failure paths:

| type    | payload                   | behaviour                                                         |
|---------|---------------------------|-------------------------------------------------------------------|
| `echo`  | any                       | logs and succeeds                                                 |
| `sleep` | `{"millis": 5000}`        | sleeps, then succeeds (kill its worker mid-sleep to see recovery) |
| `flaky` | `{"failUntilAttempt": 2}` | fails the first N attempts, then succeeds                         |
| `fail`  | any                       | always fails: retries with backoff, then dead-letters             |
| other   | any                       | no handler: dead-lettered immediately (non-retryable)             |

Add your own by registering a `JobHandler` bean.

## Failure modes handled

| Failure | What happens | Test |
|---|---|---|
| Worker crashes mid-job | Message was never acked, so the broker redelivers it; another worker takes the job over, and the crashed attempt counts toward the retry limit | `JobRecoveryIntegrationTest` |
| Worker hangs but stays connected | Its lease expires; the reaper requeues the job, or dead-letters it if no attempts are left | `LeaseReaperIntegrationTest` |
| Replaced worker reports late | Result writes are fenced on worker + attempt, so a stale result is discarded and cannot reopen a finished job | `StaleWorkerFencingIntegrationTest` |
| Replaced worker tries a side effect | Effects are fenced on ownership too: a worker that no longer owns the job applies nothing | `FencedEffectsIntegrationTest` |
| Job re-run after its effect was applied | The effect ledger skips it: applied at most once per (job, key) | `IdempotentExecutionIntegrationTest`, `FencedEffectsIntegrationTest` |
| Job keeps killing its worker | Every claim counts as an attempt, so it hits the limit and is dead-lettered | `JobRecoveryIntegrationTest` |
| Many workers receive the same job | Claim is a compare-and-set on the attempt number; exactly one wins | `JobClaimConcurrencyIntegrationTest` |
| Transient handler failure | Retried with exponential backoff (`initial x 2^(n-1)`, capped) via per-attempt delay queues | `JobRecoveryIntegrationTest`, `BackoffPolicyTest` |
| Permanent failure or retries exhausted | Job marked `DEAD`, message routed to `jobs.dlq` | `JobRecoveryIntegrationTest` |
| Duplicate or unknown-job messages | Acked and ignored (finished jobs) or dead-lettered (unknown jobs) | `JobRecoveryIntegrationTest` |
| API dies after saving a job | Job and outbox row commit together, so the relay still publishes it | `OutboxIntegrationTest` |
| Broker down at submit time | Outbox rows wait in PostgreSQL and the relay retries (design; not covered by an automated test) | - |
| Client retries a `POST` | `Idempotency-Key` returns the original job; concurrent repeats race on a unique index | `IdempotencyKeyIntegrationTest` |

### What is and is not guaranteed

- **Delivery is at-least-once** end to end: transactional outbox and publisher confirms on the way in, manual acks on
  the way out. Duplicates are expected and made harmless; exactly-once *delivery* is not claimed.
- **At most one current owner:** a claim is a compare-and-set on the attempt number, and every result or effect
  write carries `worker_id` and `attempt`. A replaced worker's writes are rejected.
- **Effects:** `IdempotentEffects.runOnceAsOwner` applies an effect at most once per (job, key) and only while the
  caller still owns the job. For effects inside this database that is exactly-once. For external systems (email,
  payments) pass `jobId + key` as their idempotency key. Anything a handler does outside this API can repeat.
- **Not handled:** a handler stuck in an infinite loop inside a healthy worker (its heartbeat keeps running; give
  handlers their own timeout), and high availability of PostgreSQL or RabbitMQ (single node each).

## How the pieces work

- **Transactional outbox.** Submit writes the job and an `outbox` row in one transaction. A relay in the API reads
  pending rows (`FOR UPDATE SKIP LOCKED`, so several API instances can share the work), publishes a batch with one
  broker confirmation, and deletes the rows. A stale-job sweeper remains as a backstop.
- **Manual acks.** A worker acks only after the outcome is recorded in PostgreSQL. A worker that dies mid-job leaves
  the message unacked and the broker redelivers it.
- **Lease and reaper.** A claim gives the worker a lease (`scheduler.lease.duration`, default 30s) renewed every 10s
  while the job runs. The reaper takes back any `RUNNING` job whose lease expired, which covers workers that hang
  while their connection stays open (the broker would never redeliver those). The lease must be longer than the
  renew interval plus any pause you expect (GC, network blips).
- **Backoff and dead-letter routing.** One delay queue per attempt (`jobs.retry.1`, `jobs.retry.2`, ...) with a fixed
  TTL that dead-letters back onto the main exchange; a single queue with per-message TTLs would block short delays
  behind long ones. Exhausted and non-retryable jobs are rejected with `requeue=false` and land in `jobs.dlq` with
  `x-death` headers.

## Benchmarks

Setup: Docker Compose on one machine (Intel(R) Core(TM) Ultra 9 288V, 32 GB RAM), 2 consumers per worker, 1,000 jobs per run, each job a
simulated 100 ms task, submitted as a single burst. Throughput is jobs divided by the time from the first job's
creation to the last job's completion, from the server's own timestamps. Every run finished with 1000 succeeded,
0 dead, 0 lost.

| Workers | Throughput | Speedup vs 1 worker | Scaling efficiency |
|--------:|-----------:|--------------------:|-------------------:|
| 1 | 14.7 jobs/s | 1.0x | - |
| 3 | 42.6 jobs/s | 2.9x | 97% |
| 6 | 78.0 jobs/s | 5.3x | 88% |

With 100 ms tasks and 2 consumers per worker the theoretical ceiling is about 20 jobs/s per worker; the remainder is
per-job database and broker overhead. Submission itself ran at 306-357 jobs/s.

Reproduce (Python 3.8+, no dependencies; wait about 60 seconds after scaling up for new workers to boot):

```bash
docker compose up -d --scale worker=1
python scripts/loadtest.py --jobs 1000 --work-ms 100        # burst: measures throughput
docker compose up -d --scale worker=3 && python scripts/loadtest.py --jobs 1000 --work-ms 100
docker compose up -d --scale worker=6 && python scripts/loadtest.py --jobs 1000 --work-ms 100
```

Latency is only meaningful when the system keeps up with the arrival rate, so the script has a steady mode. Pick a
rate below the throughput above:

```bash
python scripts/loadtest.py --jobs 600 --work-ms 100 --rate 30   # steady 30 jobs/s: reports p50/p95/p99 latency
```

In burst mode the reported latency is mostly queue wait behind the burst and should not be quoted.

**Latency (steady-load run, separate from the throughput table above):** 600 jobs submitted at a steady 30 jobs/s to 6 workers, simulated 100 ms tasks. End-to-end latency is the time from the job's creation timestamp in the API to its completion timestamp recorded by the worker: p50 156 ms, p95 232 ms, p99 633 ms, 0 lost. Throughput equaled the arrival rate (29.9 jobs/s), so the system was keeping up and the figures are not inflated by queueing. The throughput table above is a different test: a single burst of 1,000 jobs at maximum rate, where latency is mostly queue wait.

## Observability

The API exposes Prometheus metrics at `http://localhost:8081/actuator/prometheus`:

| metric | meaning |
|---|---|
| `jobs_by_status{status}` | jobs in each lifecycle state (from PostgreSQL) |
| `jobs_queue_depth{queue}` | messages in the `main`, `retry` and `dead_letter` queues (from RabbitMQ) |
| `jobs_execution_seconds{outcome}` | execution time per attempt, recorded by workers |
| `jobs_retries_total`, `jobs_dead_total{reason}`, `jobs_stale_results_total` | retries scheduled, jobs dead-lettered, discarded stale results (workers) |

Worker-side metrics live in each worker's own registry; the compose file runs workers without a web server, so to
scrape them remove `SPRING_MAIN_WEBAPPLICATIONTYPE: none` from the worker service.

## Tests

```bash
mvn verify        # needs Docker: Testcontainers starts real PostgreSQL and RabbitMQ
```

48 tests: 45 integration tests (real PostgreSQL and RabbitMQ) and 3 unit tests.

| class | what it proves |
|---|---|
| `JobRecoveryIntegrationTest` (11) | retries, backoff, retry limit and DLQ, worker death and redelivery, poison jobs, duplicates, lost publish |
| `JobClaimConcurrencyIntegrationTest` (6) | racing workers: exactly one wins; stale reads cannot claim |
| `StaleWorkerFencingIntegrationTest` (2) | a replaced worker cannot overwrite results or reopen finished jobs |
| `LeaseReaperIntegrationTest` (3) | hung worker reclaimed; exhausted job dead-lettered; renewing worker keeps its job |
| `FencedEffectsIntegrationTest` (4) | effects applied once, and only by the current owner |
| `IdempotentExecutionIntegrationTest` (5) | effect ledger, including a job re-run after its effect |
| `OutboxIntegrationTest` (3) | job and outbox row commit or roll back together; outbox drains |
| `IdempotencyKeyIntegrationTest` (6) | repeat and concurrent submissions with one key create one job |
| `JobApiIntegrationTest` (4), `MetricsIntegrationTest` (1) | HTTP behaviour, validation, Prometheus endpoint |
| `BackoffPolicyTest` (3) | backoff math (unit tests) |

## Known limitations / next steps

- Handlers must be safe to re-run for anything outside `IdempotentEffects`.
- Changing `initial-delay`, `max-delay` or `max-attempts` changes retry-queue arguments, which RabbitMQ refuses to
  re-declare; delete the `jobs.retry.*` queues when tuning.
- No job cancellation, dead-letter replay endpoint, priorities, or scheduled (future) start times.
- No authentication on the REST API.
