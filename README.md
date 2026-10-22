# Distributed Job Scheduler

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

`JobApiIntegrationTest` covers submission (202), tracking, 404 and validation errors; `BackoffPolicyTest` covers the
delay math.

## Known limitations / next steps

- At-least-once, not exactly-once: handlers should be idempotent, since a crash after a handler's side effect but
  before the result is recorded causes a re-run.
- Changing `initial-delay`, `max-delay` or `max-attempts` changes retry-queue arguments, which RabbitMQ refuses to
  re-declare; delete the `jobs.retry.*` queues when tuning.
- No job cancellation, priorities, scheduled (future) start times, or idempotency keys on submission.
- No authentication on the REST API.
