-- Lease: a RUNNING job is owned only while its worker keeps renewing this timestamp.
ALTER TABLE jobs ADD COLUMN lease_expires_at TIMESTAMPTZ;
CREATE INDEX idx_jobs_running_lease ON jobs (lease_expires_at) WHERE status = 'RUNNING';

-- Idempotency key: a client retrying a submission gets the original job back instead of a duplicate.
ALTER TABLE jobs ADD COLUMN idempotency_key VARCHAR(200);
CREATE UNIQUE INDEX uq_jobs_idempotency_key ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;

-- Transactional outbox: written in the same transaction as the job, drained by a relay that publishes to RabbitMQ.
CREATE TABLE outbox (
    id         BIGSERIAL PRIMARY KEY,
    job_id     UUID        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
