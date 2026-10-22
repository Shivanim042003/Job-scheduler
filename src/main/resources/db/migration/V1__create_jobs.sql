CREATE TABLE jobs (
    id           UUID PRIMARY KEY,
    type         VARCHAR(100) NOT NULL,
    payload      JSONB        NOT NULL DEFAULT '{}'::jsonb,
    status       VARCHAR(20)  NOT NULL,
    attempts     INT          NOT NULL DEFAULT 0,
    max_attempts INT          NOT NULL,
    last_error   TEXT,
    worker_id    VARCHAR(100),
    created_at   TIMESTAMPTZ  NOT NULL,
    updated_at   TIMESTAMPTZ  NOT NULL,
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    next_run_at  TIMESTAMPTZ,
    CONSTRAINT chk_jobs_attempts CHECK (attempts >= 0 AND attempts <= max_attempts)
);

CREATE INDEX idx_jobs_status_updated ON jobs (status, updated_at);
CREATE INDEX idx_jobs_created_at     ON jobs (created_at DESC);
