package com.jobscheduler.domain;

public enum JobStatus {
    /** Persisted and published, waiting for a worker. */
    QUEUED,
    /** Claimed by a worker. */
    RUNNING,
    /** Last attempt failed; waiting in a delay queue for the next attempt. */
    RETRYING,
    /** Terminal: completed successfully. */
    SUCCEEDED,
    /** Terminal: retries exhausted or non-retryable failure; message routed to the dead-letter queue. */
    DEAD;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == DEAD;
    }
}
