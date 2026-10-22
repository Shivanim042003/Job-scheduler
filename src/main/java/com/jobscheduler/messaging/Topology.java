package com.jobscheduler.messaging;

/**
 * RabbitMQ topology:
 *
 * <pre>
 *  producer --> jobs.exchange --(jobs.run)--> jobs.queue --> workers (manual ack)
 *                    ^                            |
 *                    |                            | basicReject(requeue=false)
 *   TTL expiry       |                            v
 *  jobs.retry.N -----+                       jobs.dlx --> jobs.dlq
 *  (one queue per attempt, fixed TTL = backoff for that attempt)
 * </pre>
 *
 * Each retry tier is its own queue with a uniform TTL, so messages expire in order. A single queue with
 * per-message TTLs would suffer head-of-line blocking: a short delay stuck behind a long one.
 */
public final class Topology {

    public static final String JOBS_EXCHANGE = "jobs.exchange";
    public static final String JOBS_QUEUE = "jobs.queue";
    public static final String JOBS_ROUTING_KEY = "jobs.run";

    public static final String DEAD_LETTER_EXCHANGE = "jobs.dlx";
    public static final String DEAD_LETTER_QUEUE = "jobs.dlq";
    public static final String DEAD_LETTER_ROUTING_KEY = "jobs.dead";

    private static final String RETRY_QUEUE_PREFIX = "jobs.retry.";

    private Topology() {
    }

    /** Delay queue used after the given (1-based) failed attempt. */
    public static String retryQueue(int failedAttempt) {
        return RETRY_QUEUE_PREFIX + failedAttempt;
    }
}
