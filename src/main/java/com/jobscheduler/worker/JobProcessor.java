package com.jobscheduler.worker;

import com.jobscheduler.config.SchedulerProperties;
import com.jobscheduler.domain.Job;
import com.jobscheduler.messaging.BackoffPolicy;
import com.jobscheduler.messaging.JobPublisher;
import com.jobscheduler.service.ClaimResult;
import com.jobscheduler.service.JobService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Executes one delivery and decides how the message must be settled. It never touches the AMQP channel; the
 * listener turns the returned {@link Settlement} into an ack or a reject, so every path is easy to test.
 */
@Component
@Profile("worker")
public class JobProcessor {

    public enum Settlement {
        /** basicAck: the message is done (job finished, retry scheduled, or duplicate ignored). */
        ACK,
        /** basicReject(requeue=false): the broker routes the message to the dead-letter queue. */
        DEAD_LETTER
    }

    private static final Logger log = LoggerFactory.getLogger(JobProcessor.class);

    private final JobService jobService;
    private final JobExecutor executor;
    private final JobPublisher publisher;
    private final BackoffPolicy backoff;
    private final MeterRegistry meterRegistry;
    private final long renewMillis;
    private final String workerId =
            System.getenv().getOrDefault("HOSTNAME", "worker") + "-" + UUID.randomUUID().toString().substring(0, 8);

    /** One daemon thread renews the leases of every job this worker is running. If the JVM hangs, renewals stop. */
    private final ScheduledExecutorService heartbeats = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "lease-heartbeat");
        thread.setDaemon(true);
        return thread;
    });

    public JobProcessor(JobService jobService, JobExecutor executor, JobPublisher publisher, BackoffPolicy backoff,
                        MeterRegistry meterRegistry, SchedulerProperties properties) {
        this.jobService = jobService;
        this.executor = executor;
        this.publisher = publisher;
        this.backoff = backoff;
        this.meterRegistry = meterRegistry;
        this.renewMillis = properties.lease().renewEvery().toMillis();
    }

    /**
     * @throws RuntimeException on infrastructure failure (database or broker unavailable). The listener then
     *                          requeues the message so nothing is lost.
     */
    public Settlement process(UUID jobId, boolean redelivered) {
        ClaimResult claim = jobService.claim(jobId, redelivered, workerId);
        switch (claim.outcome()) {
            case SKIP -> {
                log.debug("Skipping delivery for job {}", jobId);
                return Settlement.ACK;
            }
            case DEAD_LETTER -> {
                return Settlement.DEAD_LETTER;
            }
            default -> {
            }
        }

        Job job = claim.job();
        log.info("Running job {} type={} attempt={}/{} redelivered={}",
                job.getId(), job.getType(), job.getAttempts(), job.getMaxAttempts(), redelivered);

        Timer.Sample sample = Timer.start(meterRegistry);
        ScheduledFuture<?> heartbeat = heartbeats.scheduleWithFixedDelay(
                () -> renewLease(job), renewMillis, renewMillis, TimeUnit.MILLISECONDS);
        try {
            executor.execute(job);
        } catch (NonRetryableJobException e) {
            stop(sample, "non_retryable");
            log.warn("Job {} failed permanently: {}", job.getId(), e.getMessage());
            if (!jobService.markDead(job, "Non-retryable: " + e.getMessage())) {
                return staleOwner(job);
            }
            meterRegistry.counter("jobs.dead", "reason", "non_retryable").increment();
            return Settlement.DEAD_LETTER;
        } catch (Exception e) {
            stop(sample, "failed");
            return handleFailure(job, e);
        } finally {
            heartbeat.cancel(false);
        }

        stop(sample, "succeeded");
        if (!jobService.markSucceeded(job)) {
            return staleOwner(job);
        }
        log.info("Job {} succeeded on attempt {}", job.getId(), job.getAttempts());
        return Settlement.ACK;
    }

    private void renewLease(Job job) {
        try {
            if (!jobService.renewLease(job)) {
                log.warn("Lease for job {} attempt {} could not be renewed; it was reclaimed or finished",
                        job.getId(), job.getAttempts());
            }
        } catch (Exception e) {
            // Must not throw: an exception would cancel all future renewals for every running job.
            log.warn("Lease renewal failed for job {}: {}", job.getId(), e.toString());
        }
    }

    private Settlement handleFailure(Job job, Exception failure) {
        String error = failure.getClass().getSimpleName() + ": " + failure.getMessage();
        int attempt = job.getAttempts();

        if (attempt >= job.getMaxAttempts()) {
            log.error("Job {} exhausted {} attempts; dead-lettering", job.getId(), attempt, failure);
            if (!jobService.markDead(job, "Retries exhausted after " + attempt + " attempts. Last error: " + error)) {
                return staleOwner(job);
            }
            meterRegistry.counter("jobs.dead", "reason", "retries_exhausted").increment();
            return Settlement.DEAD_LETTER;
        }

        Duration delay = backoff.delayFor(attempt);
        log.warn("Job {} failed on attempt {}/{}; retrying in {}: {}",
                job.getId(), attempt, job.getMaxAttempts(), delay, error);
        if (!jobService.markRetrying(job, error, Instant.now().plus(delay))) {
            return staleOwner(job);
        }
        meterRegistry.counter("jobs.retries").increment();
        // If this publish throws, the listener requeues the original message instead of acking it,
        // so the job is redelivered rather than lost.
        publisher.publishRetry(job.getId(), attempt);
        return Settlement.ACK;
    }

    /** Another worker took this job over (or it already finished): drop this result and schedule nothing. */
    private Settlement staleOwner(Job job) {
        log.warn("Job {} attempt {} is no longer owned by this worker; discarding its result",
                job.getId(), job.getAttempts());
        meterRegistry.counter("jobs.stale.results").increment();
        return Settlement.ACK;
    }

    private void stop(Timer.Sample sample, String outcome) {
        sample.stop(meterRegistry.timer("jobs.execution", "outcome", outcome));
    }
}
