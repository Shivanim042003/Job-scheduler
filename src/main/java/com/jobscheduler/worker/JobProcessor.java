package com.jobscheduler.worker;

import com.jobscheduler.domain.Job;
import com.jobscheduler.messaging.BackoffPolicy;
import com.jobscheduler.messaging.JobPublisher;
import com.jobscheduler.service.ClaimResult;
import com.jobscheduler.service.JobService;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
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
    private final String workerId =
            System.getenv().getOrDefault("HOSTNAME", "worker") + "-" + UUID.randomUUID().toString().substring(0, 8);

    public JobProcessor(JobService jobService, JobExecutor executor, JobPublisher publisher, BackoffPolicy backoff) {
        this.jobService = jobService;
        this.executor = executor;
        this.publisher = publisher;
        this.backoff = backoff;
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

        try {
            executor.execute(job);
        } catch (NonRetryableJobException e) {
            log.warn("Job {} failed permanently: {}", job.getId(), e.getMessage());
            jobService.markDead(job.getId(), "Non-retryable: " + e.getMessage());
            return Settlement.DEAD_LETTER;
        } catch (Exception e) {
            return handleFailure(job, e);
        }

        jobService.markSucceeded(job.getId());
        log.info("Job {} succeeded on attempt {}", job.getId(), job.getAttempts());
        return Settlement.ACK;
    }

    private Settlement handleFailure(Job job, Exception failure) {
        String error = failure.getClass().getSimpleName() + ": " + failure.getMessage();
        int attempt = job.getAttempts();

        if (attempt >= job.getMaxAttempts()) {
            log.error("Job {} exhausted {} attempts; dead-lettering", job.getId(), attempt, failure);
            jobService.markDead(job.getId(), "Retries exhausted after " + attempt + " attempts. Last error: " + error);
            return Settlement.DEAD_LETTER;
        }

        Duration delay = backoff.delayFor(attempt);
        log.warn("Job {} failed on attempt {}/{}; retrying in {}: {}",
                job.getId(), attempt, job.getMaxAttempts(), delay, error);
        jobService.markRetrying(job.getId(), error, Instant.now().plus(delay));
        // If this publish throws, the listener requeues the original message instead of acking it,
        // so the job is redelivered rather than lost.
        publisher.publishRetry(job.getId(), attempt);
        return Settlement.ACK;
    }
}
