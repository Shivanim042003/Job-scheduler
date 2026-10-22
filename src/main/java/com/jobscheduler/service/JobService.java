package com.jobscheduler.service;

import com.jobscheduler.config.SchedulerProperties;
import com.jobscheduler.domain.Job;
import com.jobscheduler.domain.JobRepository;
import com.jobscheduler.domain.JobStatus;
import com.jobscheduler.messaging.JobPublisher;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class JobService {

    private static final Logger log = LoggerFactory.getLogger(JobService.class);
    private static final int MAX_ERROR_LENGTH = 2000;
    private static final int MAX_IDEMPOTENCY_KEY_LENGTH = 200;

    private static final Set<JobStatus> CLAIMABLE = EnumSet.of(JobStatus.QUEUED, JobStatus.RETRYING);
    /** After a worker crash the job is still RUNNING in the database; a redelivery may take it over. */
    private static final Set<JobStatus> CLAIMABLE_ON_REDELIVERY =
            EnumSet.of(JobStatus.QUEUED, JobStatus.RETRYING, JobStatus.RUNNING);
    private static final Set<JobStatus> NOT_TERMINAL =
            EnumSet.of(JobStatus.QUEUED, JobStatus.RETRYING, JobStatus.RUNNING);

    /** @param created false when an earlier submission with the same idempotency key was returned instead */
    public record SubmitResult(Job job, boolean created) {
    }

    private final JobRepository repository;
    private final JobPublisher publisher;
    private final SchedulerProperties properties;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;

    public JobService(JobRepository repository, JobPublisher publisher, SchedulerProperties properties,
                      JdbcTemplate jdbc, PlatformTransactionManager transactionManager) {
        this.repository = repository;
        this.publisher = publisher;
        this.properties = properties;
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(transactionManager);
    }

    // ---------------------------------------------------------------- submission

    public Job submit(String type, Map<String, Object> payload, Integer maxAttempts) {
        return submit(type, payload, maxAttempts, null).job();
    }

    /**
     * Saves the job and an outbox row in ONE transaction. A relay publishes outbox rows to RabbitMQ afterwards
     * (see {@code OutboxRelay}), so a crash between "job saved" and "message published" cannot lose the job: either
     * both rows exist and will be relayed, or neither does.
     *
     * <p>With an idempotency key, a repeated submission returns the original job instead of creating another. The
     * payload of the repeat is ignored, as with any idempotency key.
     */
    public SubmitResult submit(String type, Map<String, Object> payload, Integer maxAttempts,
                               String idempotencyKey) {
        int limit = properties.retry().maxAttempts();
        int attempts = maxAttempts == null ? limit : maxAttempts;
        if (attempts < 1 || attempts > limit) {
            throw new IllegalArgumentException("maxAttempts must be between 1 and " + limit);
        }
        if (idempotencyKey != null
                && (idempotencyKey.isBlank() || idempotencyKey.length() > MAX_IDEMPOTENCY_KEY_LENGTH)) {
            throw new IllegalArgumentException(
                    "Idempotency-Key must be 1 to " + MAX_IDEMPOTENCY_KEY_LENGTH + " characters");
        }

        if (idempotencyKey != null) {
            Optional<Job> existing = repository.findByIdempotencyKey(idempotencyKey);
            if (existing.isPresent()) {
                return new SubmitResult(existing.get(), false);
            }
        }

        try {
            Job job = tx.execute(status -> {
                Job saved = repository.saveAndFlush(new Job(type, payload, attempts, idempotencyKey));
                jdbc.update("insert into outbox (job_id) values (?)", saved.getId());
                return saved;
            });
            return new SubmitResult(job, true);
        } catch (DataIntegrityViolationException e) {
            // Two requests with the same key raced and the other one committed first: return its job.
            if (idempotencyKey == null) {
                throw e;
            }
            return new SubmitResult(repository.findByIdempotencyKey(idempotencyKey).orElseThrow(() -> e), false);
        }
    }

    public Optional<Job> find(UUID id) {
        return repository.findById(id);
    }

    public List<Job> list(JobStatus status) {
        return status == null
                ? repository.findTop100ByOrderByCreatedAtDesc()
                : repository.findTop100ByStatusOrderByCreatedAtDesc(status);
    }

    // ---------------------------------------------------------------- worker side

    /**
     * Decides what a worker may do with a delivered message and, if it may run the job, claims it atomically.
     *
     * @param redelivered whether the broker flagged the delivery as a redelivery (previous consumer died or nacked)
     */
    @Transactional
    public ClaimResult claim(UUID jobId, boolean redelivered, String workerId) {
        Optional<Job> found = repository.findById(jobId);
        if (found.isEmpty()) {
            log.warn("Message for unknown job {}; dead-lettering", jobId);
            return ClaimResult.deadLetter();
        }
        Job job = found.get();

        switch (job.getStatus()) {
            case SUCCEEDED -> {
                return ClaimResult.skip();
            }
            // A redelivered DEAD job means we recorded DEAD but the reject never reached the broker:
            // reject again so the message actually lands in the DLQ.
            case DEAD -> {
                return redelivered ? ClaimResult.deadLetter() : ClaimResult.skip();
            }
            // RUNNING + first delivery: another worker legitimately owns it. RUNNING + redelivery: its owner died.
            case RUNNING -> {
                if (!redelivered) {
                    return ClaimResult.skip();
                }
            }
            default -> {
            }
        }

        // Every claim counts as an attempt, including ones that ended in a worker crash, so a job that keeps
        // killing its worker (a poison message) still hits the retry limit instead of looping forever.
        if (job.getAttempts() >= job.getMaxAttempts()) {
            repository.markDead(jobId, NOT_TERMINAL, JobStatus.DEAD,
                    "Retry limit reached (" + job.getMaxAttempts() + " attempts); last worker did not report a result",
                    Instant.now());
            return ClaimResult.deadLetter();
        }

        Set<JobStatus> from = redelivered ? CLAIMABLE_ON_REDELIVERY : CLAIMABLE;
        Instant now = Instant.now();
        // Compare-and-set on the attempt count we just read: a concurrent claimer that got in first makes this a no-op.
        int updated = repository.claim(jobId, from, JobStatus.RUNNING, now, workerId, job.getAttempts(),
                now.plus(properties.lease().duration()));
        if (updated == 0) {
            return ClaimResult.skip();
        }
        return ClaimResult.claimed(repository.findById(jobId).orElseThrow());
    }

    /** Heartbeat from the owning worker: keeps the lease alive while the job runs. */
    public boolean renewLease(Job job) {
        return repository.renewLease(job.getId(), JobStatus.RUNNING, job.getWorkerId(), job.getAttempts(),
                Instant.now().plus(properties.lease().duration())) == 1;
    }

    // Result writes are fenced: they apply only while this exact worker still owns this exact attempt. A worker
    // that was presumed dead and replaced (its job was taken over, or already finished) gets `false` and must
    // discard its result instead of overwriting the new owner's.

    public boolean markSucceeded(Job job) {
        return repository.markSucceeded(job.getId(), JobStatus.RUNNING, JobStatus.SUCCEEDED, Instant.now(),
                job.getWorkerId(), job.getAttempts()) == 1;
    }

    public boolean markRetrying(Job job, String error, Instant nextRunAt) {
        return repository.markRetrying(job.getId(), JobStatus.RUNNING, JobStatus.RETRYING, truncate(error),
                nextRunAt, Instant.now(), job.getWorkerId(), job.getAttempts()) == 1;
    }

    public boolean markDead(Job job, String error) {
        return repository.markDeadOwned(job.getId(), JobStatus.RUNNING, JobStatus.DEAD, truncate(error),
                Instant.now(), job.getWorkerId(), job.getAttempts()) == 1;
    }

    // ---------------------------------------------------------------- recovery

    /**
     * Takes back jobs whose owner stopped renewing its lease (a hung or partitioned worker whose connection is
     * still open, so the broker never redelivers). Jobs with attempts left go back on the queue; exhausted ones
     * are declared dead and sent to the dead-letter queue. Safe to run on several API instances at once: each
     * takeover is a compare-and-set on the attempt number.
     */
    public int reapExpiredLeases() {
        Instant now = Instant.now();
        int reaped = 0;
        for (Job job : repository.findTop100ByStatusAndLeaseExpiresAtBefore(JobStatus.RUNNING, now)) {
            String reason = "Lease expired: worker " + job.getWorkerId()
                    + " stopped renewing during attempt " + job.getAttempts();
            try {
                if (job.getAttempts() >= job.getMaxAttempts()) {
                    if (repository.killExpired(job.getId(), JobStatus.RUNNING, JobStatus.DEAD, reason, now,
                            job.getAttempts()) == 1) {
                        publisher.publishDeadLetter(job.getId());
                        reaped++;
                    }
                } else if (repository.requeueExpired(job.getId(), JobStatus.RUNNING, JobStatus.QUEUED, reason, now,
                        job.getAttempts()) == 1) {
                    publisher.publishNew(job.getId());
                    reaped++;
                }
            } catch (Exception e) {
                // A requeued job whose publish failed stays QUEUED; the stale-job sweeper republishes it.
                log.warn("Could not finish reaping job {}", job.getId(), e);
            }
        }
        return reaped;
    }

    /** Republishes jobs that are QUEUED but whose message went missing. Backstop behind the outbox. */
    public int republishStaleQueued(Instant cutoff) {
        List<Job> stale = repository.findTop100ByStatusAndUpdatedAtBefore(JobStatus.QUEUED, cutoff);
        int republished = 0;
        for (Job job : stale) {
            try {
                publisher.publishNew(job.getId());
                repository.touch(job.getId(), JobStatus.QUEUED, Instant.now());
                republished++;
            } catch (Exception e) {
                log.warn("Could not republish stale job {}", job.getId(), e);
            }
        }
        return republished;
    }

    private static String truncate(String error) {
        if (error == null) {
            return null;
        }
        return error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH);
    }
}
