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
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
public class JobService {

    private static final Logger log = LoggerFactory.getLogger(JobService.class);
    private static final int MAX_ERROR_LENGTH = 2000;

    private static final Set<JobStatus> CLAIMABLE = EnumSet.of(JobStatus.QUEUED, JobStatus.RETRYING);
    /** After a worker crash the job is still RUNNING in the database; a redelivery may take it over. */
    private static final Set<JobStatus> CLAIMABLE_ON_REDELIVERY =
            EnumSet.of(JobStatus.QUEUED, JobStatus.RETRYING, JobStatus.RUNNING);
    private static final Set<JobStatus> NOT_TERMINAL =
            EnumSet.of(JobStatus.QUEUED, JobStatus.RETRYING, JobStatus.RUNNING);

    private final JobRepository repository;
    private final JobPublisher publisher;
    private final SchedulerProperties properties;

    public JobService(JobRepository repository, JobPublisher publisher, SchedulerProperties properties) {
        this.repository = repository;
        this.publisher = publisher;
        this.properties = properties;
    }

    // ---------------------------------------------------------------- submission

    /**
     * Persists the job, then publishes it once the transaction has committed. Publishing after commit means a
     * worker can never receive a message for a row it cannot see. If the publish itself fails, the job stays
     * QUEUED and {@link #republishStaleQueued(Instant)} picks it up.
     */
    @Transactional
    public Job submit(String type, Map<String, Object> payload, Integer maxAttempts) {
        int limit = properties.retry().maxAttempts();
        int attempts = maxAttempts == null ? limit : maxAttempts;
        if (attempts < 1 || attempts > limit) {
            throw new IllegalArgumentException("maxAttempts must be between 1 and " + limit);
        }

        Job job = repository.save(new Job(type, payload, attempts));
        UUID id = job.getId();

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try {
                    publisher.publishNew(id);
                } catch (Exception e) {
                    log.warn("Publish failed for job {}; the sweeper will retry it", id, e);
                }
            }
        });
        return job;
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
        int updated = repository.claim(jobId, from, JobStatus.RUNNING, Instant.now(), workerId);
        if (updated == 0) {
            return ClaimResult.skip();
        }
        return ClaimResult.claimed(repository.findById(jobId).orElseThrow());
    }

    public void markSucceeded(UUID jobId) {
        repository.markSucceeded(jobId, JobStatus.RUNNING, JobStatus.SUCCEEDED, Instant.now());
    }

    public void markRetrying(UUID jobId, String error, Instant nextRunAt) {
        repository.markRetrying(jobId, JobStatus.RUNNING, JobStatus.RETRYING, truncate(error), nextRunAt, Instant.now());
    }

    public void markDead(UUID jobId, String error) {
        repository.markDead(jobId, NOT_TERMINAL, JobStatus.DEAD, truncate(error), Instant.now());
    }

    // ---------------------------------------------------------------- recovery

    /** Republishes jobs that were saved but whose message never made it to the broker. */
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
