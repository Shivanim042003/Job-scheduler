package com.jobscheduler.domain;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

/**
 * State transitions are conditional UPDATEs ("compare-and-set" on status), so two workers racing on the
 * same job can never both win, and a stale worker can never overwrite a terminal state.
 */
public interface JobRepository extends JpaRepository<Job, UUID> {

    List<Job> findTop100ByOrderByCreatedAtDesc();

    List<Job> findTop100ByStatusOrderByCreatedAtDesc(JobStatus status);

    List<Job> findTop100ByStatusAndUpdatedAtBefore(JobStatus status, Instant cutoff);

    /** Atomically moves a job into RUNNING and counts the attempt. Returns 0 if someone else got there first. */
    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update Job j
               set j.status = :to, j.attempts = j.attempts + 1, j.startedAt = :now,
                   j.updatedAt = :now, j.workerId = :worker, j.nextRunAt = null
             where j.id = :id and j.status in :from and j.attempts < j.maxAttempts
            """)
    int claim(@Param("id") UUID id, @Param("from") Collection<JobStatus> from, @Param("to") JobStatus to,
              @Param("now") Instant now, @Param("worker") String worker);

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update Job j
               set j.status = :to, j.finishedAt = :now, j.updatedAt = :now, j.lastError = null
             where j.id = :id and j.status = :from
            """)
    int markSucceeded(@Param("id") UUID id, @Param("from") JobStatus from, @Param("to") JobStatus to,
                      @Param("now") Instant now);

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update Job j
               set j.status = :to, j.lastError = :error, j.nextRunAt = :nextRunAt, j.updatedAt = :now
             where j.id = :id and j.status = :from
            """)
    int markRetrying(@Param("id") UUID id, @Param("from") JobStatus from, @Param("to") JobStatus to,
                     @Param("error") String error, @Param("nextRunAt") Instant nextRunAt,
                     @Param("now") Instant now);

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update Job j
               set j.status = :to, j.lastError = :error, j.finishedAt = :now, j.updatedAt = :now
             where j.id = :id and j.status in :from
            """)
    int markDead(@Param("id") UUID id, @Param("from") Collection<JobStatus> from, @Param("to") JobStatus to,
                 @Param("error") String error, @Param("now") Instant now);

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update Job j set j.updatedAt = :now where j.id = :id and j.status = :status")
    int touch(@Param("id") UUID id, @Param("status") JobStatus status, @Param("now") Instant now);
}
