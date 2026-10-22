package com.jobscheduler.api;

import com.jobscheduler.domain.Job;
import com.jobscheduler.domain.JobStatus;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record JobResponse(
        UUID id,
        String type,
        Map<String, Object> payload,
        JobStatus status,
        int attempts,
        int maxAttempts,
        String lastError,
        String workerId,
        Instant createdAt,
        Instant updatedAt,
        Instant startedAt,
        Instant finishedAt,
        Instant nextRunAt) {

    public static JobResponse from(Job job) {
        return new JobResponse(job.getId(), job.getType(), job.getPayload(), job.getStatus(), job.getAttempts(),
                job.getMaxAttempts(), job.getLastError(), job.getWorkerId(), job.getCreatedAt(),
                job.getUpdatedAt(), job.getStartedAt(), job.getFinishedAt(), job.getNextRunAt());
    }
}
