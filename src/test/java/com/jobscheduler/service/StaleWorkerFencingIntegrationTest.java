package com.jobscheduler.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.jobscheduler.domain.Job;
import com.jobscheduler.domain.JobRepository;
import com.jobscheduler.domain.JobStatus;
import com.jobscheduler.support.IntegrationTest;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Worker A claims a job and is presumed dead; worker B takes it over. If A turns out to be alive (a long GC pause,
 * a network partition) and reports a result late, it must not overwrite B's run. The job is saved directly, with no
 * message published, so the live worker in the context never competes with these two.
 */
@IntegrationTest
class StaleWorkerFencingIntegrationTest {

    @Autowired JobService jobService;
    @Autowired JobRepository jobRepository;

    @Test
    void staleWorkerCannotOverwriteTheWorkerThatTookOver() {
        Job saved = jobRepository.save(new Job("echo", Map.of(), 3));
        Job staleView = jobService.claim(saved.getId(), false, "worker-a").job();
        Job takenOver = jobService.claim(saved.getId(), true, "worker-b").job();
        assertThat(staleView.getAttempts()).isEqualTo(1);
        assertThat(takenOver.getAttempts()).isEqualTo(2);

        // Every kind of late result from the stale owner is rejected...
        assertThat(jobService.markSucceeded(staleView)).isFalse();
        assertThat(jobService.markRetrying(staleView, "late failure", Instant.now())).isFalse();
        assertThat(jobService.markDead(staleView, "late failure")).isFalse();

        // ...and the new owner's job is untouched.
        Job during = jobRepository.findById(saved.getId()).orElseThrow();
        assertThat(during.getStatus()).isEqualTo(JobStatus.RUNNING);
        assertThat(during.getWorkerId()).isEqualTo("worker-b");
        assertThat(during.getAttempts()).isEqualTo(2);

        // The current owner can still finish it.
        assertThat(jobService.markSucceeded(takenOver)).isTrue();
        assertThat(jobRepository.findById(saved.getId()).orElseThrow().getStatus()).isEqualTo(JobStatus.SUCCEEDED);
    }

    @Test
    void staleWorkerCannotReopenOrKillAFinishedJob() {
        Job saved = jobRepository.save(new Job("echo", Map.of(), 3));
        Job claimed = jobService.claim(saved.getId(), false, "worker-a").job();
        assertThat(jobService.markSucceeded(claimed)).isTrue();

        // The same worker re-reporting (or a duplicate result) cannot change a terminal state.
        assertThat(jobService.markDead(claimed, "late failure")).isFalse();
        assertThat(jobService.markRetrying(claimed, "late failure", Instant.now())).isFalse();
        assertThat(jobRepository.findById(saved.getId()).orElseThrow().getStatus()).isEqualTo(JobStatus.SUCCEEDED);
    }
}
