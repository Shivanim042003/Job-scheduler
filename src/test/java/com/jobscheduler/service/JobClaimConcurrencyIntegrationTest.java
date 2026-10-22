package com.jobscheduler.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.jobscheduler.domain.Job;
import com.jobscheduler.domain.JobRepository;
import com.jobscheduler.domain.JobStatus;
import com.jobscheduler.support.IntegrationTest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Many workers receive a message for the same job at the same instant. Exactly one may win. The job is saved
 * directly (no message is published), so the live worker in the context never competes with the racers.
 */
@IntegrationTest
class JobClaimConcurrencyIntegrationTest {

    private static final int RACERS = 8;

    @Autowired JobService jobService;
    @Autowired JobRepository jobRepository;

    @RepeatedTest(5)
    void exactlyOneOfManyConcurrentWorkersClaimsAQueuedJob() throws Exception {
        Job job = jobRepository.save(new Job("echo", Map.of(), 3));

        List<ClaimResult.Outcome> outcomes = race(job.getId());

        assertThat(outcomes.stream().filter(o -> o == ClaimResult.Outcome.CLAIMED)).hasSize(1);
        assertThat(outcomes.stream().filter(o -> o == ClaimResult.Outcome.SKIP)).hasSize(RACERS - 1);
        Job after = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(after.getStatus()).isEqualTo(JobStatus.RUNNING);
        assertThat(after.getAttempts()).isEqualTo(1); // one claim, one counted attempt
    }

    @Test
    void aWorkerActingOnAStaleViewCannotClaimAJobAnotherWorkerAlreadyTook() {
        Job job = jobRepository.save(new Job("echo", Map.of(), 3));
        var claimable = EnumSet.of(JobStatus.QUEUED, JobStatus.RETRYING, JobStatus.RUNNING);

        // Two workers both read the job at attempts=0 and both try to take over (e.g. two redelivered messages).
        int first = jobRepository.claim(job.getId(), claimable, JobStatus.RUNNING, Instant.now(), "worker-a", 0);
        int second = jobRepository.claim(job.getId(), claimable, JobStatus.RUNNING, Instant.now(), "worker-b", 0);

        assertThat(first).isEqualTo(1);
        assertThat(second).isZero();
        Job after = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(after.getAttempts()).isEqualTo(1);
        assertThat(after.getWorkerId()).isEqualTo("worker-a");
    }

    private List<ClaimResult.Outcome> race(UUID jobId) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(RACERS);
        try {
            CountDownLatch ready = new CountDownLatch(RACERS);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<ClaimResult.Outcome>> futures = new ArrayList<>();
            for (int i = 0; i < RACERS; i++) {
                String worker = "racer-" + i;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    return jobService.claim(jobId, false, worker).outcome();
                }));
            }
            ready.await();
            go.countDown();
            List<ClaimResult.Outcome> outcomes = new ArrayList<>();
            for (Future<ClaimResult.Outcome> future : futures) {
                outcomes.add(future.get(30, TimeUnit.SECONDS));
            }
            return outcomes;
        } finally {
            pool.shutdownNow();
        }
    }
}
