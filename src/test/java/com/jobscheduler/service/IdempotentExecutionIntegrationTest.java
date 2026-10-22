package com.jobscheduler.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.jobscheduler.domain.Job;
import com.jobscheduler.domain.JobRepository;
import com.jobscheduler.domain.JobStatus;
import com.jobscheduler.messaging.JobPublisher;
import com.jobscheduler.support.IntegrationTest;
import com.jobscheduler.support.TestHandlers;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

@IntegrationTest
class IdempotentExecutionIntegrationTest {

    @Autowired IdempotentEffects effects;
    @Autowired JobService jobService;
    @Autowired JobRepository jobRepository;
    @Autowired JobPublisher publisher;
    @Autowired JdbcTemplate jdbc;

    @Test
    void sameEffectKeyRunsOnlyOnce() {
        UUID jobId = UUID.randomUUID();
        AtomicInteger runs = new AtomicInteger();

        boolean first = effects.runOnce(jobId, "charge", runs::incrementAndGet);
        boolean second = effects.runOnce(jobId, "charge", runs::incrementAndGet);

        assertThat(first).isTrue();
        assertThat(second).isFalse();
        assertThat(runs).hasValue(1);
    }

    @Test
    void differentKeysAreIndependent() {
        UUID jobId = UUID.randomUUID();
        AtomicInteger runs = new AtomicInteger();

        effects.runOnce(jobId, "charge", runs::incrementAndGet);
        effects.runOnce(jobId, "email", runs::incrementAndGet);

        assertThat(runs).hasValue(2);
    }

    @Test
    void concurrentCallersWithTheSameKeyApplyTheEffectOnce() throws Exception {
        UUID jobId = UUID.randomUUID();
        AtomicInteger runs = new AtomicInteger();
        int callers = 8;

        ExecutorService pool = Executors.newFixedThreadPool(callers);
        try {
            CountDownLatch ready = new CountDownLatch(callers);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<Boolean>> futures = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    return effects.runOnce(jobId, "charge", runs::incrementAndGet);
                }));
            }
            ready.await();
            go.countDown();
            int applied = 0;
            for (Future<Boolean> future : futures) {
                if (future.get(30, TimeUnit.SECONDS)) {
                    applied++;
                }
            }
            assertThat(applied).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
        assertThat(runs).hasValue(1);
    }

    @Test
    void aFailedEffectDoesNotConsumeTheKeySoARetryCanApplyIt() {
        UUID jobId = UUID.randomUUID();
        AtomicInteger runs = new AtomicInteger();

        assertThatThrownBy(() -> effects.runOnce(jobId, "charge", () -> {
            throw new IllegalStateException("downstream unavailable");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(effects.runOnce(jobId, "charge", runs::incrementAndGet)).isTrue();
        assertThat(runs).hasValue(1);
    }

    /**
     * Simulates the nasty window: the effect was applied, then the worker died before recording the result, so the
     * job runs again. The handler is invoked twice but the effect is applied once.
     */
    @Test
    void jobReexecutedAfterItsEffectWasAppliedDoesNotRepeatTheEffect() {
        Job job = jobService.submit("idempotent-counting", Map.of(), null);
        UUID id = job.getId();
        awaitStatus(id, JobStatus.SUCCEEDED);
        assertThat(TestHandlers.invocations(id)).isEqualTo(1);
        assertThat(TestHandlers.effects(id)).isEqualTo(1);

        // Rewind the job as if the result had never been recorded, then deliver it again.
        jdbc.update("update jobs set status = 'QUEUED', attempts = 0, finished_at = null where id = ?", id);
        publisher.publishNew(id);

        await().atMost(Duration.ofSeconds(30)).until(() -> TestHandlers.invocations(id) == 2);
        awaitStatus(id, JobStatus.SUCCEEDED);
        assertThat(TestHandlers.effects(id)).isEqualTo(1);
    }

    private void awaitStatus(UUID id, JobStatus expected) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100)).untilAsserted(() ->
                assertThat(jobRepository.findById(id).orElseThrow().getStatus()).isEqualTo(expected));
    }
}
