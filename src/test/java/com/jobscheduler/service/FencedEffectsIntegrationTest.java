package com.jobscheduler.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.jobscheduler.domain.Job;
import com.jobscheduler.domain.JobRepository;
import com.jobscheduler.service.IdempotentEffects.EffectResult;
import com.jobscheduler.support.IntegrationTest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Side effects are applied at most once per job and key, and only by the worker that currently owns the job. Jobs
 * are saved directly (no message published), so the live worker in the context never competes with the test.
 */
@IntegrationTest
class FencedEffectsIntegrationTest {

    @Autowired JobService jobService;
    @Autowired JobRepository jobRepository;
    @Autowired IdempotentEffects effects;

    @Test
    void currentOwnerAppliesAnEffectOnceAndRepeatsAreSkipped() {
        Job owned = claimNewJob("worker-a", false);
        AtomicInteger applied = new AtomicInteger();

        EffectResult first = effects.runOnceAsOwner(owned, "charge", applied::incrementAndGet);
        EffectResult second = effects.runOnceAsOwner(owned, "charge", applied::incrementAndGet);

        assertThat(first).isEqualTo(EffectResult.APPLIED);
        assertThat(second).isEqualTo(EffectResult.ALREADY_APPLIED);
        assertThat(applied).hasValue(1);
    }

    @Test
    void workerReplacedByATakeoverCanNoLongerApplyEffects() {
        Job saved = jobRepository.save(new Job("echo", Map.of(), 3));
        Job zombie = jobService.claim(saved.getId(), false, "worker-a").job();      // attempt 1
        Job newOwner = jobService.claim(saved.getId(), true, "worker-b").job();     // attempt 2: takeover
        AtomicInteger applied = new AtomicInteger();

        EffectResult fromZombie = effects.runOnceAsOwner(zombie, "charge", applied::incrementAndGet);
        EffectResult fromOwner = effects.runOnceAsOwner(newOwner, "charge", applied::incrementAndGet);

        assertThat(fromZombie).isEqualTo(EffectResult.NOT_OWNER);
        assertThat(fromOwner).isEqualTo(EffectResult.APPLIED);
        assertThat(applied).hasValue(1);
    }

    @Test
    void effectAppliedBeforeAWorkerWasReplacedIsNotRepeatedByTheNewOwner() {
        Job saved = jobRepository.save(new Job("echo", Map.of(), 3));
        Job first = jobService.claim(saved.getId(), false, "worker-a").job();
        AtomicInteger applied = new AtomicInteger();
        // Worker A applies the effect, then dies before recording its result.
        assertThat(effects.runOnceAsOwner(first, "charge", applied::incrementAndGet)).isEqualTo(EffectResult.APPLIED);

        Job takeover = jobService.claim(saved.getId(), true, "worker-b").job();
        EffectResult retry = effects.runOnceAsOwner(takeover, "charge", applied::incrementAndGet);

        assertThat(retry).isEqualTo(EffectResult.ALREADY_APPLIED);
        assertThat(applied).hasValue(1);
    }

    @Test
    void concurrentCallsFromTheOwnerApplyTheEffectOnce() throws Exception {
        Job owned = claimNewJob("worker-a", false);
        AtomicInteger applied = new AtomicInteger();
        int callers = 8;

        ExecutorService pool = Executors.newFixedThreadPool(callers);
        try {
            CountDownLatch ready = new CountDownLatch(callers);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<EffectResult>> futures = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    return effects.runOnceAsOwner(owned, "charge", applied::incrementAndGet);
                }));
            }
            ready.await();
            go.countDown();

            int appliedResults = 0;
            for (Future<EffectResult> future : futures) {
                if (future.get(30, TimeUnit.SECONDS) == EffectResult.APPLIED) {
                    appliedResults++;
                }
            }
            assertThat(appliedResults).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
        assertThat(applied).hasValue(1);
    }

    private Job claimNewJob(String worker, boolean redelivered) {
        Job saved = jobRepository.save(new Job("echo", Map.of(), 3));
        return jobService.claim(saved.getId(), redelivered, worker).job();
    }
}
