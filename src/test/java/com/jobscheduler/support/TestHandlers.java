package com.jobscheduler.support;

import com.jobscheduler.service.IdempotentEffects;
import com.jobscheduler.worker.JobHandler;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/** Extra job types that let tests prove how many times a job body, and its side effect, really ran. */
@TestConfiguration(proxyBeanMethods = false)
public class TestHandlers {

    private static final Map<UUID, AtomicInteger> EXECUTIONS = new ConcurrentHashMap<>();
    private static final Map<UUID, AtomicInteger> INVOCATIONS = new ConcurrentHashMap<>();
    private static final Map<UUID, AtomicInteger> EFFECTS = new ConcurrentHashMap<>();

    /** Times the "counting" handler body ran. */
    public static int executions(UUID jobId) {
        return get(EXECUTIONS, jobId);
    }

    /** Times the "idempotent-counting" handler was invoked (including re-runs). */
    public static int invocations(UUID jobId) {
        return get(INVOCATIONS, jobId);
    }

    /** Times the "idempotent-counting" side effect was actually applied. */
    public static int effects(UUID jobId) {
        return get(EFFECTS, jobId);
    }

    @Bean
    JobHandler countingHandler() {
        return JobHandler.of("counting", job -> increment(EXECUTIONS, job.getId()));
    }

    @Bean
    JobHandler idempotentCountingHandler(IdempotentEffects effects) {
        return JobHandler.of("idempotent-counting", job -> {
            increment(INVOCATIONS, job.getId());
            var result = effects.runOnceAsOwner(job, "apply", () -> increment(EFFECTS, job.getId()));
            if (result == IdempotentEffects.EffectResult.NOT_OWNER) {
                throw new IllegalStateException("Lost ownership of job " + job.getId());
            }
        });
    }

    private static int get(Map<UUID, AtomicInteger> map, UUID jobId) {
        AtomicInteger count = map.get(jobId);
        return count == null ? 0 : count.get();
    }

    private static void increment(Map<UUID, AtomicInteger> map, UUID jobId) {
        map.computeIfAbsent(jobId, id -> new AtomicInteger()).incrementAndGet();
    }
}
