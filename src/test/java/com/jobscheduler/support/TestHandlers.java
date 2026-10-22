package com.jobscheduler.support;

import com.jobscheduler.worker.JobHandler;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/** Adds a "counting" job type so tests can prove how many times a job body really executed. */
@TestConfiguration(proxyBeanMethods = false)
public class TestHandlers {

    private static final Map<UUID, AtomicInteger> EXECUTIONS = new ConcurrentHashMap<>();

    public static int executions(UUID jobId) {
        AtomicInteger count = EXECUTIONS.get(jobId);
        return count == null ? 0 : count.get();
    }

    @Bean
    JobHandler countingHandler() {
        return JobHandler.of("counting",
                job -> EXECUTIONS.computeIfAbsent(job.getId(), id -> new AtomicInteger()).incrementAndGet());
    }
}
