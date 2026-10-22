package com.jobscheduler.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Demo job types, handy for exercising the retry and dead-letter paths by hand:
 * <ul>
 *   <li><b>echo</b> - logs and succeeds</li>
 *   <li><b>sleep</b> - sleeps {@code millis} (default 1000, max 60000), then succeeds</li>
 *   <li><b>fail</b> - always throws, so it exhausts its retries and is dead-lettered</li>
 *   <li><b>flaky</b> - fails while {@code attempts <= failUntilAttempt} (default 2), then succeeds</li>
 * </ul>
 */
@Configuration
@Profile("worker")
public class BuiltInHandlers {

    private static final Logger log = LoggerFactory.getLogger(BuiltInHandlers.class);

    @Bean
    JobHandler echoHandler() {
        return JobHandler.of("echo", job -> log.info("echo job {}: {}", job.getId(), job.getPayload()));
    }

    @Bean
    JobHandler sleepHandler() {
        return JobHandler.of("sleep", job -> {
            long millis = Math.min(number(job, "millis", 1000), 60_000);
            Thread.sleep(millis);
        });
    }

    @Bean
    JobHandler failHandler() {
        return JobHandler.of("fail", job -> {
            throw new IllegalStateException("Job failed deliberately (attempt " + job.getAttempts() + ")");
        });
    }

    @Bean
    JobHandler flakyHandler() {
        return JobHandler.of("flaky", job -> {
            long failUntil = number(job, "failUntilAttempt", 2);
            if (job.getAttempts() <= failUntil) {
                throw new IllegalStateException("Simulated transient failure on attempt " + job.getAttempts());
            }
        });
    }

    private static long number(com.jobscheduler.domain.Job job, String key, long defaultValue) {
        Object value = job.getPayload().get(key);
        return value instanceof Number n ? n.longValue() : defaultValue;
    }
}
