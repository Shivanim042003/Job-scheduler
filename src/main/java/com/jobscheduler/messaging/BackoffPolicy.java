package com.jobscheduler.messaging;

import com.jobscheduler.config.SchedulerProperties;
import java.time.Duration;
import org.springframework.stereotype.Component;

/** Exponential backoff: initialDelay * 2^(attempt-1), capped at maxDelay. */
@Component
public class BackoffPolicy {

    private final Duration initialDelay;
    private final Duration maxDelay;

    public BackoffPolicy(SchedulerProperties properties) {
        this.initialDelay = properties.retry().initialDelay();
        this.maxDelay = properties.retry().maxDelay();
    }

    /** Delay to wait after the given 1-based failed attempt before running the next one. */
    public Duration delayFor(int failedAttempt) {
        if (failedAttempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
        }
        int shift = Math.min(failedAttempt - 1, 30);
        long millis = initialDelay.toMillis() * (1L << shift);
        return Duration.ofMillis(Math.min(millis, maxDelay.toMillis()));
    }
}
