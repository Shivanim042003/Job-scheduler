package com.jobscheduler.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "scheduler")
public record SchedulerProperties(@DefaultValue Retry retry, @DefaultValue Sweeper sweeper,
                                  @DefaultValue Lease lease) {

    /**
     * @param maxAttempts  total executions allowed per job (first run + retries); also the cap for per-job overrides
     * @param initialDelay delay before the second attempt; doubles with each further attempt
     * @param maxDelay     upper bound on the backoff delay
     */
    public record Retry(@DefaultValue("5") int maxAttempts,
                        @DefaultValue("2s") Duration initialDelay,
                        @DefaultValue("5m") Duration maxDelay) {
    }

    /**
     * @param intervalMillis how often the sweeper runs
     * @param staleAfter     a QUEUED job older than this is assumed to have lost its message and is republished
     */
    public record Sweeper(@DefaultValue("5000") long intervalMillis,
                          @DefaultValue("30s") Duration staleAfter) {
    }

    /**
     * @param duration          a RUNNING job whose owner has not renewed within this window is taken back
     * @param renewEvery        how often a worker renews while it runs a job (keep well below {@code duration})
     * @param reaperIntervalMillis how often the reaper looks for expired leases
     */
    public record Lease(@DefaultValue("30s") Duration duration,
                        @DefaultValue("10s") Duration renewEvery,
                        @DefaultValue("5000") long reaperIntervalMillis) {
    }
}
