package com.jobscheduler.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.jobscheduler.config.SchedulerProperties;
import com.jobscheduler.messaging.BackoffPolicy;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class BackoffPolicyTest {

    private final BackoffPolicy policy = new BackoffPolicy(new SchedulerProperties(
            new SchedulerProperties.Retry(8, Duration.ofSeconds(2), Duration.ofSeconds(30)),
            new SchedulerProperties.Sweeper(5000, Duration.ofSeconds(30)),
            new SchedulerProperties.Lease(Duration.ofSeconds(30), Duration.ofSeconds(10), 5000)));

    @Test
    void delayDoublesWithEachFailedAttempt() {
        assertThat(policy.delayFor(1)).isEqualTo(Duration.ofSeconds(2));
        assertThat(policy.delayFor(2)).isEqualTo(Duration.ofSeconds(4));
        assertThat(policy.delayFor(3)).isEqualTo(Duration.ofSeconds(8));
        assertThat(policy.delayFor(4)).isEqualTo(Duration.ofSeconds(16));
    }

    @Test
    void delayIsCappedAtMaxDelay() {
        assertThat(policy.delayFor(5)).isEqualTo(Duration.ofSeconds(30));
        assertThat(policy.delayFor(100)).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void attemptsStartAtOne() {
        assertThatThrownBy(() -> policy.delayFor(0)).isInstanceOf(IllegalArgumentException.class);
    }
}
