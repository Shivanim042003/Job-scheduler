package com.jobscheduler.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.jobscheduler.domain.Job;
import com.jobscheduler.domain.JobRepository;
import com.jobscheduler.domain.JobStatus;
import com.jobscheduler.support.IntegrationTest;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** The job row and its outbox row are one atomic write, and the relay turns outbox rows into queued messages. */
@IntegrationTest
class OutboxIntegrationTest {

    @Autowired JobService jobService;
    @Autowired JobRepository jobRepository;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager transactionManager;

    @Test
    void submittedJobIsDeliveredThroughTheOutboxAndTheOutboxDrains() {
        Job job = jobService.submit("echo", Map.of(), null);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus())
                        .isEqualTo(JobStatus.SUCCEEDED));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(outboxRows(job.getId())).isZero());
    }

    @Test
    void jobAndOutboxRowCommitOrRollBackTogether() {
        AtomicReference<UUID> id = new AtomicReference<>();

        assertThatThrownBy(() -> new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            id.set(jobService.submit("echo", Map.of(), null).getId());
            throw new IllegalStateException("caller failed after submitting");
        })).isInstanceOf(IllegalStateException.class);

        // Neither the job nor a message for it exists: nothing half-written to recover from.
        assertThat(jobRepository.findById(id.get())).isEmpty();
        assertThat(outboxRows(id.get())).isZero();
    }

    @Test
    void relayPublishesAPendingRowAndDeletesIt() {
        UUID ghost = UUID.randomUUID();
        jdbc.update("insert into outbox (job_id) values (?)", ghost);

        // The scheduled relay may get there first; either way the row must disappear.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(outboxRows(ghost)).isZero());
    }

    private int outboxRows(UUID jobId) {
        Integer count = jdbc.queryForObject("select count(*) from outbox where job_id = ?", Integer.class, jobId);
        return count == null ? 0 : count;
    }
}
