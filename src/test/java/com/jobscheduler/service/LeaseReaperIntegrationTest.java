package com.jobscheduler.service;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jobscheduler.domain.Job;
import com.jobscheduler.domain.JobRepository;
import com.jobscheduler.domain.JobStatus;
import com.jobscheduler.messaging.Topology;
import com.jobscheduler.support.IntegrationTest;
import com.jobscheduler.support.TestHandlers;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * A worker that hangs (or is cut off) while its connection stays open never gets its message redelivered by the
 * broker. Its lease expires instead and the reaper takes the job back. Tests claim jobs directly as a "hung worker"
 * that never renews, so the real worker in the context only sees the job after the reaper requeues it. The test
 * context uses a 5s lease.
 */
@IntegrationTest
class LeaseReaperIntegrationTest {

    @Autowired JobService jobService;
    @Autowired JobRepository jobRepository;
    @Autowired RabbitTemplate rabbitTemplate;

    @Test
    void jobHeldByAWorkerThatStoppedRenewingIsReclaimedAndFinishedByAnotherWorker() {
        Job saved = jobRepository.save(new Job("counting", Map.of(), 3));
        Job hungView = jobService.claim(saved.getId(), false, "hung-worker").job();
        assertThat(hungView.getLeaseExpiresAt()).isNotNull();

        awaitStatus(saved.getId(), JobStatus.SUCCEEDED);

        Job done = jobRepository.findById(saved.getId()).orElseThrow();
        assertThat(done.getAttempts()).isEqualTo(2); // the hung attempt counts
        assertThat(done.getWorkerId()).isNotEqualTo("hung-worker");
        assertThat(TestHandlers.executions(saved.getId())).isEqualTo(1);

        // The hung worker wakes up and reports a result: rejected, nothing changes.
        assertThat(jobService.markSucceeded(hungView)).isFalse();
        assertThat(jobRepository.findById(saved.getId()).orElseThrow().getStatus()).isEqualTo(JobStatus.SUCCEEDED);
    }

    @Test
    void jobWithNoAttemptsLeftIsDeadLetteredWhenItsLeaseExpires() {
        Job saved = jobRepository.save(new Job("counting", Map.of(), 1));
        jobService.claim(saved.getId(), false, "hung-worker");

        awaitStatus(saved.getId(), JobStatus.DEAD);

        Job dead = jobRepository.findById(saved.getId()).orElseThrow();
        assertThat(dead.getAttempts()).isEqualTo(1);
        assertThat(dead.getLastError()).contains("Lease expired");
        assertThat(TestHandlers.executions(saved.getId())).isZero();
        awaitDeadLetter(saved.getId());
    }

    @Test
    void aWorkerThatKeepsRenewingKeepsItsJob() throws Exception {
        Job saved = jobRepository.save(new Job("counting", Map.of(), 3));
        Job owned = jobService.claim(saved.getId(), false, "slow-worker").job();

        // Renew every second for ~8s, well past the 5s lease: the reaper must leave the job alone.
        for (int i = 0; i < 8; i++) {
            Thread.sleep(1000);
            assertThat(jobService.renewLease(owned)).isTrue();
        }

        Job still = jobRepository.findById(saved.getId()).orElseThrow();
        assertThat(still.getStatus()).isEqualTo(JobStatus.RUNNING);
        assertThat(still.getWorkerId()).isEqualTo("slow-worker");
        assertThat(still.getAttempts()).isEqualTo(1);
    }

    private void awaitStatus(UUID id, JobStatus expected) {
        await().atMost(Duration.ofSeconds(40)).pollInterval(Duration.ofMillis(200)).untilAsserted(() ->
                assertThat(jobRepository.findById(id).orElseThrow().getStatus()).isEqualTo(expected));
    }

    private void awaitDeadLetter(UUID jobId) {
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            Message message = rabbitTemplate.receive(Topology.DEAD_LETTER_QUEUE, 500);
            return message != null && new String(message.getBody(), UTF_8).contains(jobId.toString());
        });
    }
}
