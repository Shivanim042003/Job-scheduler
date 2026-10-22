package com.jobscheduler.worker;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jobscheduler.domain.Job;
import com.jobscheduler.domain.JobRepository;
import com.jobscheduler.domain.JobStatus;
import com.jobscheduler.messaging.JobPublisher;
import com.jobscheduler.messaging.Topology;
import com.jobscheduler.service.JobService;
import com.jobscheduler.support.IntegrationTest;
import com.jobscheduler.support.TestHandlers;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * End-to-end tests against real PostgreSQL and RabbitMQ: retries, backoff, dead-lettering, worker crashes,
 * message redelivery and terminal states.
 */
@IntegrationTest
class JobRecoveryIntegrationTest {

    @Autowired JobService jobService;
    @Autowired JobRepository jobRepository;
    @Autowired JobPublisher publisher;
    @Autowired RabbitTemplate rabbitTemplate;
    @Autowired AmqpAdmin amqpAdmin;
    @Autowired CachingConnectionFactory connectionFactory;
    @Autowired RabbitListenerEndpointRegistry registry;
    @Autowired JdbcTemplate jdbc;

    @AfterEach
    void makeSureWorkerIsRunning() {
        MessageListenerContainer worker = registry.getListenerContainer("jobWorker");
        if (!worker.isRunning()) {
            worker.start();
        }
    }

    // ------------------------------------------------------------------ happy path

    @Test
    void successfulJobReachesSucceededAfterOneAttempt() {
        Job job = jobService.submit("echo", Map.of("msg", "hello"), null);

        Job done = awaitStatus(job.getId(), JobStatus.SUCCEEDED);

        assertThat(done.getAttempts()).isEqualTo(1);
        assertThat(done.getFinishedAt()).isNotNull();
        assertThat(done.getLastError()).isNull();
        awaitQueueDrained(Topology.JOBS_QUEUE);
    }

    // ------------------------------------------------------------------ retries and backoff

    @Test
    void transientFailuresAreRetriedWithBackoffUntilTheJobSucceeds() {
        // Fails on attempts 1 and 2, succeeds on attempt 3 (the limit in this test context).
        Job job = jobService.submit("flaky", Map.of("failUntilAttempt", 2), null);

        long start = System.nanoTime();
        Job done = awaitStatus(job.getId(), JobStatus.SUCCEEDED);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(done.getAttempts()).isEqualTo(3);
        // Backoff before attempts 2 and 3 is 200ms + 400ms; the job cannot finish faster than that.
        assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(550));
    }

    @Test
    void jobThatKeepsFailingStopsAtTheRetryLimitAndIsDeadLettered() {
        Job job = jobService.submit("fail", Map.of(), null);

        Job dead = awaitStatus(job.getId(), JobStatus.DEAD);

        assertThat(dead.getAttempts()).isEqualTo(3);
        assertThat(dead.getLastError()).contains("Retries exhausted after 3 attempts");

        Message deadLetter = awaitDeadLetter(job.getId());
        List<Map<String, ?>> xDeath = deadLetter.getMessageProperties().getXDeathHeader();
        assertThat(xDeath).isNotEmpty();
        assertThat(String.valueOf(xDeath.get(0).get("reason"))).isEqualTo("rejected");
        awaitQueueDrained(Topology.JOBS_QUEUE);
    }

    @Test
    void perJobMaxAttemptsIsHonoured() {
        Job job = jobService.submit("fail", Map.of(), 1);

        Job dead = awaitStatus(job.getId(), JobStatus.DEAD);

        assertThat(dead.getAttempts()).isEqualTo(1);
        awaitDeadLetter(job.getId());
    }

    @Test
    void nonRetryableFailureIsDeadLetteredImmediatelyWithoutRetries() {
        Job job = jobService.submit("no-such-job-type", Map.of(), null);

        Job dead = awaitStatus(job.getId(), JobStatus.DEAD);

        assertThat(dead.getAttempts()).isEqualTo(1);
        assertThat(dead.getLastError()).contains("No handler registered");
        awaitDeadLetter(job.getId());
    }

    // ------------------------------------------------------------------ worker failure and redelivery

    @Test
    void messageIsRedeliveredAndProcessedAfterAWorkerDiesMidJob() throws Exception {
        stopWorker();
        Job job = jobService.submit("counting", Map.of(), null);

        // A worker takes the message, claims the job (RUNNING, attempt 1), then dies before acking.
        Connection doomedWorker = takeMessageWithoutAcking(job.getId());
        jobService.claim(job.getId(), false, "doomed-worker");
        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus()).isEqualTo(JobStatus.RUNNING);
        doomedWorker.close(); // the broker requeues the unacked message with redelivered=true

        startWorker();

        Job done = awaitStatus(job.getId(), JobStatus.SUCCEEDED);
        assertThat(done.getAttempts()).isEqualTo(2); // the crashed attempt counts
        assertThat(TestHandlers.executions(job.getId())).isEqualTo(1);
        awaitQueueDrained(Topology.JOBS_QUEUE);
    }

    @Test
    void jobThatKeepsKillingItsWorkerIsDeadLetteredInsteadOfLoopingForever() throws Exception {
        stopWorker();
        Job job = jobService.submit("counting", Map.of(), null);

        // Three workers in a row claim the job and die: the attempt budget (3) is spent without a single result.
        Connection doomedWorker = takeMessageWithoutAcking(job.getId());
        for (int crash = 1; crash <= 3; crash++) {
            jobService.claim(job.getId(), crash > 1, "doomed-worker-" + crash);
        }
        doomedWorker.close();

        startWorker();

        Job dead = awaitStatus(job.getId(), JobStatus.DEAD);
        assertThat(dead.getAttempts()).isEqualTo(3);
        assertThat(dead.getLastError()).contains("Retry limit reached");
        assertThat(TestHandlers.executions(job.getId())).isZero(); // never executed again
        awaitDeadLetter(job.getId());
    }

    // ------------------------------------------------------------------ terminal states and duplicates

    @Test
    void duplicateMessagesForAFinishedJobAreAcknowledgedAndNotExecutedAgain() {
        Job job = jobService.submit("counting", Map.of(), null);
        awaitStatus(job.getId(), JobStatus.SUCCEEDED);
        assertThat(TestHandlers.executions(job.getId())).isEqualTo(1);

        publisher.publishNew(job.getId());
        publisher.publishNew(job.getId());

        awaitQueueDrained(Topology.JOBS_QUEUE);
        // Stays at exactly one execution, and the job stays terminal.
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5))
                .until(() -> TestHandlers.executions(job.getId()) == 1);
        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(jobRepository.findById(job.getId()).orElseThrow().getAttempts()).isEqualTo(1);
    }

    @Test
    void deadJobStaysDeadWhenAFreshDuplicateArrives() {
        Job job = jobService.submit("fail", Map.of(), 1);
        awaitStatus(job.getId(), JobStatus.DEAD);
        awaitDeadLetter(job.getId());

        publisher.publishNew(job.getId());

        awaitQueueDrained(Topology.JOBS_QUEUE);
        Job after = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(after.getStatus()).isEqualTo(JobStatus.DEAD);
        assertThat(after.getAttempts()).isEqualTo(1);
    }

    @Test
    void messageForAnUnknownJobIsDeadLettered() {
        UUID ghost = UUID.randomUUID();

        publisher.publishNew(ghost);

        awaitDeadLetter(ghost);
    }

    // ------------------------------------------------------------------ lost publish

    @Test
    void sweeperRepublishesAJobWhoseMessageWasNeverPublished() {
        // Row committed but publish never happened (API crashed, broker down): save without publishing.
        Job job = jobRepository.save(new Job("echo", Map.of(), 3));
        jdbc.update("update jobs set updated_at = now() - interval '5 minutes' where id = ?", job.getId());

        awaitStatus(job.getId(), JobStatus.SUCCEEDED);
    }

    // ------------------------------------------------------------------ helpers

    private Job awaitStatus(UUID id, JobStatus expected) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100)).untilAsserted(() ->
                assertThat(jobRepository.findById(id).orElseThrow().getStatus()).isEqualTo(expected));
        return jobRepository.findById(id).orElseThrow();
    }

    private void awaitQueueDrained(String queue) {
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(amqpAdmin.getQueueInfo(queue).getMessageCount()).isZero());
    }

    /** Pulls messages off the DLQ until the one for this job shows up (earlier tests may have left others). */
    private Message awaitDeadLetter(UUID jobId) {
        AtomicReference<Message> found = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            Message message = rabbitTemplate.receive(Topology.DEAD_LETTER_QUEUE, 500);
            if (message != null && new String(message.getBody(), UTF_8).contains(jobId.toString())) {
                found.set(message);
                return true;
            }
            return false;
        });
        return found.get();
    }

    /**
     * Opens its own raw AMQP connection (standing in for a separate worker process), fetches this job's message
     * and deliberately never acks it. Closing the returned connection simulates the worker dying.
     */
    private Connection takeMessageWithoutAcking(UUID jobId) throws Exception {
        Connection connection = connectionFactory.getRabbitConnectionFactory().newConnection();
        Channel channel = connection.createChannel();
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            GetResponse response = channel.basicGet(Topology.JOBS_QUEUE, false);
            if (response == null) {
                return false;
            }
            if (new String(response.getBody(), UTF_8).contains(jobId.toString())) {
                return true;
            }
            channel.basicAck(response.getEnvelope().getDeliveryTag(), false); // stale duplicate from another test
            return false;
        });
        return connection;
    }

    private void stopWorker() {
        registry.getListenerContainer("jobWorker").stop();
    }

    private void startWorker() {
        registry.getListenerContainer("jobWorker").start();
    }
}
