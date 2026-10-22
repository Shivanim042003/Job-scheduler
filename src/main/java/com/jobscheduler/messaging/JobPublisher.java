package com.jobscheduler.messaging;

import java.util.Collection;
import java.util.UUID;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

/** Publishes with broker confirms, so "publish returned" means "the broker has the message". */
@Component
public class JobPublisher {

    private static final long CONFIRM_TIMEOUT_MILLIS = 5_000;

    private final RabbitTemplate rabbitTemplate;

    public JobPublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    /** Puts a job on the main queue for immediate pickup. */
    public void publishNew(UUID jobId) {
        publishNewBatch(java.util.List.of(jobId));
    }

    /** Publishes many jobs and waits for the broker's confirmation once, not once per message. */
    public void publishNewBatch(Collection<UUID> jobIds) {
        rabbitTemplate.invoke(ops -> {
            for (UUID jobId : jobIds) {
                ops.convertAndSend(Topology.JOBS_EXCHANGE, Topology.JOBS_ROUTING_KEY, new JobMessage(jobId));
            }
            ops.waitForConfirmsOrDie(CONFIRM_TIMEOUT_MILLIS);
            return null;
        });
    }

    /** Parks a job in the delay queue for the given failed attempt; the broker re-queues it when the TTL expires. */
    public void publishRetry(UUID jobId, int failedAttempt) {
        publish("", Topology.retryQueue(failedAttempt), jobId);
    }

    /** Puts a message straight onto the dead-letter queue (used when the reaper declares a job dead). */
    public void publishDeadLetter(UUID jobId) {
        publish(Topology.DEAD_LETTER_EXCHANGE, Topology.DEAD_LETTER_ROUTING_KEY, jobId);
    }

    private void publish(String exchange, String routingKey, UUID jobId) {
        rabbitTemplate.invoke(ops -> {
            ops.convertAndSend(exchange, routingKey, new JobMessage(jobId));
            ops.waitForConfirmsOrDie(CONFIRM_TIMEOUT_MILLIS);
            return null;
        });
    }
}
