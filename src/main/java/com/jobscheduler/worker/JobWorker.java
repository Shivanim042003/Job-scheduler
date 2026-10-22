package com.jobscheduler.worker;

import com.jobscheduler.messaging.JobMessage;
import com.jobscheduler.messaging.Topology;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * RabbitMQ consumer with manual acknowledgements. The message is acked only after the outcome has been recorded
 * in PostgreSQL, so a worker that dies mid-job leaves the message unacked and the broker redelivers it.
 * Delivery is therefore at-least-once; the claim logic in {@code JobService} makes redelivery safe.
 */
@Component
@Profile("worker")
public class JobWorker {

    private static final Logger log = LoggerFactory.getLogger(JobWorker.class);
    private static final long INFRASTRUCTURE_BACKOFF_MILLIS = 1_000;

    private final JobProcessor processor;

    public JobWorker(JobProcessor processor) {
        this.processor = processor;
    }

    @RabbitListener(id = "jobWorker", queues = Topology.JOBS_QUEUE, ackMode = "MANUAL")
    public void onMessage(JobMessage message,
                          Channel channel,
                          @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
                          @Header(value = AmqpHeaders.REDELIVERED, required = false) Boolean redelivered)
            throws IOException {

        if (message == null || message.jobId() == null) {
            log.warn("Malformed job message; dead-lettering");
            channel.basicReject(deliveryTag, false);
            return;
        }

        JobProcessor.Settlement settlement;
        try {
            settlement = processor.process(message.jobId(), Boolean.TRUE.equals(redelivered));
        } catch (Exception e) {
            // Database or broker trouble, not a job failure: put the message back and slow down.
            log.error("Infrastructure error while processing job {}; requeueing", message.jobId(), e);
            pause();
            channel.basicNack(deliveryTag, false, true);
            return;
        }

        switch (settlement) {
            case ACK -> channel.basicAck(deliveryTag, false);
            case DEAD_LETTER -> channel.basicReject(deliveryTag, false);
        }
    }

    private static void pause() {
        try {
            Thread.sleep(INFRASTRUCTURE_BACKOFF_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
