package com.jobscheduler.service;

import com.jobscheduler.config.SchedulerProperties;
import com.jobscheduler.domain.JobRepository;
import com.jobscheduler.domain.JobStatus;
import com.jobscheduler.messaging.Topology;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * System-level gauges, exposed on the API at /actuator/prometheus:
 * <ul>
 *   <li>{@code jobs_by_status{status}} - jobs per lifecycle state (from PostgreSQL)</li>
 *   <li>{@code jobs_queue_depth{queue}} - messages waiting in main / retry / dead-letter queues (from RabbitMQ)</li>
 * </ul>
 * Per-execution timers and retry/dead counters ({@code jobs_execution_seconds}, {@code jobs_retries_total},
 * {@code jobs_dead_total}) are recorded by the workers' own registries.
 */
@Component
@Profile("api")
public class JobMetrics {

    public JobMetrics(MeterRegistry registry, JobRepository repository, AmqpAdmin amqpAdmin,
                      SchedulerProperties properties) {
        for (JobStatus status : JobStatus.values()) {
            Gauge.builder("jobs.by.status", repository, r -> r.countByStatus(status))
                    .tag("status", status.name())
                    .register(registry);
        }

        Gauge.builder("jobs.queue.depth", () -> depth(amqpAdmin, Topology.JOBS_QUEUE))
                .tag("queue", "main").register(registry);
        Gauge.builder("jobs.queue.depth", () -> depth(amqpAdmin, Topology.DEAD_LETTER_QUEUE))
                .tag("queue", "dead_letter").register(registry);
        Gauge.builder("jobs.queue.depth", () -> {
                    int total = 0;
                    for (int attempt = 1; attempt < properties.retry().maxAttempts(); attempt++) {
                        total += depth(amqpAdmin, Topology.retryQueue(attempt));
                    }
                    return total;
                })
                .tag("queue", "retry").register(registry);
    }

    private static int depth(AmqpAdmin amqpAdmin, String queue) {
        try {
            QueueInformation info = amqpAdmin.getQueueInfo(queue);
            return info == null ? 0 : info.getMessageCount();
        } catch (Exception e) {
            return 0; // broker briefly unreachable: report 0 rather than failing the whole scrape
        }
    }
}
