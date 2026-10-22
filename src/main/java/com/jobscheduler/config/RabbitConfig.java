package com.jobscheduler.config;

import static com.jobscheduler.messaging.Topology.DEAD_LETTER_EXCHANGE;
import static com.jobscheduler.messaging.Topology.DEAD_LETTER_QUEUE;
import static com.jobscheduler.messaging.Topology.DEAD_LETTER_ROUTING_KEY;
import static com.jobscheduler.messaging.Topology.JOBS_EXCHANGE;
import static com.jobscheduler.messaging.Topology.JOBS_QUEUE;
import static com.jobscheduler.messaging.Topology.JOBS_ROUTING_KEY;

import com.jobscheduler.messaging.BackoffPolicy;
import com.jobscheduler.messaging.Topology;
import java.util.ArrayList;
import java.util.List;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

    /**
     * Declared by both roles on startup (declarations are idempotent). Note: RabbitMQ rejects re-declaring a
     * queue with different arguments, so changing the backoff settings requires deleting the retry queues.
     */
    @Bean
    Declarables jobTopology(SchedulerProperties properties, BackoffPolicy backoff) {
        DirectExchange jobsExchange = new DirectExchange(JOBS_EXCHANGE, true, false);
        DirectExchange deadLetterExchange = new DirectExchange(DEAD_LETTER_EXCHANGE, true, false);

        Queue jobsQueue = QueueBuilder.durable(JOBS_QUEUE)
                .deadLetterExchange(DEAD_LETTER_EXCHANGE)
                .deadLetterRoutingKey(DEAD_LETTER_ROUTING_KEY)
                .build();
        Queue deadLetterQueue = QueueBuilder.durable(DEAD_LETTER_QUEUE).build();

        List<Declarable> declarables = new ArrayList<>(List.<Declarable>of(
                jobsExchange, deadLetterExchange, jobsQueue, deadLetterQueue,
                BindingBuilder.bind(jobsQueue).to(jobsExchange).with(JOBS_ROUTING_KEY),
                BindingBuilder.bind(deadLetterQueue).to(deadLetterExchange).with(DEAD_LETTER_ROUTING_KEY)));

        // A failed attempt N (< maxAttempts) is parked in jobs.retry.N until its TTL expires, then the broker
        // dead-letters it back onto the main exchange. No consumers, no bindings: published via the default
        // exchange by queue name.
        for (int attempt = 1; attempt < properties.retry().maxAttempts(); attempt++) {
            declarables.add(QueueBuilder.durable(Topology.retryQueue(attempt))
                    .ttl(Math.toIntExact(backoff.delayFor(attempt).toMillis()))
                    .deadLetterExchange(JOBS_EXCHANGE)
                    .deadLetterRoutingKey(JOBS_ROUTING_KEY)
                    .build());
        }
        return new Declarables(declarables);
    }

    @Bean
    MessageConverter messageConverter() {
        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
        DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
        typeMapper.setTrustedPackages("com.jobscheduler.messaging");
        typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.INFERRED);
        converter.setJavaTypeMapper(typeMapper);
        return converter;
    }
}
