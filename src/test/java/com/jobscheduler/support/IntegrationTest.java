package com.jobscheduler.support;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

/**
 * Full application (API + worker in one JVM) against real containers. Every test class using this annotation
 * shares one cached Spring context, so the containers start once. Backoff is shortened so retry tests finish
 * in seconds.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "scheduler.retry.max-attempts=3",
                "scheduler.retry.initial-delay=200ms",
                "scheduler.retry.max-delay=2s",
                "scheduler.sweeper.interval-millis=500",
                "scheduler.sweeper.stale-after=2s",
                "spring.rabbitmq.listener.simple.concurrency=1",
                "scheduler.lease.duration=5s",
                "scheduler.lease.renew-every=1s",
                "scheduler.lease.reaper-interval-millis=500",
                "management.prometheus.metrics.export.enabled=true"
        })
@ActiveProfiles({"api", "worker"})
@Import({TestcontainersConfig.class, TestHandlers.class})
public @interface IntegrationTest {
}
