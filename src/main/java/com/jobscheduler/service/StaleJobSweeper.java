package com.jobscheduler.service;

import com.jobscheduler.config.SchedulerProperties;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Safety net for the gap between "row committed" and "message published". If the API crashed or the broker was
 * unreachable in that window the job would sit in QUEUED forever; this re-publishes it. Duplicate messages are
 * harmless because workers claim jobs with a conditional update.
 */
@Component
@Profile("api")
public class StaleJobSweeper {

    private static final Logger log = LoggerFactory.getLogger(StaleJobSweeper.class);

    private final JobService jobService;
    private final SchedulerProperties properties;

    public StaleJobSweeper(JobService jobService, SchedulerProperties properties) {
        this.jobService = jobService;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${scheduler.sweeper.interval-millis:5000}")
    public void sweep() {
        Instant cutoff = Instant.now().minus(properties.sweeper().staleAfter());
        int republished = jobService.republishStaleQueued(cutoff);
        if (republished > 0) {
            log.info("Republished {} stale queued job(s)", republished);
        }
    }
}
