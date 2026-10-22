package com.jobscheduler.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Periodically reclaims jobs whose worker stopped renewing its lease. See {@link JobService#reapExpiredLeases()}. */
@Component
@Profile("api")
public class LeaseReaper {

    private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);

    private final JobService jobService;

    public LeaseReaper(JobService jobService) {
        this.jobService = jobService;
    }

    @Scheduled(fixedDelayString = "${scheduler.lease.reaper-interval-millis:5000}")
    public void reap() {
        try {
            int reaped = jobService.reapExpiredLeases();
            if (reaped > 0) {
                log.warn("Reclaimed {} job(s) whose worker stopped renewing its lease", reaped);
            }
        } catch (Exception e) {
            log.warn("Lease reaper pass failed; will retry: {}", e.toString());
        }
    }
}
