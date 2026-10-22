package com.jobscheduler.worker;

import com.jobscheduler.domain.Job;

/** Business logic for one job type. Register an implementation as a Spring bean to add a new type. */
public interface JobHandler {

    String type();

    /**
     * Runs the job. Any exception triggers the retry policy; throw {@link NonRetryableJobException} to skip
     * retries and dead-letter immediately.
     */
    void handle(Job job) throws Exception;

    static JobHandler of(String type, Body body) {
        return new JobHandler() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public void handle(Job job) throws Exception {
                body.run(job);
            }
        };
    }

    @FunctionalInterface
    interface Body {
        void run(Job job) throws Exception;
    }
}
