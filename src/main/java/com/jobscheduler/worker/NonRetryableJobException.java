package com.jobscheduler.worker;

/** A failure that retrying cannot fix (bad payload, unknown job type). The job is dead-lettered immediately. */
public class NonRetryableJobException extends RuntimeException {

    public NonRetryableJobException(String message) {
        super(message);
    }
}
