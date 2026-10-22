package com.jobscheduler.service;

import com.jobscheduler.domain.Job;

/** What a worker should do with a delivered message. */
public record ClaimResult(Outcome outcome, Job job) {

    public enum Outcome {
        /** The worker owns the job and must execute it. */
        CLAIMED,
        /** Nothing to do (already finished, owned by another worker, or lost a race): ack and move on. */
        SKIP,
        /** The message cannot be processed: reject without requeue so the broker routes it to the DLQ. */
        DEAD_LETTER
    }

    static ClaimResult claimed(Job job) {
        return new ClaimResult(Outcome.CLAIMED, job);
    }

    static ClaimResult skip() {
        return new ClaimResult(Outcome.SKIP, null);
    }

    static ClaimResult deadLetter() {
        return new ClaimResult(Outcome.DEAD_LETTER, null);
    }
}
