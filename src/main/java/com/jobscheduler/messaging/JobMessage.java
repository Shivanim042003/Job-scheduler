package com.jobscheduler.messaging;

import java.util.UUID;

/** The queue carries only an ID; PostgreSQL is the source of truth for everything else. */
public record JobMessage(UUID jobId) {
}
