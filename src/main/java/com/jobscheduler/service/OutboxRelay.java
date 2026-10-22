package com.jobscheduler.service;

import com.jobscheduler.messaging.JobPublisher;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Drains the transactional outbox: reads pending rows, publishes them to RabbitMQ in one batch with a single
 * broker confirmation, and deletes them, all inside one transaction. If the process dies after publishing but before
 * the delete commits, the rows are published again; duplicates are harmless because workers claim jobs with a
 * conditional update. {@code FOR UPDATE SKIP LOCKED} lets several API instances relay in parallel without
 * publishing the same row twice.
 */
@Component
@Profile("api")
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    static final int BATCH_SIZE = 200;

    private final JdbcTemplate jdbc;
    private final JobPublisher publisher;
    private final TransactionTemplate tx;

    public OutboxRelay(JdbcTemplate jdbc, JobPublisher publisher, PlatformTransactionManager transactionManager) {
        this.jdbc = jdbc;
        this.publisher = publisher;
        this.tx = new TransactionTemplate(transactionManager);
    }

    @Scheduled(fixedDelayString = "${scheduler.outbox.poll-millis:50}")
    public void poll() {
        try {
            int relayed;
            do {
                relayed = relayOnce();
            } while (relayed == BATCH_SIZE);
        } catch (Exception e) {
            // Broker or database trouble: the rows stay in the outbox and the next poll retries them.
            log.warn("Outbox relay failed; will retry: {}", e.toString());
        }
    }

    /** @return number of outbox rows published in this pass */
    public int relayOnce() {
        Integer relayed = tx.execute(status -> {
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "select id, job_id from outbox order by id limit ? for update skip locked", BATCH_SIZE);
            if (rows.isEmpty()) {
                return 0;
            }
            List<Long> ids = new ArrayList<>(rows.size());
            List<UUID> jobIds = new ArrayList<>(rows.size());
            for (Map<String, Object> row : rows) {
                ids.add(((Number) row.get("id")).longValue());
                jobIds.add((UUID) row.get("job_id"));
            }
            publisher.publishNewBatch(jobIds);
            jdbc.batchUpdate("delete from outbox where id = ?", ids, ids.size(),
                    (ps, id) -> ps.setLong(1, id));
            return rows.size();
        });
        return relayed == null ? 0 : relayed;
    }
}
