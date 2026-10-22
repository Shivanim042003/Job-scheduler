package com.jobscheduler.service;

import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Makes a side effect safe to attempt more than once. Delivery is at-least-once, so a job can be executed again
 * after a crash that happened <em>after</em> its side effect but <em>before</em> its result was recorded. Handlers
 * wrap each effect in {@link #runOnce}; the second and later attempts skip it.
 *
 * <p>How strong the guarantee is depends on where the effect lives:
 * <ul>
 *   <li><b>Effect in this PostgreSQL database</b> (insert a row, update a balance): the ledger row and the effect
 *       commit in one transaction, so the effect is applied exactly once.</li>
 *   <li><b>Effect in an external system</b> (email, payment API): no local ledger can make that atomic. Pass
 *       {@code jobId + effectKey} to the external system as its idempotency key and let it deduplicate.</li>
 * </ul>
 */
@Service
public class IdempotentEffects {

    private final JdbcTemplate jdbc;

    public IdempotentEffects(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * @return true if the effect ran now, false if it had already been applied for this job and key. If the effect
     *         throws, the transaction rolls back, the key is not consumed, and a retry will run it again.
     */
    @Transactional
    public boolean runOnce(UUID jobId, String effectKey, Runnable effect) {
        // A concurrent caller with the same key blocks on this insert until we commit or roll back.
        int inserted = jdbc.update(
                "insert into job_effects (job_id, effect_key) values (?, ?) on conflict do nothing", jobId, effectKey);
        if (inserted == 0) {
            return false;
        }
        effect.run();
        return true;
    }
}
