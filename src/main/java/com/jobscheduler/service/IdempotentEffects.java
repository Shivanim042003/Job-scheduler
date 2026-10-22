package com.jobscheduler.service;

import com.jobscheduler.domain.Job;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Makes a job's side effects safe under at-least-once delivery. A job can be executed again after a crash that
 * happened <em>after</em> its side effect but <em>before</em> its result was recorded, and a hung "zombie" worker can
 * keep running after its job was taken over. Handlers wrap each effect in {@link #runOnceAsOwner}, which guarantees:
 *
 * <ul>
 *   <li><b>At most once per (job, effectKey):</b> a ledger row with a unique key is written in the same transaction as
 *       the effect, so a repeat sees the row and skips the effect.</li>
 *   <li><b>Only by the current owner:</b> the call first locks the job row and checks that this worker still owns
 *       this exact attempt. A worker that was replaced gets {@link EffectResult#NOT_OWNER} and applies nothing. The
 *       row lock also holds off a concurrent takeover until the effect has committed.</li>
 * </ul>
 *
 * <p>For effects inside this PostgreSQL database (insert a row, update a balance) that makes the effect exactly-once.
 * For effects in an external system (email, payment API) a local ledger cannot make the call atomic: pass
 * {@code jobId + effectKey} to the external system as its idempotency key and let it deduplicate. Anything a handler
 * does outside this API can repeat.
 */
@Service
public class IdempotentEffects {

    public enum EffectResult {
        /** The effect ran now. */
        APPLIED,
        /** The effect had already been applied for this job and key; nothing ran. */
        ALREADY_APPLIED,
        /** This worker no longer owns the job (taken over or finished); nothing ran. */
        NOT_OWNER
    }

    private final JdbcTemplate jdbc;

    public IdempotentEffects(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Applies {@code effect} once per (job, effectKey), and only if {@code job} (as returned by the worker's claim)
     * is still RUNNING and owned by the same worker and attempt. If the effect throws, the transaction rolls back,
     * the key is not consumed, and a later attempt will run it again.
     */
    @Transactional
    public EffectResult runOnceAsOwner(Job job, String effectKey, Runnable effect) {
        // FOR SHARE: blocks a concurrent takeover/reaper UPDATE of this row until we commit, and fails the
        // ownership check if the takeover already happened.
        List<Integer> owned = jdbc.queryForList(
                "select 1 from jobs where id = ? and status = 'RUNNING' and worker_id = ? and attempts = ? for share",
                Integer.class, job.getId(), job.getWorkerId(), job.getAttempts());
        if (owned.isEmpty()) {
            return EffectResult.NOT_OWNER;
        }
        return runOnce(job.getId(), effectKey, effect) ? EffectResult.APPLIED : EffectResult.ALREADY_APPLIED;
    }

    /**
     * Lower-level building block without the ownership check. Prefer {@link #runOnceAsOwner} inside handlers.
     *
     * @return true if the effect ran now, false if it had already been applied for this job and key
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
