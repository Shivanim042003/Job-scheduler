-- Ledger of side effects already applied, so a re-executed job does not repeat them.
CREATE TABLE job_effects (
    job_id     UUID         NOT NULL,
    effect_key VARCHAR(200) NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, effect_key)
);
