-- Phase 5: alerts, Chaos Lab runs, Data Time Machine replays (ADR-0011, ADR-0012).

-- Diagnostic alerts raised by deterministic checks (signalflow.observability.alerts). One FIRING row per
-- (rule, subject); a later evaluation that no longer sees the condition marks it RESOLVED.
CREATE TABLE IF NOT EXISTS ops.alerts (
    alert_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rule         text        NOT NULL,
    subject      text        NOT NULL,
    severity     text        NOT NULL CHECK (severity IN ('WARNING', 'CRITICAL')),
    status       text        NOT NULL CHECK (status IN ('FIRING', 'RESOLVED')),
    summary      text        NOT NULL,
    details      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    first_seen   timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen    timestamptz NOT NULL DEFAULT clock_timestamp(),
    resolved_at  timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_alerts_firing ON ops.alerts (rule, subject) WHERE status = 'FIRING';

-- Chaos Lab incident runs. Recorded in the control database; the incident itself runs in a disposable lab database.
CREATE TABLE IF NOT EXISTS ops.chaos_runs (
    run_id        text        PRIMARY KEY,
    incident      text        NOT NULL CHECK (incident IN ('A', 'B', 'C', 'D', 'E')),
    title         text        NOT NULL,
    lab_database  text        NOT NULL,
    dataset       jsonb       NOT NULL,
    operator      text        NOT NULL,
    operator_role text        NOT NULL,
    status        text        NOT NULL CHECK (status IN ('RUNNING', 'PASSED', 'FAILED', 'ERROR')),
    phase         text        NOT NULL,
    started_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at   timestamptz,
    report        jsonb       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS ix_chaos_runs_started ON ops.chaos_runs (started_at);

-- What a clean run of a lab dataset produces (Silver fingerprint, Gold totals). Post-recovery state is compared with
-- it. Keyed by the dataset's generator config.
CREATE TABLE IF NOT EXISTS ops.chaos_references (
    dataset_key  text        PRIMARY KEY,
    dataset      jsonb       NOT NULL,
    metrics      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Data Time Machine: one row per replay of an event-time window from Bronze.
CREATE TABLE IF NOT EXISTS ops.replay_runs (
    replay_id          text        PRIMARY KEY,
    stream             text        NOT NULL CHECK (stream IN ('telecom', 'mfg')),
    window_start       timestamptz NOT NULL,
    window_end         timestamptz NOT NULL CHECK (window_end > window_start),
    requested_by       text        NOT NULL,
    status             text        NOT NULL CHECK (status IN ('PREVIEWED', 'PROMOTED', 'UNDONE', 'DISCARDED')),
    bronze_rows        integer     NOT NULL,
    summary            jsonb       NOT NULL,          -- counts per change and per outcome
    metrics_diff       jsonb       NOT NULL,          -- Silver metrics per business date / event type, current vs replay
    verification       jsonb       NOT NULL DEFAULT '[]'::jsonb,
    gold_before        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    gold_after         jsonb,
    silver_fingerprint text        NOT NULL,          -- affected Silver rows when the preview was computed
    replay_fingerprint text        NOT NULL,          -- the same rows as the replay produces them
    created_at         timestamptz NOT NULL DEFAULT clock_timestamp(),
    promoted_at        timestamptz,
    promoted_by        text,
    undone_at          timestamptz,
    undone_by          text,
    lake               jsonb       NOT NULL DEFAULT '{}'::jsonb   -- Iceberg snapshot ids before/after a lake sync
);

-- One row per replayed Bronze record: what the current code makes of it, isolated from Silver until promotion.
CREATE TABLE IF NOT EXISTS ops.replay_events (
    replay_id      text        NOT NULL REFERENCES ops.replay_runs (replay_id) ON DELETE CASCADE,
    bronze_id      bigint      NOT NULL,
    event_id       uuid,
    outcome        text        NOT NULL CHECK (outcome IN ('ACCEPTED', 'DUPLICATE', 'QUARANTINED')),
    change         text        NOT NULL CHECK (change IN ('UNCHANGED', 'CHANGED', 'INSERT', 'REMOVE', 'NONE')),
    row            jsonb,                             -- the silver.events row the replay produces (ACCEPTED)
    current_row    jsonb,                             -- the silver.events row there now (CHANGED / REMOVE)
    reason         text,                              -- quarantine reason (QUARANTINED)
    PRIMARY KEY (replay_id, bronze_id)
);

-- Rows a promotion replaced, removed or inserted, per table, so the promotion can be undone exactly.
CREATE TABLE IF NOT EXISTS ops.replay_archive (
    replay_id      text        NOT NULL REFERENCES ops.replay_runs (replay_id) ON DELETE CASCADE,
    target         text        NOT NULL CHECK (target IN ('silver.events', 'silver.quarantine', 'ops.duplicate_events',
                                                         'ops.late_events')),
    key            text        NOT NULL,
    action         text        NOT NULL CHECK (action IN ('REPLACED', 'REMOVED', 'INSERTED')),
    row            jsonb,                             -- the previous row (NULL for INSERTED)
    PRIMARY KEY (replay_id, target, key)
);
