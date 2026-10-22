-- Phase 4: Revenue Leak Hunter cases, operator accounts and the audit log (ADR-0010).
CREATE SCHEMA IF NOT EXISTS assurance;

-- Rule catalogue, written by the hunter from code on every run (the code is the single definition).
CREATE TABLE IF NOT EXISTS assurance.rules (
    rule_id          text PRIMARY KEY,
    domain           text        NOT NULL CHECK (domain IN ('telecom', 'mfg')),
    title            text        NOT NULL,
    description      text        NOT NULL,
    exposure_kind    text        NOT NULL,
    expected_within  interval    NOT NULL,   -- normal processing time: PENDING until then
    confirm_after    interval    NOT NULL,   -- late-data horizon: CONFIRMED_BY_RULE after this
    sql_sha256       char(64)    NOT NULL,
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- One case per (rule, subject). Status is the lifecycle; rule_status is what the rule computes at the last run.
CREATE TABLE IF NOT EXISTS assurance.cases (
    case_id            text PRIMARY KEY,
    rule_id            text        NOT NULL REFERENCES assurance.rules,
    domain             text        NOT NULL,
    subject_key        text        NOT NULL,
    status             text        NOT NULL CHECK (status IN ('PENDING', 'SUSPICIOUS', 'CONFIRMED_BY_RULE', 'RESOLVED')),
    rule_status        text        CHECK (rule_status IN ('PENDING', 'SUSPICIOUS', 'CONFIRMED_BY_RULE')),
    observed           boolean     NOT NULL,          -- discrepancy present at the last hunter run
    resolution         text,                          -- AUTO_CLEARED or an analyst resolution code
    business_unit      text        NOT NULL,          -- channel, dealer region or warehouse
    region_code        text,
    business_date      date        NOT NULL,          -- Asia/Dhaka date of the anchoring transaction
    anchor_time        timestamptz NOT NULL,          -- event time the windows are measured from
    summary            text        NOT NULL,
    exposure_bdt       numeric(16,2) NOT NULL,        -- estimate, never an audited loss
    evidence           jsonb       NOT NULL,          -- supporting transaction ids, amounts and times
    evidence_sha256    char(64)    NOT NULL,
    assignee           text,
    first_detected_at  timestamptz NOT NULL DEFAULT now(),
    first_detected_as_of timestamptz NOT NULL,        -- stream clock at first detection
    confirmed_as_of    timestamptz,                   -- stream clock when the rule first confirmed it
    resolved_at        timestamptz,
    resolved_as_of     timestamptz,
    resolved_by        text,
    last_evaluated_as_of timestamptz NOT NULL,
    updated_at         timestamptz NOT NULL DEFAULT now(),
    version            integer     NOT NULL DEFAULT 1, -- optimistic concurrency for analyst actions
    UNIQUE (rule_id, subject_key),
    CHECK ((status = 'RESOLVED') = (resolution IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS ix_cases_status ON assurance.cases (status, domain);
CREATE INDEX IF NOT EXISTS ix_cases_business_date ON assurance.cases (business_date);

-- Append-only case history: every status change, note, assignment and resolution.
CREATE TABLE IF NOT EXISTS assurance.case_events (
    case_event_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    case_id        text        NOT NULL REFERENCES assurance.cases,
    action         text        NOT NULL,       -- OPENED, STATUS_CHANGED, AUTO_CLEARED, REOPENED, NOTE, ASSIGNED, RESOLVED
    from_status    text,
    to_status      text,
    actor          text        NOT NULL,       -- 'system:leak-hunter' or an operator username
    as_of          timestamptz,                -- stream clock for hunter actions
    note           text,
    details        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    at             timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_case_events_case ON assurance.case_events (case_id, case_event_id);

CREATE TABLE IF NOT EXISTS assurance.hunter_runs (
    run_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    as_of        jsonb       NOT NULL,          -- stream clocks used, per stream
    observed     integer     NOT NULL,
    opened       integer     NOT NULL,
    status_changes integer   NOT NULL,
    auto_cleared integer     NOT NULL,
    reopened     integer     NOT NULL,
    per_rule     jsonb       NOT NULL,
    started_at   timestamptz NOT NULL,
    finished_at  timestamptz NOT NULL DEFAULT now()
);

-- Offline evaluation results (written by the separate evaluation package, never read by detection).
CREATE TABLE IF NOT EXISTS assurance.evaluation_runs (
    evaluation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset       jsonb       NOT NULL,          -- generator config of the evaluated dataset
    results       jsonb       NOT NULL,          -- per rule: positives, detected, confirmed, precision, recall
    benign_checks jsonb       NOT NULL,          -- delayed-but-normal scenarios and whether any was confirmed
    passed        boolean     NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);

-- Operators and roles for actions in the control tower (viewer < analyst < operator < admin).
CREATE TABLE IF NOT EXISTS ops.operators (
    username       text PRIMARY KEY CHECK (username ~ '^[a-z][a-z0-9_.-]{2,31}$'),
    display_name   text        NOT NULL,
    role           text        NOT NULL CHECK (role IN ('viewer', 'analyst', 'operator', 'admin')),
    password_hash  text        NOT NULL,          -- scrypt, see signalflow.security
    disabled       boolean     NOT NULL DEFAULT false,
    created_at     timestamptz NOT NULL DEFAULT now()
);

-- Audit log of operator actions, including denied attempts. Rows are written in the action's transaction.
CREATE TABLE IF NOT EXISTS ops.audit_log (
    audit_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    at           timestamptz NOT NULL DEFAULT now(),
    actor        text        NOT NULL,
    actor_role   text,
    action       text        NOT NULL,
    target_type  text,
    target_id    text,
    outcome      text        NOT NULL CHECK (outcome IN ('OK', 'DENIED', 'FAILED')),
    details      jsonb       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS ix_audit_log_at ON ops.audit_log (at);
