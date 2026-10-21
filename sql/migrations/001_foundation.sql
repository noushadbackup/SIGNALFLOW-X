-- SignalFlow X — lite profile foundation: bronze, silver, ops schemas.
-- Gold is owned by dbt (schema `gold`), reference data by the reference loader (schema `ref`).

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS ref;

-- Bronze: one row per received record, verbatim. Extracted columns are best-effort hints, never trusted.
CREATE TABLE IF NOT EXISTS bronze.raw_events (
    bronze_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_stream    text        NOT NULL,          -- e.g. 'file:arrivals', later 'kafka:tel.recharge.v1'
    source_position  text        NOT NULL,          -- e.g. '<file>:<line>', later '<partition>:<offset>'
    arrival_time     timestamptz,                   -- transport time (file wrapper / Kafka record timestamp)
    ingested_at      timestamptz NOT NULL DEFAULT now(),
    ingest_batch_id  text        NOT NULL,
    event_id         text,
    event_type       text,
    schema_version   text,
    event_time       timestamptz,
    raw_record       text        NOT NULL,          -- original line text, byte-for-byte
    record_sha256    char(64)    NOT NULL,
    parse_error      text,
    CONSTRAINT uq_bronze_source_position UNIQUE (source_stream, source_position)
);
CREATE INDEX IF NOT EXISTS ix_bronze_event_id ON bronze.raw_events (event_id);
CREATE INDEX IF NOT EXISTS ix_bronze_event_time ON bronze.raw_events (event_time);

-- Silver: one row per unique, contract-valid event.
CREATE TABLE IF NOT EXISTS silver.events (
    event_id          uuid        PRIMARY KEY,
    event_type        text        NOT NULL,
    schema_version    text        NOT NULL,
    contract_id       text        NOT NULL,
    stream            text        NOT NULL CHECK (stream IN ('telecom', 'mfg')),
    source_system     text        NOT NULL,
    event_time        timestamptz NOT NULL,
    produced_at       timestamptz NOT NULL,
    arrival_time      timestamptz,
    payload           jsonb       NOT NULL,
    event_sha256      char(64)    NOT NULL,      -- canonical JSON of the whole event; duplicate vs conflict
    arrival_class     text        NOT NULL CHECK (arrival_class IN ('ON_TIME', 'LATE_WITHIN_WATERMARK', 'LATE_BEYOND_WATERMARK')),
    lateness_seconds  integer     NOT NULL,
    bronze_id         bigint      NOT NULL REFERENCES bronze.raw_events (bronze_id),
    processed_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_silver_events_type_time ON silver.events (event_type, event_time);

CREATE TABLE IF NOT EXISTS silver.quarantine (
    bronze_id      bigint      PRIMARY KEY REFERENCES bronze.raw_events (bronze_id),
    event_id       text,
    event_type     text,
    schema_version text,
    reason_code    text        NOT NULL,
    details        jsonb       NOT NULL,
    contract_id    text,
    quarantined_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_quarantine_reason ON silver.quarantine (reason_code);

-- Ops: processing state and evidence.
CREATE TABLE IF NOT EXISTS ops.checkpoints (
    processor        text        PRIMARY KEY,
    last_bronze_id   bigint      NOT NULL DEFAULT 0,
    updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.stream_state (
    stream          text        PRIMARY KEY,
    max_event_time  timestamptz,
    watermark       timestamptz,
    allowed_lateness interval   NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.duplicate_events (
    bronze_id        bigint      PRIMARY KEY REFERENCES bronze.raw_events (bronze_id),
    event_id         uuid        NOT NULL,
    first_bronze_id  bigint      NOT NULL,
    detected_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.late_events (
    event_id          uuid        PRIMARY KEY,
    stream            text        NOT NULL,
    event_time        timestamptz NOT NULL,
    watermark_at_arrival timestamptz NOT NULL,
    lateness_seconds  integer     NOT NULL,
    business_date     date        NOT NULL,     -- Asia/Dhaka
    detected_at       timestamptz NOT NULL DEFAULT now()
);

-- A business date whose KPIs were already final when a beyond-watermark event arrived.
CREATE TABLE IF NOT EXISTS ops.kpi_restatements (
    stream          text        NOT NULL,
    business_date   date        NOT NULL,
    late_event_count integer    NOT NULL,
    first_detected_at timestamptz NOT NULL DEFAULT now(),
    last_detected_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stream, business_date)
);

CREATE TABLE IF NOT EXISTS ops.processing_runs (
    run_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    processor       text        NOT NULL,
    from_bronze_id  bigint      NOT NULL,
    to_bronze_id    bigint      NOT NULL,
    rows_read       integer     NOT NULL,
    rows_accepted   integer     NOT NULL,
    rows_quarantined integer    NOT NULL,
    rows_duplicate  integer     NOT NULL,
    started_at      timestamptz NOT NULL,
    finished_at     timestamptz NOT NULL DEFAULT now()
);

-- Reference (master) data snapshots, loaded by replace-in-transaction.
CREATE TABLE IF NOT EXISTS ref.regions (
    region_code text PRIMARY KEY, region_name text NOT NULL, demand_weight numeric(6,4) NOT NULL
);
CREATE TABLE IF NOT EXISTS ref.packages (
    package_code text PRIMARY KEY, package_name text NOT NULL, category text NOT NULL,
    price_bdt numeric(14,2) NOT NULL, validity_days integer NOT NULL, data_mb integer NOT NULL, voice_minutes integer NOT NULL
);
CREATE TABLE IF NOT EXISTS ref.products (
    sku text PRIMARY KEY, product_name text NOT NULL, category text NOT NULL, dealer_price_bdt numeric(14,2) NOT NULL
);
CREATE TABLE IF NOT EXISTS ref.factories (
    factory_id text PRIMARY KEY, factory_name text NOT NULL, region_code text NOT NULL REFERENCES ref.regions
);
CREATE TABLE IF NOT EXISTS ref.warehouses (
    warehouse_id text PRIMARY KEY, warehouse_name text NOT NULL, region_code text NOT NULL REFERENCES ref.regions,
    is_central boolean NOT NULL
);
CREATE TABLE IF NOT EXISTS ref.dealers (
    dealer_id text PRIMARY KEY, dealer_name text NOT NULL, region_code text NOT NULL REFERENCES ref.regions,
    tier text NOT NULL, credit_days integer NOT NULL
);

CREATE TABLE IF NOT EXISTS ops.schema_migrations (
    version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now()
);
