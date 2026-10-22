-- Phase 3: observability log of lake publications. The Iceberg tables are authoritative for what was published
-- (an append table's high-water mark is the max of its mark column, committed with the rows); this table only
-- records each run for the control tower and for troubleshooting.
CREATE TABLE IF NOT EXISTS ops.lake_publish_runs (
    run_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_name    text        NOT NULL,
    mode          text        NOT NULL CHECK (mode IN ('append', 'replace')),
    rows_written  integer     NOT NULL,
    hwm_before    bigint,
    hwm_after     bigint,
    snapshot_id   bigint,
    skipped       text,
    seconds       numeric(10,2) NOT NULL,
    published_at  timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS ix_lake_publish_runs_table ON ops.lake_publish_runs (table_name, published_at);

-- Results of `signalflow lake compare` (Gold on Postgres vs Gold on Trino/Iceberg), one row per model per check.
CREATE TABLE IF NOT EXISTS ops.lake_differential_checks (
    check_id       bigint      NOT NULL,
    model          text        NOT NULL,
    rows_postgres  integer     NOT NULL,
    rows_lake      integer     NOT NULL,
    identical      boolean     NOT NULL,
    checked_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (check_id, model)
);
CREATE SEQUENCE IF NOT EXISTS ops.lake_differential_check_seq;
