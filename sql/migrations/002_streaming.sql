-- SignalFlow X — Phase 2 streaming path (Kafka → Spark → Postgres Bronze/Silver).

-- One row per committed Spark micro-batch, written in the same transaction as the batch's Bronze/Silver rows.
-- Spark re-executes a batch with the same batch_id and the same offsets after a crash between our commit and its
-- own offset commit; finding the row here turns that replay into a no-op (ADR-0008).
CREATE TABLE IF NOT EXISTS ops.stream_commits (
    query_name      text        NOT NULL,
    batch_id        bigint      NOT NULL,
    rows_received   integer     NOT NULL,
    rows_new        integer     NOT NULL,          -- rows not already in Bronze (0 on a full replay)
    offsets         jsonb       NOT NULL,          -- {"topic": {"partition": [min_offset, max_offset]}}
    committed_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (query_name, batch_id)
);

-- Replays that were detected and skipped, kept as evidence that the idempotency path actually ran.
CREATE TABLE IF NOT EXISTS ops.stream_replays (
    query_name  text        NOT NULL,
    batch_id    bigint      NOT NULL,
    detected_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Master-data change events from Debezium table CDC (ERP dealers, products, warehouses), kept raw.
CREATE TABLE IF NOT EXISTS bronze.master_data_changes (
    change_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_topic     text        NOT NULL,
    source_position  text        NOT NULL,         -- '<partition>:<offset>'
    record_key       text,
    op               text,                         -- c / u / d / r from the Debezium envelope
    source_ts        timestamptz,
    raw_record       text,                         -- null for tombstones
    ingested_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_master_change_position UNIQUE (source_topic, source_position)
);
