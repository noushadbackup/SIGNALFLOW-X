-- Simulated manufacturer ERP (separate database). The ERP writes its business row and an outbox row in ONE
-- transaction; Debezium streams the outbox from the WAL, so there is no dual write to Kafka (ADR-0003).
CREATE SCHEMA IF NOT EXISTS erp;

-- Current state of each business document (order, invoice, shipment, ...), keyed by the contract's business key.
CREATE TABLE IF NOT EXISTS erp.documents (
    doc_type      text        NOT NULL,     -- e.g. 'invoice'
    doc_id        text        NOT NULL,     -- value of the contract's x-business-key
    last_event_id uuid        NOT NULL,
    status        text,
    body          jsonb       NOT NULL,
    version       integer     NOT NULL DEFAULT 1,
    updated_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (doc_type, doc_id)
);

-- Debezium Outbox Event Router columns: id, aggregatetype (routes the topic), aggregateid (message key), type,
-- payload (message value, kept as text so the bytes the ERP wrote are exactly the bytes on the topic).
CREATE TABLE IF NOT EXISTS erp.outbox (
    id            uuid        PRIMARY KEY,
    aggregatetype text        NOT NULL,     -- '<event_type>.v<major>' or 'mfg.unrouted.v1'
    aggregateid   text        NOT NULL,     -- event_id: same key ⇒ same partition ⇒ per-event order kept
    type          text        NOT NULL,
    payload       text        NOT NULL,
    arrival_time  text        NOT NULL,     -- forwarded as a Kafka header for the future-timestamp check
    created_at    timestamptz NOT NULL DEFAULT now()
);

-- Debezium heartbeat target. The table is in the outbox publication but not in the connector's include list: writing
-- it every few seconds moves the replication slot forward even when the ERP is idle, so the slot never pins WAL
-- produced by the other databases in this cluster.
CREATE TABLE IF NOT EXISTS erp.cdc_heartbeat (id integer PRIMARY KEY, beat_at timestamptz NOT NULL);

-- Master data, captured with plain table CDC (full before-images on update).
CREATE TABLE IF NOT EXISTS erp.dealers (
    dealer_id    text PRIMARY KEY,
    dealer_name  text NOT NULL,
    region_code  text NOT NULL,
    tier         text NOT NULL,
    credit_days  integer NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS erp.products (
    sku              text PRIMARY KEY,
    product_name     text NOT NULL,
    category         text NOT NULL,
    dealer_price_bdt numeric(12, 2) NOT NULL,
    updated_at       timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS erp.warehouses (
    warehouse_id   text PRIMARY KEY,
    warehouse_name text NOT NULL,
    region_code    text NOT NULL,
    is_central     boolean NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE erp.dealers REPLICA IDENTITY FULL;
ALTER TABLE erp.products REPLICA IDENTITY FULL;
ALTER TABLE erp.warehouses REPLICA IDENTITY FULL;
