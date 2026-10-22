"""What is published to the lake, and how (ADR-0009).

Two modes, chosen per table by how the Postgres table changes:

* ``append`` for insert-only tables. The high-water mark is a column whose values are committed in increasing
  order (identity columns written by a single writer: the Bronze loader / Silver processor in lite, the
  advisory-locked streaming sink in the stream profile). The published mark is the max of that column in the
  Iceberg table itself, so rows and mark are committed together and a crash between "rows written" and "mark
  saved" cannot happen. Each append snapshot also records the mark in its summary, for audit.
* ``replace`` for small mutable tables (watermarks, restatement log, master data). The whole table is replaced in
  one snapshot, only when its content hash changed, so the snapshot history shows real changes only.

Every select lists its columns explicitly, with JSON as text and UUIDs as text: Iceberg has no jsonb type and
Spark has no uuid type, and an explicit list keeps the lake schema from drifting when Postgres gains a column.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class TableSpec:
    layer: str                      # bronze / silver / ops / ref (Iceberg namespace, before the prefix)
    name: str
    select: str                     # one row per Iceberg row, run against the source database
    mode: Literal["append", "replace"]
    hwm: str | None = None          # append: output column used as the high-water mark
    partition_days: str | None = None  # timestamp column partitioned by day(), or None
    sort_by: tuple[str, ...] = ()

    @property
    def key(self) -> str:
        return f"{self.layer}.{self.name}"


TABLES: tuple[TableSpec, ...] = (
    TableSpec("bronze", "raw_events", """
        SELECT bronze_id, source_stream, source_position, arrival_time, ingested_at, ingest_batch_id, event_id,
               event_type, schema_version, event_time, raw_record, record_sha256::text AS record_sha256, parse_error
        FROM bronze.raw_events""", "append", hwm="bronze_id", partition_days="ingested_at", sort_by=("bronze_id",)),
    TableSpec("bronze", "master_data_changes", """
        SELECT change_id, source_topic, source_position, record_key, op, source_ts, raw_record, ingested_at
        FROM bronze.master_data_changes""", "append", hwm="change_id", sort_by=("change_id",)),
    TableSpec("silver", "events", """
        SELECT event_id::text AS event_id, event_type, schema_version, contract_id, stream, source_system,
               event_time, produced_at, arrival_time, payload::text AS payload, event_sha256::text AS event_sha256,
               arrival_class, lateness_seconds, bronze_id, processed_at
        FROM silver.events""", "append", hwm="bronze_id", partition_days="event_time",
              sort_by=("event_type", "event_time")),
    TableSpec("silver", "quarantine", """
        SELECT bronze_id, event_id, event_type, schema_version, reason_code, details::text AS details, contract_id,
               quarantined_at
        FROM silver.quarantine""", "append", hwm="bronze_id", sort_by=("bronze_id",)),
    TableSpec("ops", "duplicate_events", """
        SELECT bronze_id, event_id::text AS event_id, first_bronze_id, detected_at
        FROM ops.duplicate_events""", "append", hwm="bronze_id", sort_by=("bronze_id",)),
    # late_events has no ordered id of its own; it is written in the same transaction as the Silver row, so the
    # Silver row's bronze_id orders it.
    TableSpec("ops", "late_events", """
        SELECT l.event_id::text AS event_id, l.stream, l.event_time, l.watermark_at_arrival, l.lateness_seconds,
               l.business_date, l.detected_at, e.bronze_id
        FROM ops.late_events l JOIN silver.events e ON e.event_id = l.event_id""", "append", hwm="bronze_id",
              sort_by=("bronze_id",)),
    TableSpec("ops", "processing_runs", """
        SELECT run_id, processor, from_bronze_id, to_bronze_id, rows_read, rows_accepted, rows_quarantined,
               rows_duplicate, started_at, finished_at
        FROM ops.processing_runs""", "append", hwm="run_id", sort_by=("run_id",)),
    TableSpec("ops", "stream_state", """
        SELECT stream, max_event_time, watermark, extract(epoch FROM allowed_lateness)::bigint
               AS allowed_lateness_seconds, updated_at
        FROM ops.stream_state""", "replace", sort_by=("stream",)),
    TableSpec("ops", "kpi_restatements", """
        SELECT stream, business_date, late_event_count, first_detected_at, last_detected_at
        FROM ops.kpi_restatements""", "replace", sort_by=("stream", "business_date")),
    TableSpec("ref", "regions", "SELECT region_code, region_name, demand_weight FROM ref.regions", "replace",
              sort_by=("region_code",)),
    TableSpec("ref", "packages", """
        SELECT package_code, package_name, category, price_bdt, validity_days, data_mb, voice_minutes
        FROM ref.packages""", "replace", sort_by=("package_code",)),
    TableSpec("ref", "products", "SELECT sku, product_name, category, dealer_price_bdt FROM ref.products",
              "replace", sort_by=("sku",)),
    TableSpec("ref", "factories", "SELECT factory_id, factory_name, region_code FROM ref.factories", "replace",
              sort_by=("factory_id",)),
    TableSpec("ref", "warehouses", "SELECT warehouse_id, warehouse_name, region_code, is_central FROM ref.warehouses",
              "replace", sort_by=("warehouse_id",)),
    TableSpec("ref", "dealers", "SELECT dealer_id, dealer_name, region_code, tier, credit_days FROM ref.dealers",
              "replace", sort_by=("dealer_id",)),
)

HWM_PROPERTY = "signalflow.hwm"
CONTENT_PROPERTY = "signalflow.content-md5"
SOURCE_PROPERTY = "signalflow.source-db"
SOURCE_INSTANCE_PROPERTY = "signalflow.source-instance"  # changes when the source database is recreated


def by_key(key: str) -> TableSpec:
    for t in TABLES:
        if t.key == key:
            return t
    raise KeyError(key)
