"""Postgres sink for the Spark streaming job. One Spark micro-batch = one Postgres transaction that writes Bronze,
Silver, quarantine, ops evidence, stream clocks and the (query, batch_id) commit marker together (ADR-0008).

Spark guarantees a re-executed batch has the same batch_id and the same offsets, so finding the marker means the
batch already committed and the replay is skipped. Bronze's unique (source_stream, source_position) key is a second
line of defence if the Spark checkpoint itself is lost."""

from __future__ import annotations

import json
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg

from ..contracts import parse_utc
from ..db import require_autocommit
from ..log import get_logger
from ..processing.silver import BatchStats, InjectedFault, ProcessorBusy, Validated, apply_validated, record_run

PROCESSOR = "silver.spark"
log = get_logger(__name__)


@dataclass(frozen=True)
class KafkaRecord:
    """A Kafka message after executor-side validation (see spark_silver.validate_partition)."""
    topic: str
    partition: int
    offset: int
    kafka_ts: datetime | None
    arrival_header: str | None
    value: str | None
    event_id: str | None
    event_type: str | None
    schema_version: str | None
    event_time: str | None
    reason: str | None
    details: str            # JSON list
    contract_id: str | None

    @property
    def position(self) -> str:
        return f"{self.partition}:{self.offset}"

    @property
    def arrival_time(self) -> datetime | None:
        if self.arrival_header:
            try:
                return parse_utc(self.arrival_header)
            except ValueError:
                pass
        return self.kafka_ts


@dataclass(frozen=True)
class CommitResult:
    batch_id: int
    replay: bool
    rows_received: int
    rows_new: int
    stats: BatchStats | None


def _fault(name: str) -> int | None:
    value = os.environ.get(name)
    if value is None:
        return None
    if os.environ.get("SIGNALFLOW_ALLOW_FAULT_INJECTION") != "1":
        raise PermissionError(f"{name} requires SIGNALFLOW_ALLOW_FAULT_INJECTION=1")
    return int(value)


def _offsets(records: list[KafkaRecord]) -> dict[str, dict[str, list[int]]]:
    out: dict[str, dict[str, list[int]]] = defaultdict(dict)
    for r in records:
        lo_hi = out[r.topic].setdefault(str(r.partition), [r.offset, r.offset])
        lo_hi[0], lo_hi[1] = min(lo_hi[0], r.offset), max(lo_hi[1], r.offset)
    return dict(out)


def _ts_or_none(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return parse_utc(value)
    except ValueError:
        return None


def commit_batch(conn: psycopg.Connection, query_name: str, batch_id: int,
                 records: list[KafkaRecord]) -> CommitResult:
    require_autocommit(conn)
    records = sorted(records, key=lambda r: (r.topic, r.partition, r.offset))  # per-key arrival order
    started = datetime.now(UTC)
    with conn.transaction():
        locked = conn.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s))",
                              (f"{PROCESSOR}:{query_name}",)).fetchone()
        if not locked or not locked[0]:
            raise ProcessorBusy(f"another {PROCESSOR} instance is writing query {query_name}")
        if conn.execute("SELECT 1 FROM ops.stream_commits WHERE query_name = %s AND batch_id = %s",
                        (query_name, batch_id)).fetchone():
            conn.execute("INSERT INTO ops.stream_replays (query_name, batch_id) VALUES (%s, %s)",
                         (query_name, batch_id))
            log.warning("replayed batch already committed; skipped", extra={"query": query_name, "batch": batch_id})
            return CommitResult(batch_id, True, len(records), 0, None)

        conn.execute(
            """CREATE TEMP TABLE IF NOT EXISTS kafka_stage (
                   ord int, source_stream text, source_position text, arrival_time timestamptz, event_id text,
                   event_type text, schema_version text, event_time timestamptz, raw_record text, parse_error text
               ) ON COMMIT DELETE ROWS""")
        with conn.cursor().copy(
                "COPY kafka_stage (ord, source_stream, source_position, arrival_time, event_id, event_type, "
                "schema_version, event_time, raw_record, parse_error) FROM STDIN") as cp:
            for i, r in enumerate(records):
                parse_error = json.loads(r.details)[0] if r.reason == "UNPARSEABLE" else None
                cp.write_row((i, f"kafka:{r.topic}", r.position, r.arrival_time, r.event_id, r.event_type,
                              r.schema_version, _ts_or_none(r.event_time), r.value or "", parse_error))
        inserted = conn.execute(
            """INSERT INTO bronze.raw_events (source_stream, source_position, arrival_time, ingest_batch_id, event_id,
                   event_type, schema_version, event_time, raw_record, record_sha256, parse_error)
               SELECT source_stream, source_position, arrival_time, %s, event_id, event_type, schema_version,
                      event_time, raw_record, encode(sha256(convert_to(raw_record, 'UTF8')), 'hex'), parse_error
               FROM kafka_stage ORDER BY ord
               ON CONFLICT (source_stream, source_position) DO NOTHING
               RETURNING bronze_id, source_stream, source_position""",
            (f"{query_name}:{batch_id}",)).fetchall()
        bronze_ids = {(stream, pos): bid for bid, stream, pos in inserted}

        validated: list[Validated] = []
        for r in records:
            bid = bronze_ids.get((f"kafka:{r.topic}", r.position))
            if bid is None:
                continue  # already in Bronze from an earlier commit (checkpoint loss): processed then
            event = None
            if r.reason != "UNPARSEABLE" and r.value is not None:
                parsed = json.loads(r.value)
                event = parsed if isinstance(parsed, dict) else None
            validated.append(Validated(bid, event, r.reason, json.loads(r.details), r.contract_id, r.arrival_time))

        stats: BatchStats | None = None
        if validated:
            ids = [v.bronze_id for v in validated]
            stats = BatchStats(min(ids) - 1, max(ids), read=len(validated))
            apply_validated(conn, validated, stats)
            record_run(conn, PROCESSOR, stats, started)

        if _fault("SIGNALFLOW_FAULT_FAIL_IN_BATCH") == batch_id:
            raise InjectedFault(f"injected fault inside batch {batch_id} (rolled back)")

        conn.execute(
            """INSERT INTO ops.stream_commits (query_name, batch_id, rows_received, rows_new, offsets)
               VALUES (%s, %s, %s, %s, %s)""",
            (query_name, batch_id, len(records), len(validated), json.dumps(_offsets(records))))
    result = CommitResult(batch_id, False, len(records), len(validated), stats)
    log.info("stream batch committed", extra={
        "query": query_name, "batch": batch_id, "received": len(records), "new": len(validated),
        "accepted": stats.accepted if stats else 0, "duplicates": stats.duplicates if stats else 0,
        "quarantined": dict(stats.quarantined) if stats else {}})
    if _fault("SIGNALFLOW_FAULT_EXIT_AFTER_BATCH") == batch_id:
        log.warning("fault injection: exiting after commit, before Spark records the batch", extra={"batch": batch_id})
        os._exit(70)
    return result


def commit_master_changes(conn: psycopg.Connection, rows: list[dict[str, Any]]) -> int:
    """Raw Debezium table-CDC events → bronze.master_data_changes. Idempotent by Kafka position."""
    require_autocommit(conn)
    if not rows:
        return 0
    with conn.transaction():
        cur = conn.cursor()
        cur.executemany(
            """INSERT INTO bronze.master_data_changes (source_topic, source_position, record_key, op, source_ts,
                   raw_record) VALUES (%(topic)s, %(position)s, %(key)s, %(op)s, %(source_ts)s, %(value)s)
               ON CONFLICT (source_topic, source_position) DO NOTHING""", rows)
        return len(rows)
