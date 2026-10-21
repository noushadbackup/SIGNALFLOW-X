"""Bronze → Silver micro-batch processor (lite profile).

Per micro-batch, in ONE transaction: read Bronze rows after the checkpoint, validate each against its contract,
dedupe by event_id, classify lateness against the stream clock at batch start, write Silver / quarantine / ops
evidence, advance stream state and the checkpoint. A crash anywhere rolls the whole batch back (ADR-0006).
"""

from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

import psycopg

from ..contracts import INVALID_TIMESTAMP, MAX_PRODUCER_CLOCK_SKEW, ContractRegistry, parse_utc
from ..db import require_autocommit
from ..log import get_logger
from .lateness import ALLOWED_LATENESS, LATE_BEYOND, business_date, classify, is_final

PROCESSOR = "silver.lite"
STREAMS = ("telecom", "mfg")
UNPARSEABLE = "UNPARSEABLE"
EVENT_ID_CONFLICT = "EVENT_ID_CONFLICT"
log = get_logger(__name__)


class InjectedFault(RuntimeError):
    """Raised by the test-only fault injection hook."""


class ProcessorBusy(RuntimeError):
    """Another processor instance holds the checkpoint lock."""


@dataclass
class BatchStats:
    from_bronze_id: int
    to_bronze_id: int
    read: int = 0
    accepted: int = 0
    duplicates: int = 0
    quarantined: Counter[str] = field(default_factory=Counter)
    arrival_classes: Counter[str] = field(default_factory=Counter)
    restated_dates: set[tuple[str, date]] = field(default_factory=set)


@dataclass
class _StreamClock:
    max_event_time: datetime | None
    batch_max: datetime | None = None


def event_hash(event: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(event, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def stream_of(event_type: str) -> str:
    return "telecom" if event_type.startswith("telecom.") else "mfg"


def process(conn: psycopg.Connection, registry: ContractRegistry, chunk_size: int = 5000,
            fail_after_rows: int | None = None) -> list[BatchStats]:
    """Process all pending Bronze rows in micro-batches of ``chunk_size``. Returns one BatchStats per batch."""
    require_autocommit(conn)
    if fail_after_rows is not None and os.environ.get("SIGNALFLOW_ALLOW_FAULT_INJECTION") != "1":
        raise PermissionError("fault injection requires SIGNALFLOW_ALLOW_FAULT_INJECTION=1")
    batches: list[BatchStats] = []
    rows_seen = 0
    while True:
        stats = _process_batch(conn, registry, chunk_size, fail_after_rows, rows_seen)
        if stats is None:
            return batches
        rows_seen += stats.read
        batches.append(stats)


def _process_batch(conn: psycopg.Connection, registry: ContractRegistry, chunk_size: int,
                   fail_after_rows: int | None, rows_seen: int) -> BatchStats | None:
    started = datetime.now(UTC)
    with conn.transaction():
        conn.execute("INSERT INTO ops.checkpoints (processor) VALUES (%s) ON CONFLICT DO NOTHING", (PROCESSOR,))
        try:
            row = conn.execute("SELECT last_bronze_id FROM ops.checkpoints WHERE processor = %s FOR UPDATE NOWAIT",
                               (PROCESSOR,)).fetchone()
        except psycopg.errors.LockNotAvailable as exc:
            raise ProcessorBusy("another silver processor is running") from exc
        assert row is not None
        checkpoint = int(row[0])
        rows = conn.execute(
            """SELECT bronze_id, raw_record, parse_error, arrival_time FROM bronze.raw_events
               WHERE bronze_id > %s ORDER BY bronze_id LIMIT %s""", (checkpoint, chunk_size)).fetchall()
        if not rows:
            return None
        stats = BatchStats(checkpoint, int(rows[-1][0]), read=len(rows))
        arrival_by_id = {r[0]: r[3] for r in rows}

        for s in STREAMS:
            conn.execute("INSERT INTO ops.stream_state (stream, allowed_lateness) VALUES (%s, %s) "
                         "ON CONFLICT DO NOTHING", (s, ALLOWED_LATENESS))
        clocks = {s: _StreamClock(m) for s, m in conn.execute("SELECT stream, max_event_time FROM ops.stream_state")}

        # Pass 1: parse + validate (no DB access).
        parsed: list[tuple[int, dict[str, Any] | None, str | None, list[str], str | None]] = []
        for bronze_id, raw, parse_error, arrival_time in rows:
            event, reason, details, contract_id = _validate(registry, raw, parse_error, arrival_time)
            parsed.append((bronze_id, event, reason, details, contract_id))

        candidate_ids = [e["event_id"] for _, e, reason, _, _ in parsed if e is not None and reason is None]
        existing: dict[str, tuple[str, int]] = {
            str(eid): (sha, bid) for eid, sha, bid in conn.execute(
                "SELECT event_id, event_sha256, bronze_id FROM silver.events WHERE event_id = ANY(%s::uuid[])",
                (candidate_ids,))
        }

        silver_rows: list[tuple[Any, ...]] = []
        quarantine_rows: list[tuple[Any, ...]] = []
        duplicate_rows: list[tuple[Any, ...]] = []
        late_rows: list[tuple[Any, ...]] = []
        restatements: Counter[tuple[str, date]] = Counter()

        for bronze_id, event, reason, details, contract_id in parsed:
            if reason is not None:
                quarantine_rows.append(_quarantine_row(bronze_id, event, reason, details, contract_id))
                stats.quarantined[reason] += 1
                continue
            assert event is not None
            sha = event_hash(event)
            eid = event["event_id"]
            if eid in existing:
                first_sha, first_bronze = existing[eid]
                if first_sha == sha:
                    duplicate_rows.append((bronze_id, eid, first_bronze))
                    stats.duplicates += 1
                else:
                    quarantine_rows.append(_quarantine_row(
                        bronze_id, event, EVENT_ID_CONFLICT,
                        [f"event_id already accepted from bronze_id {first_bronze} with a different payload"],
                        contract_id))
                    stats.quarantined[EVENT_ID_CONFLICT] += 1
                continue
            existing[eid] = (sha, bronze_id)

            stream = stream_of(event["event_type"])
            clock = clocks[stream]
            event_time = parse_utc(event["event_time"])
            arrival_class, lateness = classify(event_time, clock.max_event_time)
            stats.arrival_classes[arrival_class] += 1
            if clock.batch_max is None or event_time > clock.batch_max:
                clock.batch_max = event_time
            if arrival_class == LATE_BEYOND:
                assert clock.max_event_time is not None
                watermark = clock.max_event_time - ALLOWED_LATENESS
                bdate = business_date(event_time)
                late_rows.append((eid, stream, event_time, watermark, lateness, bdate))
                if is_final(bdate, watermark):
                    restatements[(stream, bdate)] += 1
            silver_rows.append((
                eid, event["event_type"], event["schema_version"], contract_id, stream, event["source_system"],
                event_time, parse_utc(event["produced_at"]), arrival_by_id[bronze_id],
                json.dumps(event["payload"], separators=(",", ":")), sha, arrival_class, lateness, bronze_id,
            ))

        with conn.cursor() as cur:
            if silver_rows:
                with cur.copy(
                    "COPY silver.events (event_id, event_type, schema_version, contract_id, stream, source_system, "
                    "event_time, produced_at, arrival_time, payload, event_sha256, arrival_class, lateness_seconds, "
                    "bronze_id) FROM STDIN") as cp:
                    for r in silver_rows:
                        cp.write_row(r)
            if quarantine_rows:
                cur.executemany(
                    """INSERT INTO silver.quarantine (bronze_id, event_id, event_type, schema_version, reason_code,
                           details, contract_id) VALUES (%s, %s, %s, %s, %s, %s, %s)""", quarantine_rows)
            if duplicate_rows:
                cur.executemany("INSERT INTO ops.duplicate_events (bronze_id, event_id, first_bronze_id) "
                                "VALUES (%s, %s, %s)", duplicate_rows)
            if late_rows:
                cur.executemany(
                    """INSERT INTO ops.late_events (event_id, stream, event_time, watermark_at_arrival,
                           lateness_seconds, business_date) VALUES (%s, %s, %s, %s, %s, %s)""", late_rows)
            if restatements:
                cur.executemany(
                    """INSERT INTO ops.kpi_restatements (stream, business_date, late_event_count) VALUES (%s, %s, %s)
                       ON CONFLICT (stream, business_date) DO UPDATE
                       SET late_event_count = ops.kpi_restatements.late_event_count + EXCLUDED.late_event_count,
                           last_detected_at = now()""",
                    [(s, d, n) for (s, d), n in restatements.items()])
                stats.restated_dates = set(restatements)

        if fail_after_rows is not None and rows_seen + len(rows) > fail_after_rows:
            raise InjectedFault(f"injected fault after writing batch {stats.from_bronze_id}..{stats.to_bronze_id}")

        for stream, clock in clocks.items():
            if clock.batch_max is None:
                continue
            new_max = max(clock.batch_max, clock.max_event_time) if clock.max_event_time else clock.batch_max
            conn.execute(
                """UPDATE ops.stream_state SET max_event_time = %s, watermark = %s - allowed_lateness,
                       updated_at = now() WHERE stream = %s""", (new_max, new_max, stream))
        conn.execute("UPDATE ops.checkpoints SET last_bronze_id = %s, updated_at = now() WHERE processor = %s",
                     (stats.to_bronze_id, PROCESSOR))
        stats.accepted = len(silver_rows)
        conn.execute(
            """INSERT INTO ops.processing_runs (processor, from_bronze_id, to_bronze_id, rows_read, rows_accepted,
                   rows_quarantined, rows_duplicate, started_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (PROCESSOR, stats.from_bronze_id, stats.to_bronze_id, stats.read, stats.accepted,
             sum(stats.quarantined.values()), stats.duplicates, started))
    log.info("silver batch committed", extra={
        "from_bronze_id": stats.from_bronze_id, "to_bronze_id": stats.to_bronze_id, "read": stats.read,
        "accepted": stats.accepted, "duplicates": stats.duplicates, "quarantined": dict(stats.quarantined),
        "arrival_classes": dict(stats.arrival_classes)})
    return stats


def _validate(registry: ContractRegistry, raw: str, parse_error: str | None, arrival_time: datetime | None
              ) -> tuple[dict[str, Any] | None, str | None, list[str], str | None]:
    if parse_error is not None:
        return None, UNPARSEABLE, [parse_error], None
    try:
        event = json.loads(raw)["event"]
    except (ValueError, KeyError, TypeError) as exc:
        return None, UNPARSEABLE, [f"{type(exc).__name__}: {exc}"], None
    result = registry.validate(event)
    if not result.ok:
        return (event if isinstance(event, dict) else None), result.reason_code, result.details, result.contract_id
    # A future event time would drag the stream clock forward and mislabel everything after it as late.
    if arrival_time is not None and parse_utc(event["event_time"]) > arrival_time + MAX_PRODUCER_CLOCK_SKEW:
        return event, INVALID_TIMESTAMP, ["event_time is in the future relative to arrival"], result.contract_id
    return event, None, [], result.contract_id


def _quarantine_row(bronze_id: int, event: dict[str, Any] | None, reason: str, details: list[str],
                    contract_id: str | None) -> tuple[Any, ...]:
    def text(key: str) -> str | None:
        value = event.get(key) if event else None
        return value if isinstance(value, str) else None

    return (bronze_id, text("event_id"), text("event_type"), text("schema_version"), reason,
            json.dumps(details), contract_id)


def pending_rows(conn: psycopg.Connection) -> int:
    row = conn.execute(
        """SELECT count(*) FROM bronze.raw_events
           WHERE bronze_id > coalesce((SELECT last_bronze_id FROM ops.checkpoints WHERE processor = %s), 0)""",
        (PROCESSOR,)).fetchone()
    return int(row[0]) if row else 0
