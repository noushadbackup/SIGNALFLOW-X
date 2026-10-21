"""File transport → Bronze. Each line is stored verbatim; re-ingesting a file is a no-op.

Idempotency key: (source_stream, source_position) with position '<file name>:<line number>'. The Kafka loader in
Phase 2 uses '<partition>:<offset>' under the same key.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg

from ..db import require_autocommit
from ..log import get_logger

SOURCE_STREAM = "file:arrivals"
log = get_logger(__name__)


@dataclass(frozen=True)
class IngestResult:
    file: str
    lines: int
    inserted: int

    @property
    def skipped(self) -> int:
        return self.lines - self.inserted


def _hints(line: str) -> tuple[Any, ...]:
    """Best-effort extraction for indexing; any failure becomes parse_error, never an exception."""
    try:
        wrapper = json.loads(line)
        if not isinstance(wrapper, dict) or not isinstance(wrapper.get("event"), dict):
            raise ValueError("record has no 'event' object")
        event = wrapper["event"]
        hint = {k: event.get(k) if isinstance(event.get(k), str) else None
                for k in ("event_id", "event_type", "schema_version", "event_time")}
        return (wrapper.get("arrival_time") if isinstance(wrapper.get("arrival_time"), str) else None,
                hint["event_id"], hint["event_type"], hint["schema_version"], hint["event_time"], None)
    except (ValueError, TypeError) as exc:  # json.JSONDecodeError is a ValueError
        return (None, None, None, None, None, f"{type(exc).__name__}: {exc}"[:500])


def ingest_file(conn: psycopg.Connection, path: Path) -> IngestResult:
    require_autocommit(conn)
    lines = path.read_text().splitlines()
    batch_id = f"{path.name}:{uuid.uuid4().hex[:12]}"
    with conn.transaction():
        conn.execute(
            """CREATE TEMP TABLE IF NOT EXISTS bronze_stage (
                   line_no int, arrival_time text, event_id text, event_type text, schema_version text,
                   event_time text, raw_record text, record_sha256 text, parse_error text
               ) ON COMMIT DELETE ROWS"""
        )
        with conn.cursor().copy(
            "COPY bronze_stage (line_no, arrival_time, event_id, event_type, schema_version, event_time, "
            "raw_record, record_sha256, parse_error) FROM STDIN"
        ) as cp:
            for i, line in enumerate(lines, start=1):
                arrival, event_id, event_type, version, event_time, err = _hints(line)
                cp.write_row((i, arrival, event_id, event_type, version, event_time, line,
                              hashlib.sha256(line.encode()).hexdigest(), err))
        # Hints that fail to cast are nulled rather than failing the load: Bronze must accept anything.
        cur = conn.execute(
            """INSERT INTO bronze.raw_events (source_stream, source_position, arrival_time, ingest_batch_id,
                   event_id, event_type, schema_version, event_time, raw_record, record_sha256, parse_error)
               SELECT %(stream)s, %(file)s || ':' || line_no,
                      CASE WHEN arrival_time ~ '^\\d{4}-\\d{2}-\\d{2}T[0-9:.]+Z$' THEN arrival_time::timestamptz END,
                      %(batch)s, event_id, event_type, schema_version,
                      CASE WHEN event_time ~ '^\\d{4}-\\d{2}-\\d{2}T[0-9:.]+Z$' THEN event_time::timestamptz END,
                      raw_record, record_sha256, parse_error
               FROM bronze_stage ORDER BY line_no
               ON CONFLICT (source_stream, source_position) DO NOTHING""",
            {"stream": SOURCE_STREAM, "file": path.name, "batch": batch_id},
        )
        inserted = cur.rowcount
    result = IngestResult(path.name, len(lines), inserted)
    log.debug("bronze ingest", extra={"file": path.name, "lines": len(lines), "inserted": inserted,
                                     "skipped_existing": result.skipped})
    return result


def arrival_files(arrivals_dir: Path) -> list[Path]:
    return sorted(arrivals_dir.glob("*.jsonl"))
