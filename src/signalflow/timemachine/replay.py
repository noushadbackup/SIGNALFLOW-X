"""Data Time Machine: replay an event-time window from Bronze, compare, promote, undo (ADR-0012).

Bronze is the immutable record of what arrived. A replay re-derives the Silver rows of one stream's event-time
window from it, with the current contracts and the same rules as the processors:

* validation through the processors' own functions (file wrapper or Kafka value, by Bronze source),
* first arrival wins per event_id, also against Silver rows outside the window,
* lateness against the stream clock at the start of the original batch, reconstructed from ``ops.processing_runs``
  (the max accepted event time before the batch), so arrival classes are reproduced, not re-guessed.

The result goes to ``ops.replay_events`` only. Nothing in Silver changes until an operator promotes it; promotion is
one transaction that checks the window has not changed since the preview, archives every row it replaces, removes
or inserts (Silver, quarantine, duplicates, late events), records KPI restatements for days that were already final,
and is audited. ``undo`` restores the archive exactly, after checking nobody changed the promoted rows since.

Not claimed: a replay cannot recover records Bronze never received, and Bronze records without an event time
(unparseable lines) belong to no window.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .. import CONTRACTS_DIR
from ..contracts import ContractRegistry, parse_utc
from ..log import get_logger
from ..processing.lateness import ALLOWED_LATENESS, LATE_BEYOND, business_date, classify, is_final
from ..processing.silver import (
    EVENT_ID_CONFLICT,
    UNPARSEABLE,
    _validate_file_record,
    event_hash,
    stream_of,
    validate_message,
)
from ..processing.silver import PROCESSOR as LITE_PROCESSOR
from ..security import Operator, audit, authorize
from ..streaming.sink import PROCESSOR as SPARK_PROCESSOR

log = get_logger(__name__)
MAX_WINDOW = timedelta(days=3)       # a preview must stay reviewable by a person
SILVER_COLUMNS = ("event_id", "event_type", "schema_version", "contract_id", "stream", "source_system", "event_time",
                  "produced_at", "arrival_time", "payload", "event_sha256", "arrival_class", "lateness_seconds",
                  "bronze_id")
COMPARED = SILVER_COLUMNS  # payload too: a faulty write can change it without touching event_sha256
SPARK_LOCK = f"{SPARK_PROCESSOR}:silver_events"  # the advisory lock the streaming sink takes per batch


class ReplayError(RuntimeError):
    """The replay cannot be computed or applied as asked (the message says why)."""


class ProcessorRunning(ReplayError):
    """A Silver processor holds its lock; promotion would race it."""


@dataclass
class Window:
    stream: str
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.stream not in ("telecom", "mfg"):
            raise ReplayError(f"unknown stream {self.stream!r}")
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ReplayError("window bounds must carry a time zone")
        if not self.start < self.end:
            raise ReplayError("window start must be before its end")
        if self.end - self.start > MAX_WINDOW:
            raise ReplayError(f"a replay window is at most {MAX_WINDOW}; split longer ranges")

    def params(self) -> dict[str, Any]:
        return {"stream": self.stream, "like": f"{self.stream}.%", "start": self.start, "end": self.end}


# --------------------------------------------------------------------------------------------------- inspect

def inspect_window(conn: psycopg.Connection, window: Window, sample: int = 25) -> dict[str, Any]:
    """What the window holds now: per event type in Bronze, Silver, quarantine and duplicates, plus raw samples."""
    p = window.params()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """WITH b AS (SELECT bronze_id, event_type FROM bronze.raw_events
                          WHERE event_type LIKE %(like)s AND event_time >= %(start)s AND event_time < %(end)s)
               SELECT b.event_type, count(*) AS bronze_rows,
                      count(*) FILTER (WHERE q.bronze_id IS NOT NULL) AS quarantined,
                      count(*) FILTER (WHERE d.bronze_id IS NOT NULL) AS duplicates
               FROM b LEFT JOIN silver.quarantine q USING (bronze_id) LEFT JOIN ops.duplicate_events d USING (bronze_id)
               GROUP BY 1 ORDER BY 1""", p)
        bronze = {r["event_type"]: r for r in cur.fetchall()}
        cur.execute(
            """SELECT event_type, count(*) AS silver_rows, count(*) FILTER (WHERE arrival_class <> 'ON_TIME') AS late,
                      sum((payload ->> 'amount_bdt')::numeric) AS amount_bdt
               FROM silver.events WHERE stream = %(stream)s AND event_time >= %(start)s AND event_time < %(end)s
               GROUP BY 1 ORDER BY 1""", p)
        silver = {r["event_type"]: r for r in cur.fetchall()}
        cur.execute(
            """SELECT b.bronze_id, b.source_stream, b.arrival_time, b.event_time, b.event_type, b.raw_record,
                      CASE WHEN s.event_id IS NOT NULL THEN 'SILVER' WHEN q.bronze_id IS NOT NULL THEN q.reason_code
                           WHEN d.bronze_id IS NOT NULL THEN 'DUPLICATE' ELSE 'NOT PROCESSED' END AS state
               FROM bronze.raw_events b LEFT JOIN silver.events s ON s.bronze_id = b.bronze_id
               LEFT JOIN silver.quarantine q ON q.bronze_id = b.bronze_id
               LEFT JOIN ops.duplicate_events d ON d.bronze_id = b.bronze_id
               WHERE b.event_type LIKE %(like)s AND b.event_time >= %(start)s AND b.event_time < %(end)s
               ORDER BY b.bronze_id LIMIT %(sample)s""", p | {"sample": sample})
        samples = cur.fetchall()
    types = sorted(set(bronze) | set(silver))
    return {
        "window": {"stream": window.stream, "start": window.start, "end": window.end},
        "event_types": [{"event_type": t,
                         "bronze_rows": bronze.get(t, {}).get("bronze_rows", 0),
                         "quarantined": bronze.get(t, {}).get("quarantined", 0),
                         "duplicates": bronze.get(t, {}).get("duplicates", 0),
                         "silver_rows": silver.get(t, {}).get("silver_rows", 0),
                         "late": silver.get(t, {}).get("late", 0),
                         "amount_bdt": silver.get(t, {}).get("amount_bdt")} for t in types],
        "samples": samples,
    }


# --------------------------------------------------------------------------------------------------- preview

@dataclass
class _Record:
    bronze_id: int
    outcome: str
    event_id: str | None = None
    row: dict[str, Any] | None = None
    reason: str | None = None
    details: list[str] = field(default_factory=list)
    contract_id: str | None = None
    event: dict[str, Any] | None = None
    first_bronze_id: int | None = None


def _validate(registry: ContractRegistry, source_stream: str, raw: str, parse_error: str | None,
              arrival: datetime | None) -> tuple[dict[str, Any] | None, str | None, list[str], str | None]:
    if source_stream.startswith("kafka:"):
        if parse_error is not None:
            return None, UNPARSEABLE, [parse_error], None
        return validate_message(registry, raw, arrival)
    return _validate_file_record(registry, raw, parse_error, arrival)


def _clock_at(conn: psycopg.Connection, stream: str, bronze_ids: list[int]) -> dict[int, datetime | None]:
    """Stream clock at the start of the batch that processed each Bronze row: the max event time of the accepted
    rows before that batch (quarantined rows never moved the clock; duplicates repeat an earlier event time)."""
    runs = conn.execute(
        """SELECT b.id, r.from_bronze_id
           FROM unnest(%s::bigint[]) AS b(id)
           LEFT JOIN LATERAL (SELECT from_bronze_id FROM ops.processing_runs
                              WHERE from_bronze_id < b.id AND to_bronze_id >= b.id
                              ORDER BY run_id DESC LIMIT 1) r ON true""", (bronze_ids,)).fetchall()
    missing = [b for b, f in runs if f is None]
    if missing:
        raise ReplayError(f"{len(missing)} Bronze rows in the window were never processed (first: {missing[0]}); "
                          "let the processor catch up before replaying")
    out: dict[int, datetime | None] = {}
    cache: dict[int, datetime | None] = {}
    for bid, start in runs:
        if start not in cache:
            row = conn.execute(
                """SELECT max(b.event_time) FROM bronze.raw_events b
                   WHERE b.bronze_id <= %s AND b.event_type LIKE %s
                     AND NOT EXISTS (SELECT 1 FROM silver.quarantine q WHERE q.bronze_id = b.bronze_id)""",
                (start, f"{stream}.%")).fetchone()
            cache[start] = row[0] if row else None
        out[bid] = cache[start]
    return out


def _silver_row(event: dict[str, Any], contract_id: str | None, arrival: datetime | None, bronze_id: int,
                clock: datetime | None) -> dict[str, Any]:
    event_time = parse_utc(event["event_time"])
    arrival_class, lateness = classify(event_time, clock)
    return {"event_id": event["event_id"], "event_type": event["event_type"],
            "schema_version": event["schema_version"], "contract_id": contract_id,
            "stream": stream_of(event["event_type"]), "source_system": event["source_system"],
            "event_time": event_time, "produced_at": parse_utc(event["produced_at"]), "arrival_time": arrival,
            "payload": event["payload"], "event_sha256": event_hash(event), "arrival_class": arrival_class,
            "lateness_seconds": lateness, "bronze_id": bronze_id,
            "_watermark": (clock - ALLOWED_LATENESS) if clock is not None else None}


def _same(a: dict[str, Any], b: dict[str, Any]) -> bool:
    return all(_norm(a[c]) == _norm(b[c]) for c in COMPARED)


def _payload_digest(payload: Any) -> str:
    return hashlib.md5(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def _norm(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.astimezone(UTC)
    if isinstance(v, uuid.UUID):
        return str(v)
    return v


def fingerprint(rows: dict[str, dict[str, Any] | None]) -> str:
    """Order-independent digest of Silver rows by event id (None = absent)."""
    parts = []
    for eid in sorted(rows):
        r = rows[eid]
        parts.append(f"{eid}:-" if r is None else
                     f"{eid}:{r['event_sha256']}:{_payload_digest(r['payload'])}:{r['arrival_class']}:"
                     f"{r['lateness_seconds']}:{r['bronze_id']}:{_norm_json(r['event_time'])}")
    return hashlib.md5(",".join(parts).encode()).hexdigest()


def _current_rows(conn: psycopg.Connection, event_ids: list[str]) -> dict[str, dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(f"SELECT {', '.join(SILVER_COLUMNS)}, processed_at FROM silver.events "
                    "WHERE event_id = ANY(%s::uuid[])", (event_ids,))
        return {str(r["event_id"]): r | {"event_id": str(r["event_id"]), "event_sha256": r["event_sha256"].strip()}
                for r in cur.fetchall()}


def _amount(row: dict[str, Any]) -> Decimal:
    value = row["payload"].get("amount_bdt") if isinstance(row["payload"], dict) else None
    try:
        return Decimal(str(value)) if value is not None else Decimal(0)
    except ArithmeticError:
        return Decimal(0)


def _metrics(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = defaultdict(lambda: {"events": 0, "amount_bdt": Decimal(0)})
    for r in rows:
        key = f"{business_date(r['event_time'])}|{r['event_type']}"
        out[key]["events"] += 1
        out[key]["amount_bdt"] += _amount(r)
    return dict(out)


def _jsonable(v: Any) -> Any:
    return json.loads(json.dumps(v, default=str))


def preview(conn: psycopg.Connection, operator: Operator | None, window: Window,
            registry: ContractRegistry | None = None) -> dict[str, Any]:
    """Replay the window into ``ops.replay_events`` and return the preview summary. Silver is not modified."""
    replay_id = f"REPLAY-{window.stream}-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:4]}"
    operator = authorize(conn, operator, "replay.preview", "replay_window",
                         f"{window.stream} {window.start.isoformat()}..{window.end.isoformat()}")
    registry = registry or ContractRegistry.load(CONTRACTS_DIR)
    p = window.params()
    rows = conn.execute(
        """SELECT bronze_id, source_stream, raw_record, parse_error, arrival_time FROM bronze.raw_events
           WHERE (event_type LIKE %(like)s AND event_time >= %(start)s AND event_time < %(end)s)
              OR bronze_id IN (SELECT bronze_id FROM silver.events
                               WHERE stream = %(stream)s AND event_time >= %(start)s AND event_time < %(end)s)
           ORDER BY bronze_id""", p).fetchall()
    if not rows:
        raise ReplayError("the window holds no Bronze records")
    clocks = _clock_at(conn, window.stream, [r[0] for r in rows])
    candidate_ids = {r[0] for r in rows}
    arrivals = {r[0]: r[4] for r in rows}

    records: list[_Record] = []
    for bronze_id, source_stream, raw, parse_error, arrival in rows:
        event, reason, details, contract_id = _validate(registry, source_stream, raw, parse_error, arrival)
        eid = event.get("event_id") if isinstance(event, dict) and isinstance(event.get("event_id"), str) else None
        records.append(_Record(bronze_id, "QUARANTINED" if reason else "ACCEPTED", eid, reason=reason,
                               details=details, contract_id=contract_id, event=event))

    # First arrival wins, also against an accepted Silver row of the same event id outside the window.
    accepted_ids = sorted({r.event_id for r in records if r.outcome == "ACCEPTED" and r.event_id})
    outside = {str(e): (sha.strip(), bid) for e, sha, bid in conn.execute(
        "SELECT event_id, event_sha256, bronze_id FROM silver.events WHERE event_id = ANY(%s::uuid[])",
        (accepted_ids,)) if bid not in candidate_ids}
    first: dict[str, tuple[str, int]] = dict(outside)
    for rec in records:
        if rec.outcome != "ACCEPTED":
            continue
        assert rec.event is not None and rec.event_id is not None
        sha = event_hash(rec.event)
        seen = first.get(rec.event_id)
        if seen is not None and seen[1] < rec.bronze_id:
            if seen[0] == sha:
                rec.outcome, rec.first_bronze_id = "DUPLICATE", seen[1]
            else:
                rec.outcome, rec.reason = "QUARANTINED", EVENT_ID_CONFLICT
                rec.details = [f"event_id already accepted from bronze_id {seen[1]} with a different payload"]
            continue
        first[rec.event_id] = (sha, rec.bronze_id)
        rec.row = _silver_row(rec.event, rec.contract_id, arrivals[rec.bronze_id], rec.bronze_id,
                              clocks[rec.bronze_id])

    replayed = {r.event_id: r.row for r in records if r.row is not None and r.event_id}
    window_ids = [str(e) for (e,) in conn.execute(
        """SELECT event_id FROM silver.events
           WHERE stream = %(stream)s AND event_time >= %(start)s AND event_time < %(end)s""", p)]
    affected = sorted(set(window_ids) | set(replayed))
    current = _current_rows(conn, affected)

    changes: Counter[str] = Counter()
    events_rows: list[tuple[Any, ...]] = []
    explained: set[str] = set()
    for rec in records:
        change, current_row = "NONE", None
        if rec.row is not None and rec.event_id:
            cur_row = current.get(rec.event_id)
            change = "INSERT" if cur_row is None else ("UNCHANGED" if _same(rec.row, cur_row) else "CHANGED")
            current_row = cur_row if change == "CHANGED" else None
            explained.add(rec.event_id)
        changes[change] += 1
        events_rows.append((replay_id, rec.bronze_id, rec.event_id, rec.outcome, change,
                            Jsonb(_jsonable(rec.row)) if rec.row else None,
                            Jsonb(_jsonable(current_row)) if current_row else None, rec.reason))
    # Silver rows the replay does not reproduce (their record is now quarantined or a duplicate): removed.
    by_bronze = {r.bronze_id: i for i, r in enumerate(records)}
    for eid in affected:
        cur_row = current.get(eid)
        if cur_row is None or eid in explained:
            continue
        i = by_bronze[cur_row["bronze_id"]]
        r = events_rows[i]
        events_rows[i] = r[:4] + ("REMOVE", r[5], Jsonb(_jsonable(cur_row)), r[7])
        changes["REMOVE"] += 1
        changes[r[4]] -= 1

    outcomes = Counter(r.outcome for r in records)
    current_list = [current[e] for e in affected if e in current and _in_window(current[e], window)]
    replay_list = [r for r in replayed.values() if r is not None and _in_window(r, window)]
    before, after = _metrics(current_list), _metrics(replay_list)
    metrics_diff = [{"key": k, "business_date": k.split("|")[0], "event_type": k.split("|")[1],
                     "events_current": before.get(k, {}).get("events", 0),
                     "events_replay": after.get(k, {}).get("events", 0),
                     "amount_current": str(before.get(k, {}).get("amount_bdt", Decimal(0))),
                     "amount_replay": str(after.get(k, {}).get("amount_bdt", Decimal(0)))}
                    for k in sorted(set(before) | set(after))]
    for m in metrics_diff:
        m["changed"] = m["events_current"] != m["events_replay"] or m["amount_current"] != m["amount_replay"]

    silver_fp = fingerprint({e: current.get(e) for e in affected})
    replay_fp = fingerprint({e: replayed.get(e) for e in affected})
    verification = _preview_checks(conn, records, outcomes, rows)
    dates = sorted({m["business_date"] for m in metrics_diff if m["changed"]})
    gold_before = gold_snapshot(conn, window.stream, dates)
    summary = {"records": len(records), "outcomes": dict(outcomes),
               "changes": {k: v for k, v in changes.items() if v}, "affected_event_ids": len(affected),
               "changed_business_dates": dates,
               "event_types_changed": sorted({m["event_type"] for m in metrics_diff if m["changed"]}),
               "quarantine_reasons": dict(Counter(r.reason for r in records if r.reason))}
    with conn.transaction():
        conn.execute(
            """INSERT INTO ops.replay_runs (replay_id, stream, window_start, window_end, requested_by, status,
                   bronze_rows, summary, metrics_diff, verification, gold_before, silver_fingerprint,
                   replay_fingerprint)
               VALUES (%s, %s, %s, %s, %s, 'PREVIEWED', %s, %s, %s, %s, %s, %s, %s)""",
            (replay_id, window.stream, window.start, window.end, operator.username, len(records), Jsonb(summary),
             Jsonb(metrics_diff), Jsonb(verification), Jsonb(gold_before), silver_fp, replay_fp))
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO ops.replay_events (replay_id, bronze_id, event_id, outcome, change, row, current_row,
                       reason) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", events_rows)
        audit(conn, operator.username, operator.role, "replay.preview", "OK", "replay", replay_id,
              {"window": [window.start.isoformat(), window.end.isoformat()], "stream": window.stream,
               "changes": summary["changes"]})
    log.info("replay previewed", extra={"replay_id": replay_id, "changes": summary["changes"]})
    return get(conn, replay_id)


def _in_window(row: dict[str, Any], window: Window) -> bool:
    t = row["event_time"]
    return bool(window.start <= t < window.end)


def _preview_checks(conn: psycopg.Connection, records: list[_Record], outcomes: Counter[str],
                    rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    ids = [r.bronze_id for r in records]
    dup_now = {b for (b,) in conn.execute("SELECT bronze_id FROM ops.duplicate_events WHERE bronze_id = ANY(%s)",
                                          (ids,))}
    q_now = {b for (b,) in conn.execute("SELECT bronze_id FROM silver.quarantine WHERE bronze_id = ANY(%s)", (ids,))}
    dup_replay = {r.bronze_id for r in records if r.outcome == "DUPLICATE"}
    q_replay = {r.bronze_id for r in records if r.outcome == "QUARANTINED"}
    return [
        _check("Every replayed record has exactly one outcome (accepted + duplicate + quarantined = records)",
               len(rows), sum(outcomes.values())),
        _check("Duplicates found by the replay match the recorded duplicates", sorted(dup_now), sorted(dup_replay)),
        _check("Records the replay quarantines match the current quarantine", sorted(q_now), sorted(q_replay)),
    ]


def _check(name: str, expected: Any, actual: Any) -> dict[str, Any]:
    return {"name": name, "expected": _jsonable(expected), "actual": _jsonable(actual),
            "passed": _jsonable(expected) == _jsonable(actual)}


# --------------------------------------------------------------------------------------------------- gold

GOLD_METRICS: dict[str, list[tuple[str, str]]] = {
    "telecom": [
        ("revenue", """SELECT business_date::text, sum(gross_recharge_bdt), sum(successful_recharges),
                              sum(recharge_attempts), sum(billed_bdt), sum(successful_without_billing)
                       FROM gold.mart_telecom_revenue_daily WHERE business_date = ANY(%s::date[]) GROUP BY 1"""),
        ("usage", """SELECT business_date::text, sum(rated_bdt), sum(usage_records)
                     FROM gold.mart_telecom_usage_daily WHERE business_date = ANY(%s::date[]) GROUP BY 1"""),
    ],
    "mfg": [
        ("sales", """SELECT business_date::text, sum(ordered_value_bdt), sum(orders)
                     FROM gold.mart_mfg_sales_daily WHERE business_date = ANY(%s::date[]) GROUP BY 1"""),
        ("stock", """SELECT 'all', sum(on_hand_qty), count(*) FILTER (WHERE position_status = 'UNCERTAIN')
                     FROM gold.mart_inventory_position WHERE %s::date[] IS NOT NULL"""),
    ],
}
GOLD_LABELS = {"revenue": ("gross_recharge_bdt", "successful_recharges", "recharge_attempts", "billed_bdt",
                           "successful_without_billing"),
               "usage": ("rated_bdt", "usage_records"), "sales": ("ordered_value_bdt", "orders"),
               "stock": ("on_hand_qty", "uncertain_positions")}


def gold_snapshot(conn: psycopg.Connection, stream: str, dates: list[str]) -> dict[str, str]:
    """Gold figures for the given business dates, as text keyed ``metric|date``. Missing marts are skipped."""
    out: dict[str, str] = {}
    for name, query in GOLD_METRICS[stream]:
        try:
            with conn.transaction():
                fetched = conn.execute(query, ([date.fromisoformat(d) for d in dates],)).fetchall()
        except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn):
            continue  # mart not built (yet) in this database
        for row in fetched:
            for label, value in zip(GOLD_LABELS[name], row[1:], strict=True):
                out[f"{label}|{row[0]}"] = str(value)
    return out


def gold_diff(before: dict[str, str], after: dict[str, str]) -> list[dict[str, str]]:
    return [{"metric": k.split("|")[0], "business_date": k.split("|")[1], "before": before.get(k, "-"),
             "after": after.get(k, "-")} for k in sorted(set(before) | set(after)) if before.get(k) != after.get(k)]


# --------------------------------------------------------------------------------------------------- promote

def get(conn: psycopg.Connection, replay_id: str) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM ops.replay_runs WHERE replay_id = %s", (replay_id,))
        run = cur.fetchone()
    if run is None:
        raise ReplayError(f"unknown replay {replay_id}")
    return dict(run)


def _lock_processors(conn: psycopg.Connection) -> None:
    try:
        conn.execute("SELECT 1 FROM ops.checkpoints WHERE processor = %s FOR UPDATE NOWAIT", (LITE_PROCESSOR,))
    except psycopg.errors.LockNotAvailable as exc:
        raise ProcessorRunning("the lite Silver processor is running; promote when it is idle") from exc
    row = conn.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s))", (SPARK_LOCK,)).fetchone()
    if not row or not row[0]:
        raise ProcessorRunning("the streaming sink is committing a batch; promote when it is idle")


def _replay_rows(conn: psycopg.Connection, replay_id: str) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM ops.replay_events WHERE replay_id = %s ORDER BY bronze_id", (replay_id,))
        return cur.fetchall()


def _affected_ids(events: list[dict[str, Any]]) -> list[str]:
    ids = {str(e["event_id"]) for e in events if e["event_id"] is not None and e["row"] is not None}
    ids |= {str(e["current_row"]["event_id"]) for e in events if e["current_row"]}
    return sorted(ids)


def _archive(conn: psycopg.Connection, replay_id: str, target: str, key: str, action: str,
             row: dict[str, Any] | None) -> None:
    conn.execute("""INSERT INTO ops.replay_archive (replay_id, target, key, action, row) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (replay_id, target, key) DO NOTHING""",
                 (replay_id, target, key, action, Jsonb(row) if row is not None else None))


def _archive_row(conn: psycopg.Connection, replay_id: str, target: str, key: str, action: str) -> None:
    """Archive the row inside the database: a round trip through Python would turn jsonb 300.00 into 300.0."""
    col = TARGET_KEYS[target]
    conn.execute(f"""INSERT INTO ops.replay_archive (replay_id, target, key, action, row)
                     SELECT %s, %s, %s, %s, to_jsonb(t) FROM {target} t WHERE t.{col}::text = %s
                     ON CONFLICT (replay_id, target, key) DO NOTHING""", (replay_id, target, key, action, key))


TARGET_KEYS = {"silver.events": "event_id", "silver.quarantine": "bronze_id", "ops.duplicate_events": "bronze_id",
               "ops.late_events": "event_id"}


def _row_json(conn: psycopg.Connection, target: str, key: str) -> dict[str, Any] | None:
    col = TARGET_KEYS[target]
    row = conn.execute(f"SELECT to_jsonb(t) FROM {target} t WHERE t.{col}::text = %s", (key,)).fetchone()
    return row[0] if row else None


def _rows_json(conn: psycopg.Connection, target: str, keys: list[str]) -> dict[str, dict[str, Any]]:
    col = TARGET_KEYS[target]
    return {k: r for k, r in conn.execute(
        f"SELECT t.{col}::text, to_jsonb(t) FROM {target} t WHERE t.{col}::text = ANY(%s)", (keys,))}


def _put(conn: psycopg.Connection, replay_id: str, target: str, key: str, desired: dict[str, Any] | None,
         insert_sql: str, existing: dict[str, Any] | None) -> bool:
    """Make ``target``'s row for ``key`` equal ``desired`` (None = absent), archiving what was there."""
    col = TARGET_KEYS[target]
    if desired is None:
        if existing is None:
            return False
        _archive_row(conn, replay_id, target, key, "REMOVED")
        conn.execute(f"DELETE FROM {target} WHERE {col}::text = %s", (key,))
        return True
    if existing is not None:
        _archive_row(conn, replay_id, target, key, "REPLACED")
        conn.execute(f"DELETE FROM {target} WHERE {col}::text = %s", (key,))
    else:
        _archive(conn, replay_id, target, key, "INSERTED", None)
    conn.execute(insert_sql, desired)
    return True


SILVER_INSERT = """INSERT INTO silver.events (event_id, event_type, schema_version, contract_id, stream, source_system,
                       event_time, produced_at, arrival_time, payload, event_sha256, arrival_class, lateness_seconds,
                       bronze_id)
                   VALUES (%(event_id)s, %(event_type)s, %(schema_version)s, %(contract_id)s, %(stream)s,
                           %(source_system)s, %(event_time)s, %(produced_at)s, %(arrival_time)s, %(payload)s,
                           %(event_sha256)s, %(arrival_class)s, %(lateness_seconds)s, %(bronze_id)s)"""
QUARANTINE_INSERT = """INSERT INTO silver.quarantine (bronze_id, event_id, event_type, schema_version, reason_code,
                           details, contract_id)
                       VALUES (%(bronze_id)s, %(event_id)s, %(event_type)s, %(schema_version)s, %(reason_code)s,
                               %(details)s, %(contract_id)s)"""
DUPLICATE_INSERT = """INSERT INTO ops.duplicate_events (bronze_id, event_id, first_bronze_id)
                      VALUES (%(bronze_id)s, %(event_id)s, %(first_bronze_id)s)"""
LATE_INSERT = """INSERT INTO ops.late_events (event_id, stream, event_time, watermark_at_arrival, lateness_seconds,
                     business_date)
                 VALUES (%(event_id)s, %(stream)s, %(event_time)s, %(watermark)s, %(lateness_seconds)s,
                         %(business_date)s)"""


def _desired(conn: psycopg.Connection, events: list[dict[str, Any]]) -> dict[str, dict[str, dict[str, Any] | None]]:
    """The state each target table must have for the replayed keys after promotion."""
    silver: dict[str, dict[str, Any] | None] = {e: None for e in _affected_ids(events)}
    quarantine: dict[str, dict[str, Any] | None] = {}
    duplicates: dict[str, dict[str, Any] | None] = {}
    late: dict[str, dict[str, Any] | None] = {}
    first_by_event = {str(e["event_id"]): e["bronze_id"] for e in events if e["outcome"] == "ACCEPTED"}
    quarantined = [e["bronze_id"] for e in events if e["outcome"] == "QUARANTINED"]
    raw: dict[int, str] = dict(conn.execute(
        "SELECT bronze_id, raw_record FROM bronze.raw_events WHERE bronze_id = ANY(%s)", (quarantined,)).fetchall())
    for e in events:
        key = str(e["bronze_id"])
        quarantine[key] = None
        duplicates[key] = None
        if e["outcome"] == "ACCEPTED":
            row = dict(e["row"])
            row["payload"] = Jsonb(row["payload"])
            silver[str(e["event_id"])] = row
            if row["arrival_class"] == LATE_BEYOND:
                event_time = _ts(row["event_time"])
                late[str(e["event_id"])] = {
                    "event_id": row["event_id"], "stream": row["stream"], "event_time": event_time,
                    "watermark": _ts(row["_watermark"]), "lateness_seconds": row["lateness_seconds"],
                    "business_date": business_date(event_time)}
            else:
                late[str(e["event_id"])] = None
        elif e["outcome"] == "DUPLICATE":
            duplicates[key] = {"bronze_id": e["bronze_id"], "event_id": str(e["event_id"]),
                               "first_bronze_id": first_by_event.get(str(e["event_id"]),
                                                                    _outside_first(conn, str(e["event_id"])))}
        else:
            quarantine[key] = _quarantine_desired(e, raw.get(e["bronze_id"]))
    for eid in silver:
        late.setdefault(eid, None)
    return {"silver.events": silver, "silver.quarantine": quarantine, "ops.duplicate_events": duplicates,
            "ops.late_events": late}


def _ts(v: Any) -> datetime:
    if isinstance(v, datetime):
        return v
    return datetime.fromisoformat(str(v))


def _outside_first(conn: psycopg.Connection, event_id: str) -> int:
    row = conn.execute("SELECT bronze_id FROM silver.events WHERE event_id = %s", (event_id,)).fetchone()
    if row is None:
        raise ReplayError(f"duplicate of {event_id} has no first arrival in Silver")
    return int(row[0])


def _quarantine_desired(e: dict[str, Any], raw: str | None) -> dict[str, Any]:
    event: dict[str, Any] = {}
    try:
        parsed = json.loads(raw) if raw else None
        if isinstance(parsed, dict):
            event = parsed.get("event", parsed) if isinstance(parsed.get("event", parsed), dict) else {}
    except ValueError:
        pass

    def text(k: str) -> str | None:
        v = event.get(k)
        return v if isinstance(v, str) else None

    return {"bronze_id": e["bronze_id"], "event_id": text("event_id"), "event_type": text("event_type"),
            "schema_version": text("schema_version"), "reason_code": e["reason"],
            "details": Jsonb([f"replay {e['replay_id']}: {e['reason']}"]), "contract_id": None}


def _equal(target: str, existing: dict[str, Any] | None, desired: dict[str, Any] | None) -> bool:
    if existing is None or desired is None:
        return existing is None and desired is None
    if target == "silver.events":
        want = dict(desired, payload=desired["payload"].obj if isinstance(desired["payload"], Jsonb)
                    else desired["payload"])
        return all(str(_norm_json(existing[c])) == str(_norm_json(want[c])) for c in COMPARED if c != "payload") \
            and _payload_digest(existing["payload"]) == _payload_digest(want["payload"])
    if target == "silver.quarantine":
        return bool(existing["reason_code"] == desired["reason_code"])
    if target == "ops.duplicate_events":
        return bool(existing["first_bronze_id"] == desired["first_bronze_id"])
    return bool(existing["lateness_seconds"] == desired["lateness_seconds"])


def _norm_json(v: Any) -> Any:
    """Compare JSON-stored and database values alike: timestamps as UTC ISO text, char(n) without padding."""
    if isinstance(v, str):
        if len(v) >= 19 and v[4] == "-" and v[10] in "T ":
            try:
                return datetime.fromisoformat(v).astimezone(UTC).isoformat()
            except ValueError:
                pass
        return v.strip()
    if isinstance(v, datetime):
        return v.astimezone(UTC).isoformat()
    return v


def _restate(conn: psycopg.Connection, stream: str, days: Counter[date]) -> list[str]:
    row = conn.execute("SELECT watermark FROM ops.stream_state WHERE stream = %s", (stream,)).fetchone()
    watermark = row[0] if row else None
    restated = [(d, n) for d, n in sorted(days.items()) if is_final(d, watermark)]
    for d, n in restated:
        conn.execute(
            """INSERT INTO ops.kpi_restatements (stream, business_date, late_event_count) VALUES (%s, %s, %s)
               ON CONFLICT (stream, business_date) DO UPDATE
               SET late_event_count = ops.kpi_restatements.late_event_count + EXCLUDED.late_event_count,
                   last_detected_at = now()""", (stream, d, n))
    return [str(d) for d, _ in restated]


def promote(conn: psycopg.Connection, operator: Operator | None, replay_id: str) -> dict[str, Any]:
    """Apply a previewed replay to Silver in one transaction. Rebuild Gold afterwards (``rebuild_gold``)."""
    operator = authorize(conn, operator, "replay.promote", "replay", replay_id)
    with conn.transaction():
        run = conn.execute("SELECT status FROM ops.replay_runs WHERE replay_id = %s FOR UPDATE",
                           (replay_id,)).fetchone()
        if run is None or run[0] != "PREVIEWED":
            raise ReplayError(f"{replay_id} is {run[0] if run else 'unknown'}; only a PREVIEWED replay can be promoted")
        run_d = get(conn, replay_id)
        _lock_processors(conn)
        events = _replay_rows(conn, replay_id)
        ids = _affected_ids(events)
        if fingerprint(_current_rows_all(conn, ids)) != run_d["silver_fingerprint"]:
            raise ReplayError("Silver changed in this window since the preview; preview again")
        desired = _desired(conn, events)
        applied: Counter[str] = Counter()
        days: Counter[date] = Counter()
        sql_for = {"silver.events": SILVER_INSERT, "silver.quarantine": QUARANTINE_INSERT,
                   "ops.duplicate_events": DUPLICATE_INSERT, "ops.late_events": LATE_INSERT}
        for target in ("ops.late_events", "ops.duplicate_events", "silver.quarantine", "silver.events"):
            current_rows = _rows_json(conn, target, list(desired[target]))
            for key, want in desired[target].items():
                existing = current_rows.get(key)
                if _equal(target, existing, want):
                    continue
                _put(conn, replay_id, target, key, want, sql_for[target], existing)
                applied[target] += 1
                if target == "silver.events":
                    for d in {business_date(_ts(r["event_time"])) for r in (existing, want) if r is not None}:
                        days[d] += 1
        restated = _restate(conn, run_d["stream"], days)
        replay_fp = fingerprint(_current_rows_all(conn, ids))
        conn.execute("""UPDATE ops.replay_runs SET status = 'PROMOTED', promoted_at = clock_timestamp(),
                        promoted_by = %s, summary = summary || %s WHERE replay_id = %s""",
                     (operator.username, Jsonb({"applied": dict(applied), "restated_business_dates": restated}),
                      replay_id))
        if replay_fp != run_d["replay_fingerprint"]:
            raise ReplayError("promoted Silver does not match the preview; rolled back")
        audit(conn, operator.username, operator.role, "replay.promote", "OK", "replay", replay_id,
              {"applied": dict(applied), "restated_business_dates": restated})
    log.info("replay promoted", extra={"replay_id": replay_id, "applied": dict(applied)})
    return get(conn, replay_id)


def _current_rows_all(conn: psycopg.Connection, ids: list[str]) -> dict[str, dict[str, Any] | None]:
    cur = _current_rows(conn, ids)
    return {e: cur.get(e) for e in ids}


def undo(conn: psycopg.Connection, operator: Operator | None, replay_id: str) -> dict[str, Any]:
    """Restore every row a promotion archived, if the promoted rows are still as the promotion left them."""
    operator = authorize(conn, operator, "replay.undo", "replay", replay_id)
    with conn.transaction():
        run = conn.execute("SELECT status FROM ops.replay_runs WHERE replay_id = %s FOR UPDATE",
                           (replay_id,)).fetchone()
        if run is None or run[0] != "PROMOTED":
            raise ReplayError(f"{replay_id} is {run[0] if run else 'unknown'}; only a PROMOTED replay can be undone")
        run_d = get(conn, replay_id)
        _lock_processors(conn)
        ids = _affected_ids(_replay_rows(conn, replay_id))
        if fingerprint(_current_rows_all(conn, ids)) != run_d["replay_fingerprint"]:
            raise ReplayError("Silver changed since the promotion; undo would overwrite newer data")
        archived = conn.execute("""SELECT target, key, action, row ->> 'event_time' FROM ops.replay_archive
                                   WHERE replay_id = %s""", (replay_id,)).fetchall()
        days: Counter[date] = Counter()
        for target, key, action, old_time in archived:
            col = TARGET_KEYS[target]
            current = _row_json(conn, target, key)
            conn.execute(f"DELETE FROM {target} WHERE {col}::text = %s", (key,))
            if action in ("REPLACED", "REMOVED"):  # restored inside the database, byte for byte
                conn.execute(f"""INSERT INTO {target}
                                 SELECT (jsonb_populate_record(null::{target}, a.row)).* FROM ops.replay_archive a
                                 WHERE a.replay_id = %s AND a.target = %s AND a.key = %s""", (replay_id, target, key))
            if target == "silver.events":
                times = [t for t in (current["event_time"] if current else None, old_time) if t is not None]
                for d in {business_date(_ts(t)) for t in times}:
                    days[d] += 1
        restated = _restate(conn, run_d["stream"], days)
        if fingerprint(_current_rows_all(conn, ids)) != run_d["silver_fingerprint"]:
            raise ReplayError("undo did not restore the pre-promotion rows; rolled back")
        conn.execute("""UPDATE ops.replay_runs SET status = 'UNDONE', undone_at = clock_timestamp(), undone_by = %s,
                        summary = summary || %s WHERE replay_id = %s""",
                     (operator.username, Jsonb({"undo_restated_business_dates": restated}), replay_id))
        audit(conn, operator.username, operator.role, "replay.undo", "OK", "replay", replay_id,
              {"rows_restored": len(archived)})
    return get(conn, replay_id)


def discard(conn: psycopg.Connection, operator: Operator | None, replay_id: str) -> dict[str, Any]:
    operator = authorize(conn, operator, "replay.preview", "replay", replay_id)
    with conn.transaction():
        n = conn.execute("""UPDATE ops.replay_runs SET status = 'DISCARDED' WHERE replay_id = %s
                            AND status = 'PREVIEWED'""", (replay_id,)).rowcount
        if n != 1:
            raise ReplayError(f"{replay_id} is not a PREVIEWED replay")
        conn.execute("DELETE FROM ops.replay_events WHERE replay_id = %s", (replay_id,))
        audit(conn, operator.username, operator.role, "replay.discard", "OK", "replay", replay_id, {})
    return get(conn, replay_id)


# --------------------------------------------------------------------------------------------------- gold rebuild

def changed_event_types(conn: psycopg.Connection, replay_id: str) -> list[str]:
    return [t for (t,) in conn.execute(
        """SELECT DISTINCT coalesce(row ->> 'event_type', current_row ->> 'event_type') FROM ops.replay_events
           WHERE replay_id = %s AND change IN ('CHANGED', 'INSERT', 'REMOVE') ORDER BY 1""", (replay_id,))]


def rebuild_gold(conn: psycopg.Connection, replay_id: str, database: str | None = None) -> dict[str, Any]:
    """Rebuild the dbt models downstream of the changed event types, then compare Gold with the preview's
    snapshot and reconcile it with Silver for the changed business dates."""
    from ..lineage.graph import build, downstream
    from ..pipeline import run_dbt

    run = get(conn, replay_id)
    if run["status"] not in ("PROMOTED", "UNDONE"):
        raise ReplayError(f"{replay_id} is {run['status']}; Gold is rebuilt after a promotion or an undo")
    types = changed_event_types(conn, replay_id)
    g = build()
    downstream_models = {n.split(":", 1)[1] for n in downstream(g, [f"silver:{t}" for t in types], "lite")
                         if n.startswith("model:")} if types else set()
    # Only models this database has built: a lab builds a subset of the project.
    built = {r for (r,) in conn.execute(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname IN ('silver', 'gold') AND c.relkind IN ('r', 'v', 'm')")}
    selected = sorted(downstream_models & built)
    if selected:
        proc = run_dbt("run", ["--select", *selected], database=database or conn.info.dbname)
        if proc.returncode != 0:
            raise ReplayError("dbt run failed:\n" + proc.stdout[-2000:])
    dates = list(run["summary"].get("changed_business_dates", []))
    after = gold_snapshot(conn, run["stream"], dates)
    checks = reconcile(conn, run["stream"], dates)
    with conn.transaction():
        conn.execute("""UPDATE ops.replay_runs SET gold_after = %s,
                        summary = summary || %s WHERE replay_id = %s""",
                     (Jsonb(after), Jsonb({"post_checks": checks, "dbt_selected": selected,
                                           "gold_rebuilt_after": run["status"]}), replay_id))
    return {"selected": selected, "gold_diff": gold_diff(run["gold_before"], after), "checks": checks}


RECONCILE: dict[str, list[tuple[str, str, str]]] = {
    "telecom": [(
        "Gold gross recharge equals the Silver sum of successful recharges",
        """SELECT (event_time AT TIME ZONE 'Asia/Dhaka')::date::text, sum((payload ->> 'amount_bdt')::numeric)
           FROM silver.events WHERE event_type = 'telecom.recharge' AND payload ->> 'status' = 'SUCCESS'
             AND (event_time AT TIME ZONE 'Asia/Dhaka')::date = ANY(%s::date[]) GROUP BY 1""",
        """SELECT business_date::text, sum(gross_recharge_bdt) FROM gold.mart_telecom_revenue_daily
           WHERE business_date = ANY(%s::date[]) GROUP BY 1""")],
    "mfg": [(
        "Gold ordered value equals the Silver sum of dealer order lines",
        """SELECT (event_time AT TIME ZONE 'Asia/Dhaka')::date::text,
                  sum((payload ->> 'quantity')::int * (payload ->> 'unit_price_bdt')::numeric)
           FROM silver.events WHERE event_type = 'mfg.dealer_order'
             AND (event_time AT TIME ZONE 'Asia/Dhaka')::date = ANY(%s::date[]) GROUP BY 1""",
        """SELECT business_date::text, sum(ordered_value_bdt) FROM gold.mart_mfg_sales_daily
           WHERE business_date = ANY(%s::date[]) GROUP BY 1"""), (
        "Gold net stock movement equals the Silver sum of inventory movement deltas",
        """SELECT (event_time AT TIME ZONE 'Asia/Dhaka')::date::text, sum((payload ->> 'quantity_delta')::int)
           FROM silver.events WHERE event_type = 'mfg.inventory_movement'
             AND (event_time AT TIME ZONE 'Asia/Dhaka')::date = ANY(%s::date[]) GROUP BY 1""",
        """SELECT business_date::text, sum(quantity_delta) FROM gold.fct_inventory_movement
           WHERE business_date = ANY(%s::date[]) GROUP BY 1""")],
}


def _money(v: Any) -> str:
    return str(Decimal(str(v)).quantize(Decimal("0.01"))) if v is not None else "0.00"


def reconcile(conn: psycopg.Connection, stream: str, dates: list[str]) -> list[dict[str, Any]]:
    days = [date.fromisoformat(d) for d in dates]
    out = []
    for name, silver_sql, gold_sql in RECONCILE[stream]:
        try:
            with conn.transaction():
                gold = {d: _money(v) for d, v in conn.execute(gold_sql, (days,))}
        except psycopg.errors.UndefinedTable:
            out.append({"name": name, "skipped": "the Gold model is not built in this database", "passed": None})
            continue
        silver = {d: _money(v) for d, v in conn.execute(silver_sql, (days,))}
        out.append(_check(f"{name} ({len(days)} changed business days)", silver, gold))
    return out
