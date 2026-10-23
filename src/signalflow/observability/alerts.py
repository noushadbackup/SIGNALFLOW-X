"""Deterministic diagnostic alerts over the ops evidence tables.

Each check is a SQL query that returns the subjects currently in breach. ``evaluate`` opens a FIRING alert for a new
subject, refreshes one that is still in breach, and resolves alerts whose subject is no longer returned, all in one
transaction. Prometheus evaluates the same conditions from the exporter's metrics (observability/prometheus); this
table is what the control tower and incident reports cite, with the record identifiers behind each alert.

Windows are measured on the wall clock of processing (``ops.processing_runs.finished_at``): alerts are about the
platform's behaviour now, unlike Leak Hunter cases, which age on event time.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from ..db import require_autocommit
from ..log import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class AlertRule:
    name: str
    severity: str
    description: str
    sql: str  # returns subject, summary, details (jsonb); parameters: window, threshold, stall


def _seconds(env: str, default: int) -> timedelta:
    return timedelta(seconds=int(os.environ.get(env, default)))


WINDOW = _seconds("SIGNALFLOW_ALERT_WINDOW_SECONDS", 600)
STALL_AFTER = _seconds("SIGNALFLOW_STALL_SECONDS", 120)

RULES: tuple[AlertRule, ...] = (
    AlertRule(
        "CONTRACT_VIOLATIONS", "CRITICAL",
        "Records quarantined in the window, per contract and reason, at or above the threshold (default 5).",
        """SELECT coalesce(q.contract_id, q.event_type, 'unknown') || ' / ' || q.reason_code AS subject,
                  count(*) || ' records quarantined as ' || q.reason_code || ' for '
                      || coalesce(q.contract_id, q.event_type, 'an unknown event type') || ' in the last '
                      || %(window)s::text AS summary,
                  jsonb_build_object('reason_code', q.reason_code, 'contract_id', q.contract_id,
                                     'count', count(*), 'sample_bronze_ids',
                                     (array_agg(q.bronze_id ORDER BY q.bronze_id))[1:10],
                                     'sample_details', (array_agg(q.details ORDER BY q.bronze_id))[1:3]) AS details
           FROM silver.quarantine q
           WHERE q.quarantined_at >= greatest(clock_timestamp() - %(window)s::interval, %(since)s)
           GROUP BY q.contract_id, q.event_type, q.reason_code
           HAVING count(*) >= %(threshold)s"""),
    AlertRule(
        "DUPLICATE_SURGE", "WARNING",
        "Redelivered records logged as duplicates in the window, at or above 10 x the threshold (default 50).",
        """SELECT s.stream AS subject,
                  count(*) || ' duplicate deliveries of ' || count(DISTINCT d.event_id) || ' ' || s.stream
                      || ' events in the last ' || %(window)s::text || '; none was counted twice' AS summary,
                  jsonb_build_object('duplicates', count(*), 'distinct_events', count(DISTINCT d.event_id),
                                     'sample_event_ids', (array_agg(DISTINCT d.event_id::text))[1:10]) AS details
           FROM ops.duplicate_events d JOIN silver.events s ON s.event_id = d.event_id
           WHERE d.detected_at >= greatest(clock_timestamp() - %(window)s::interval, %(since)s)
           GROUP BY s.stream
           HAVING count(*) >= 10 * %(threshold)s"""),
    AlertRule(
        "LATE_BEYOND_WATERMARK", "WARNING",
        "Beyond-watermark events for business days that were already final; those KPIs are marked RESTATED.",
        """SELECT l.stream || ' ' || l.business_date AS subject,
                  count(*) || ' ' || l.stream || ' events for ' || l.business_date
                      || ' arrived beyond the watermark (up to ' || round(max(l.lateness_seconds) / 3600.0, 1)
                      || ' h late); KPIs for that day are restated' AS summary,
                  jsonb_build_object('stream', l.stream, 'business_date', l.business_date, 'events', count(*),
                                     'max_lateness_hours', round(max(l.lateness_seconds) / 3600.0, 2),
                                     'sample_event_ids', (array_agg(l.event_id::text ORDER BY l.event_id))[1:10])
                      AS details
           FROM ops.late_events l
           JOIN ops.kpi_restatements k ON k.stream = l.stream AND k.business_date = l.business_date
           WHERE l.detected_at >= greatest(clock_timestamp() - %(window)s::interval, %(since)s)
           GROUP BY l.stream, l.business_date"""),
    AlertRule(
        "PROCESSING_STALLED", "CRITICAL",
        "Bronze holds records beyond a processor's checkpoint and the checkpoint has not moved for the stall time.",
        """SELECT c.processor AS subject,
                  (SELECT count(*) FROM bronze.raw_events b WHERE b.bronze_id > c.last_bronze_id)
                      || ' Bronze records wait beyond checkpoint ' || c.last_bronze_id || ' of ' || c.processor
                      || '; the checkpoint last moved at ' || to_char(c.updated_at, 'YYYY-MM-DD HH24:MI:SS TZ')
                      AS summary,
                  jsonb_build_object('checkpoint', c.last_bronze_id,
                                     'bronze_max', (SELECT max(bronze_id) FROM bronze.raw_events),
                                     'checkpoint_updated_at', c.updated_at) AS details
           FROM ops.checkpoints c
           WHERE c.updated_at < clock_timestamp() - %(stall)s::interval
             AND EXISTS (SELECT 1 FROM bronze.raw_events b WHERE b.bronze_id > c.last_bronze_id)"""),
)


def evaluate(conn: psycopg.Connection, window: timedelta = WINDOW, threshold: int = 5,
             stall_after: timedelta = STALL_AFTER, since: datetime | None = None) -> dict[str, Any]:
    """Evaluate every rule; return {opened, refreshed, resolved, firing: [...]}. ``since`` narrows the window
    (the Chaos Lab passes the start of the incident, so the lab's own build does not raise alerts)."""
    require_autocommit(conn)
    params = {"window": window, "threshold": threshold, "stall": stall_after,
              "since": since or datetime(1970, 1, 1, tzinfo=UTC)}
    opened = refreshed = resolved = 0
    with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
        conn.execute("SELECT pg_advisory_xact_lock(hashtext('signalflow.alerts'))")
        for rule in RULES:
            cur.execute(rule.sql, params)
            breaches = {r["subject"]: r for r in cur.fetchall()}
            cur.execute("SELECT alert_id, subject FROM ops.alerts WHERE rule = %s AND status = 'FIRING'", (rule.name,))
            firing = {r["subject"]: r["alert_id"] for r in cur.fetchall()}
            for subject, b in breaches.items():
                if subject in firing:
                    cur.execute("""UPDATE ops.alerts SET summary = %s, details = %s, last_seen = clock_timestamp()
                                   WHERE alert_id = %s""", (b["summary"], Jsonb(b["details"]), firing[subject]))
                    refreshed += 1
                else:
                    cur.execute("""INSERT INTO ops.alerts (rule, subject, severity, status, summary, details)
                                   VALUES (%s, %s, %s, 'FIRING', %s, %s)""",
                                (rule.name, subject, rule.severity, b["summary"], Jsonb(b["details"])))
                    opened += 1
            for subject, alert_id in firing.items():
                if subject not in breaches:
                    cur.execute("""UPDATE ops.alerts SET status = 'RESOLVED', resolved_at = clock_timestamp()
                                   WHERE alert_id = %s""", (alert_id,))
                    resolved += 1
        cur.execute("""SELECT alert_id, rule, subject, severity, summary, details, first_seen, last_seen
                       FROM ops.alerts WHERE status = 'FIRING' ORDER BY severity, first_seen""")
        now_firing = cur.fetchall()
    if opened or resolved:
        log.info("alerts evaluated", extra={"opened": opened, "resolved": resolved, "firing": len(now_firing)})
    return {"opened": opened, "refreshed": refreshed, "resolved": resolved, "firing": now_firing}
