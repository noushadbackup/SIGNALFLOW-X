"""Live state on top of the lineage graph: freshness per node, and what each firing alert is likely to affect."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from .catalog import DATA_THROUGH, Profile
from .graph import Graph, Reach, downstream, job_failure

PROCESSOR_JOBS = {"silver.lite": "silver_processor", "silver.spark": "spark_silver"}


def _exists(conn: psycopg.Connection, name: str) -> bool:
    row = conn.execute("SELECT to_regclass(%s) IS NOT NULL", (name,)).fetchone()
    return bool(row and row[0])


def freshness(conn: psycopg.Connection, g: Graph) -> dict[str, dict[str, Any]]:
    """Per node: row counts and the newest times the database holds. Only nodes with a measurable signal appear."""
    out: dict[str, dict[str, Any]] = {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""SELECT event_type, count(*) AS rows, max(ingested_at) AS last_ingested
                       FROM bronze.raw_events WHERE event_type IS NOT NULL GROUP BY 1""")
        for r in cur.fetchall():
            if f"bronze:{r['event_type']}" in g.nodes:
                out[f"bronze:{r['event_type']}"] = {"rows": r["rows"], "last_ingested": r["last_ingested"]}
        cur.execute("""SELECT event_type, count(*) AS rows, max(event_time) AS max_event_time,
                              max(processed_at) AS last_processed
                       FROM silver.events GROUP BY 1""")
        for r in cur.fetchall():
            if f"silver:{r['event_type']}" in g.nodes:
                out[f"silver:{r['event_type']}"] = {"rows": r["rows"], "max_event_time": r["max_event_time"],
                                                    "last_processed": r["last_processed"]}
        cur.execute("SELECT stream, max_event_time, watermark, updated_at FROM ops.stream_state")
        for r in cur.fetchall():
            out[f"table:ops.stream_state#{r['stream']}"] = {k: r[k] for k in ("max_event_time", "watermark",
                                                                               "updated_at")}
        for model, column in DATA_THROUGH.items():
            if f"model:{model}" in g.nodes and _exists(conn, f"gold.{model}"):
                cur.execute(sql.SQL("SELECT count(*) AS rows, max({}) AS data_through FROM {}").format(
                    sql.Identifier(column), sql.Identifier("gold", model)))
                out[f"model:{model}"] = dict(cur.fetchone() or {})
        if _exists(conn, "assurance.cases"):
            cur.execute("""SELECT rule_id, count(*) FILTER (WHERE status <> 'RESOLVED') AS open_cases,
                                  coalesce(sum(exposure_bdt) FILTER (WHERE status <> 'RESOLVED'), 0) AS open_exposure
                           FROM assurance.cases GROUP BY 1""")
            last = conn.execute("SELECT max(finished_at) FROM assurance.hunter_runs").fetchone()
            for r in cur.fetchall():
                if f"rule:{r['rule_id']}" in g.nodes:
                    out[f"rule:{r['rule_id']}"] = dict(r) | {"last_hunt": last[0] if last else None}
    return out


@dataclass
class AlertImpact:
    alert: dict[str, Any]
    starts: list[str]                 # nodes the alert points at
    job: str | None = None            # or the job it says has stopped
    reach: dict[str, Reach] = field(default_factory=dict)

    def affected(self, g: Graph, kind: str) -> list[str]:
        return sorted((n for n in self.reach if g.nodes[n].kind == kind),
                      key=lambda n: (self.reach[n].distance, n))


def alert_starts(g: Graph, rule: str, subject: str) -> tuple[list[str], str | None]:
    """Graph nodes (or the job) an alert subject identifies. Unknown subjects map to nothing, not to a guess."""
    if rule == "CONTRACT_VIOLATIONS":
        event_type = subject.split(" / ", 1)[0].split("/", 1)[0]
        node = f"bronze:{event_type}"
        return ([node] if node in g.nodes else []), None
    if rule == "DUPLICATE_SURGE":
        return ["table:ops.duplicate_events"], None
    if rule == "LATE_BEYOND_WATERMARK":
        node = f"table:ops.kpi_restatements#{subject.split(' ', 1)[0]}"
        return ([node] if node in g.nodes else []), None
    if rule == "PROCESSING_STALLED":
        return [], PROCESSOR_JOBS.get(subject)
    return [], None


def alert_impacts(conn: psycopg.Connection, g: Graph, profile: Profile) -> list[AlertImpact]:
    if not _exists(conn, "ops.alerts"):
        return []
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""SELECT alert_id, rule, subject, severity, summary, details, first_seen, last_seen
                       FROM ops.alerts WHERE status = 'FIRING' ORDER BY first_seen""")
        alerts = cur.fetchall()
    out = []
    for a in alerts:
        starts, job = alert_starts(g, a["rule"], a["subject"])
        if job:
            reach = job_failure(g, job, profile)
        elif starts:
            reach = downstream(g, starts, profile)
        else:
            reach = {}
        out.append(AlertImpact(dict(a), starts, job, reach))
    return out
