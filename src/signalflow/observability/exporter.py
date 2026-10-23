"""Prometheus exporter: platform state read from Postgres at scrape time, in the text exposition format.

Every value is a query over the tables the platform already writes (Bronze, Silver, quarantine, checkpoints,
processing runs, stream clocks, alerts, Leak Hunter cases, lake publish runs, Chaos Lab and replay runs). Nothing is
counted in memory, so a restarted exporter reports the same numbers, and a scrape that fails says so through
``signalflow_up 0`` instead of serving stale values. Written against the stdlib: the format is a few lines of text.

    signalflow metrics serve --port 9101           # binds 127.0.0.1 unless --host is given
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import psycopg

from ..log import get_logger

log = get_logger(__name__)


@dataclass
class Metric:
    name: str
    kind: str  # gauge or counter
    help: str
    samples: list[tuple[dict[str, str], float]] = field(default_factory=list)

    def add(self, value: Any, **labels: Any) -> None:
        if value is not None:
            self.samples.append(({k: str(v) for k, v in labels.items()}, float(value)))


def _escape(v: str) -> str:
    return v.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def render(metrics: list[Metric]) -> str:
    out: list[str] = []
    for m in metrics:
        out.append(f"# HELP {m.name} {m.help}")
        out.append(f"# TYPE {m.name} {m.kind}")
        for labels, value in m.samples:
            lbl = ",".join(f'{k}="{_escape(v)}"' for k, v in sorted(labels.items()))
            out.append(f"{m.name}{{{lbl}}} {value!r}" if lbl else f"{m.name} {value!r}")
    return "\n".join(out) + "\n"


def _exists(conn: psycopg.Connection, name: str) -> bool:
    row = conn.execute("SELECT to_regclass(%s) IS NOT NULL", (name,)).fetchone()
    return bool(row and row[0])


def _rows(conn: psycopg.Connection, sql: str, table: str) -> Iterator[tuple[Any, ...]]:
    if _exists(conn, table):
        yield from conn.execute(sql).fetchall()


def collect(conn: psycopg.Connection) -> list[Metric]:
    m = {k: Metric(k, kind, h) for k, kind, h in (
        ("signalflow_bronze_records", "counter", "Records received into Bronze, by stream (event type prefix)"),
        ("signalflow_silver_events", "counter", "Accepted Silver events by stream and event type"),
        ("signalflow_silver_late_events", "counter", "Accepted events by arrival class (not ON_TIME)"),
        ("signalflow_quarantined_records", "counter", "Quarantined records by reason"),
        ("signalflow_duplicate_records", "counter", "Duplicate deliveries absorbed (first arrival wins)"),
        ("signalflow_processing_backlog_records", "gauge", "Bronze records after the lite processor checkpoint"),
        ("signalflow_last_processing_age_seconds", "gauge", "Seconds since a processor last committed a batch"),
        ("signalflow_last_batch_duration_seconds", "gauge", "Duration of each processor's last batch"),
        ("signalflow_last_batch_rows", "gauge", "Rows read by each processor's last batch"),
        ("signalflow_stream_max_event_time_seconds", "gauge", "Stream clock (max accepted event time), Unix time"),
        ("signalflow_stream_watermark_seconds", "gauge", "Stream watermark, Unix time"),
        ("signalflow_kpi_restated_days", "gauge", "Business days restated by late data or replays, per stream"),
        ("signalflow_alerts_firing", "gauge", "Diagnostic alerts currently FIRING, by rule and severity"),
        ("signalflow_leak_cases_open", "gauge", "Open Leak Hunter cases by rule and status"),
        ("signalflow_leak_exposure_bdt", "gauge", "Exposure of open Leak Hunter cases by rule (BDT)"),
        ("signalflow_hunter_last_run_age_seconds", "gauge", "Seconds since the Leak Hunter last finished"),
        ("signalflow_lake_last_publish_age_seconds", "gauge", "Seconds since the last successful lake publish"),
        ("signalflow_lake_gold_mismatched_tables", "gauge", "Gold tables differing between Postgres and the lake "
                                                            "in the latest differential check"),
        ("signalflow_chaos_runs", "counter", "Chaos Lab incident runs by incident and status"),
        ("signalflow_replays", "counter", "Data Time Machine replays by status"),
    )}
    for stream, n in _rows(conn, """SELECT split_part(coalesce(event_type, 'unknown.x'), '.', 1), count(*)
                                    FROM bronze.raw_events GROUP BY 1""", "bronze.raw_events"):
        m["signalflow_bronze_records"].add(n, stream=stream)
    for stream, et, n in _rows(conn, "SELECT stream, event_type, count(*) FROM silver.events GROUP BY 1, 2",
                               "silver.events"):
        m["signalflow_silver_events"].add(n, stream=stream, event_type=et)
    for stream, cls, n in _rows(conn, """SELECT stream, arrival_class, count(*) FROM silver.events
                                         WHERE arrival_class <> 'ON_TIME' GROUP BY 1, 2""", "silver.events"):
        m["signalflow_silver_late_events"].add(n, stream=stream, arrival_class=cls)
    for reason, n in _rows(conn, "SELECT reason_code, count(*) FROM silver.quarantine GROUP BY 1", "silver.quarantine"):
        m["signalflow_quarantined_records"].add(n, reason=reason)
    for (n,) in _rows(conn, "SELECT count(*) FROM ops.duplicate_events", "ops.duplicate_events"):
        m["signalflow_duplicate_records"].add(n)
    for (n,) in _rows(conn, """SELECT count(*) FROM bronze.raw_events WHERE bronze_id >
                               coalesce((SELECT last_bronze_id FROM ops.checkpoints WHERE processor = 'silver.lite'), 0)
                               AND NOT EXISTS (SELECT 1 FROM ops.stream_commits)""", "ops.stream_commits"):
        m["signalflow_processing_backlog_records"].add(n, processor="silver.lite")
    for proc, age, dur, rows in _rows(conn, """
            SELECT DISTINCT ON (processor) processor, extract(epoch FROM clock_timestamp() - finished_at),
                   extract(epoch FROM finished_at - started_at), rows_read
            FROM ops.processing_runs ORDER BY processor, run_id DESC""", "ops.processing_runs"):
        m["signalflow_last_processing_age_seconds"].add(age, processor=proc)
        m["signalflow_last_batch_duration_seconds"].add(dur, processor=proc)
        m["signalflow_last_batch_rows"].add(rows, processor=proc)
    for stream, mx, wm in _rows(conn, """SELECT stream, extract(epoch FROM max_event_time),
                                                extract(epoch FROM watermark)
                                         FROM ops.stream_state""", "ops.stream_state"):
        m["signalflow_stream_max_event_time_seconds"].add(mx, stream=stream)
        m["signalflow_stream_watermark_seconds"].add(wm, stream=stream)
    for stream, n in _rows(conn, "SELECT stream, count(*) FROM ops.kpi_restatements GROUP BY 1",
                           "ops.kpi_restatements"):
        m["signalflow_kpi_restated_days"].add(n, stream=stream)
    for rule, sev, n in _rows(conn, """SELECT rule, severity, count(*) FROM ops.alerts WHERE status = 'FIRING'
                                       GROUP BY 1, 2""", "ops.alerts"):
        m["signalflow_alerts_firing"].add(n, rule=rule, severity=sev)
    for rule, status, n, exposure in _rows(conn, """
            SELECT rule_id, status, count(*), sum(exposure_bdt) FROM assurance.cases WHERE status <> 'RESOLVED'
            GROUP BY 1, 2""", "assurance.cases"):
        m["signalflow_leak_cases_open"].add(n, rule=rule, status=status)
        m["signalflow_leak_exposure_bdt"].add(exposure, rule=rule, status=status)
    for (age,) in _rows(conn, """SELECT extract(epoch FROM clock_timestamp() - max(finished_at))
                                 FROM assurance.hunter_runs""", "assurance.hunter_runs"):
        m["signalflow_hunter_last_run_age_seconds"].add(age)
    for (age,) in _rows(conn, """SELECT extract(epoch FROM clock_timestamp() - max(published_at))
                                 FROM ops.lake_publish_runs""", "ops.lake_publish_runs"):
        m["signalflow_lake_last_publish_age_seconds"].add(age)
    for (n,) in _rows(conn, """SELECT count(*) FILTER (WHERE NOT identical) FROM ops.lake_differential_checks
                               WHERE check_id = (SELECT max(check_id) FROM ops.lake_differential_checks)""",
                      "ops.lake_differential_checks"):
        m["signalflow_lake_gold_mismatched_tables"].add(n)
    for inc, status, n in _rows(conn, "SELECT incident, status, count(*) FROM ops.chaos_runs GROUP BY 1, 2",
                                "ops.chaos_runs"):
        m["signalflow_chaos_runs"].add(n, incident=inc, status=status)
    for status, n in _rows(conn, "SELECT status, count(*) FROM ops.replay_runs GROUP BY 1", "ops.replay_runs"):
        m["signalflow_replays"].add(n, status=status)
    return list(m.values())


def scrape(database: str | None = None) -> str:
    t0 = time.perf_counter()
    up = Metric("signalflow_up", "gauge", "1 if the last scrape could read Postgres")
    duration = Metric("signalflow_scrape_duration_seconds", "gauge", "Time taken to read the metrics")
    try:
        with psycopg.connect(dbname=database, autocommit=True, application_name="signalflow-exporter") as conn:
            metrics = collect(conn)
        up.add(1)
    except psycopg.Error as exc:
        log.warning("scrape failed", extra={"error": str(exc)[:300]})
        metrics = []
        up.add(0)
    duration.add(round(time.perf_counter() - t0, 4))
    return render([up, duration, *metrics])


def serve(host: str, port: int, database: str | None) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 (http.server API)
            if self.path not in ("/metrics", "/"):
                self.send_error(404)
                return
            body = scrape(database).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args: Any) -> None:  # quiet: Prometheus scrapes every 15 s
            return

    server = ThreadingHTTPServer((host, port), Handler)
    log.info("metrics exporter listening", extra={"host": host, "port": port, "database": database})
    server.serve_forever()


def main(argv: list[str] | None = None) -> int:
    """Entry point for the exporter container (no dbt or Spark needed: psycopg only)."""
    import argparse
    import os

    parser = argparse.ArgumentParser(prog="signalflow-exporter")
    parser.add_argument("--host", default=os.environ.get("SIGNALFLOW_EXPORTER_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("SIGNALFLOW_EXPORTER_PORT", "9101")))
    parser.add_argument("--database", default=os.environ.get("PGDATABASE"))
    args = parser.parse_args(argv)
    serve(args.host, args.port, args.database)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
