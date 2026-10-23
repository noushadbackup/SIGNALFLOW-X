"""The benchmark experiments. Each one runs real pipeline code and returns what it measured plus its checks.

A check compares an observed value with the generator's truth manifest or with another run of the same data.
Experiments run in the order of ``runner.EXPERIMENTS``; later ones reuse the Silver fingerprint of the continuous run
and the processor capacity measured by the backfill.
"""

from __future__ import annotations

import cProfile
import json
import os
import pstats
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import psycopg

from signalflow import CONTRACTS_DIR
from signalflow.assurance.hunter import hunt
from signalflow.chaos.lab import silver_fingerprint
from signalflow.contracts import ContractRegistry
from signalflow.db import migrate
from signalflow.generator import generate
from signalflow.ingest.bronze_loader import SOURCE_STREAM, _hints, arrival_files, ingest_file
from signalflow.pipeline import prepare, run_dbt
from signalflow.processing.silver import PROCESSOR, pending_rows, process
from signalflow.security import Operator
from signalflow.timemachine.replay import MAX_WINDOW, Window, preview
from signalflow_eval.leak_hunter import run as evaluate_leak_hunter

from .measure import Clock, percentiles, pg_counters, pg_delta, run_worker
from .workloads import Workload, connect, drop, recreate

DHAKA = ZoneInfo("Asia/Dhaka")


@dataclass
class Bench:
    workload: Workload
    db: str          # the continuous run: Silver, Gold, Leak Hunter, SQL, Time Machine
    bronze_db: str   # Bronze loaded, nothing processed: the template every backfill starts from
    work_db: str     # scratch: one experiment at a time
    log_dir: Path
    params: dict[str, Any]
    registry: ContractRegistry = field(default_factory=lambda: ContractRegistry.load(CONTRACTS_DIR, "compiled"))
    truth: dict[str, Any] = field(default_factory=dict)
    fingerprint: dict[str, Any] | None = None
    capacity_rows_per_second: float | None = None

    @property
    def data(self) -> Path:
        return self.workload.directory()

    def files(self) -> list[Path]:
        return arrival_files(self.data / "arrivals")

    def files_up_to(self, lines: int, minimum: int = 2) -> list[Path]:
        chosen: list[Path] = []
        total = 0
        for path in self.files():
            if total >= lines and len(chosen) >= minimum:
                break
            chosen.append(path)
            total += _line_count(path)
        return chosen


def _line_count(path: Path) -> int:
    with path.open("rb") as fh:
        return sum(1 for _ in fh)


def check(name: str, expected: Any, observed: Any) -> dict[str, Any]:
    return {"check": name, "expected": expected, "observed": observed, "passed": expected == observed}


def scalar(conn: psycopg.Connection, query: str, params: Any = None) -> Any:
    row = conn.execute(query, params).fetchone()
    return row[0] if row else None


def counts(conn: psycopg.Connection, query: str) -> dict[str, int]:
    return {str(k): int(v) for k, v in conn.execute(query).fetchall()}


# --------------------------------------------------------------------------------------------- correctness

def silver_checks(conn: psycopg.Connection, b: Bench) -> list[dict[str, Any]]:
    """Silver, quarantine and duplicate log against the truth manifest."""
    t = b.truth
    expected_ids = set((b.data / "truth" / "valid_event_ids.txt").read_text().split())
    actual_ids = {r[0] for r in conn.execute("SELECT event_id::text FROM silver.events")}
    quarantine = counts(conn, "SELECT reason_code, count(*) FROM silver.quarantine GROUP BY 1")
    return [
        check("Silver holds every unique valid event once", t["unique_valid_events"], len(actual_ids)),
        check("Silver event ids equal the valid event ids (missing, unexpected)", [0, 0],
              [len(expected_ids - actual_ids), len(actual_ids - expected_ids)]),
        check("Technical duplicates logged, not loaded", t["technical_duplicates"],
              scalar(conn, "SELECT count(*) FROM ops.duplicate_events")),
        check("Quarantine by reason", t["expected_quarantine_by_reason"], quarantine),
    ]


def fingerprint_check(conn: psycopg.Connection, b: Bench, label: str) -> dict[str, Any]:
    fp = silver_fingerprint(conn)
    return check(f"Silver fingerprint equals the continuous run ({label})", b.fingerprint, fp)


def batch_stats(conn: psycopg.Connection) -> dict[str, Any]:
    rows = conn.execute(
        """SELECT extract(epoch FROM finished_at - started_at) * 1000, rows_read, started_at, finished_at
           FROM ops.processing_runs WHERE processor = %s ORDER BY to_bronze_id""", (PROCESSOR,)).fetchall()
    if not rows:
        return {"batches": 0}
    span = (rows[-1][3] - rows[0][2]).total_seconds()
    read = sum(int(r[1]) for r in rows)
    return {"batches": len(rows), "batch_ms": percentiles([float(r[0]) for r in rows]),
            "processing_span_seconds": round(span, 3),
            "rows_per_second_processing_span": round(read / span) if span else None}


def runs_are_contiguous(conn: psycopg.Connection) -> int:
    """Number of processor runs whose range does not start where the previous one ended (gaps or overlaps)."""
    return int(scalar(conn, """
        SELECT count(*) FROM (
            SELECT from_bronze_id, lag(to_bronze_id) OVER (ORDER BY to_bronze_id) AS prev
            FROM ops.processing_runs WHERE processor = %s) r
        WHERE prev IS NOT NULL AND from_bronze_id <> prev""", (PROCESSOR,)))


# --------------------------------------------------------------------------------------------- experiments

def generation(b: Bench) -> dict[str, Any]:
    """Generate the workload once. Its speed describes the generator only, not the pipeline."""
    manifest = b.data / "truth" / "manifest.json"
    seconds = None
    if not manifest.exists():
        t0 = time.perf_counter()
        generate(b.workload.config(), b.data, overwrite=True)
        seconds = round(time.perf_counter() - t0, 2)
    b.truth = json.loads(manifest.read_text())
    files = b.files()
    return {
        "generated_now": seconds is not None, "seconds": seconds,
        "lines_per_second": round(b.truth["lines_total"] / seconds) if seconds else None,
        "arrival_files": len(files), "arrival_bytes": sum(p.stat().st_size for p in files),
        "lines": b.truth["lines_total"], "unique_valid_events": b.truth["unique_valid_events"],
        "valid_events_by_type": b.truth["valid_events_by_type"],
        "technical_duplicates": b.truth["technical_duplicates"],
        "quarantine_expected": sum(b.truth["expected_quarantine_by_reason"].values()),
    }


ROW_INSERT = """INSERT INTO bronze.raw_events (source_stream, source_position, arrival_time, ingest_batch_id, event_id,
                    event_type, schema_version, event_time, raw_record, record_sha256, parse_error)
                VALUES (%(stream)s, %(pos)s,
                        CASE WHEN %(arrival)s ~ '^\\d{4}-\\d{2}-\\d{2}T[0-9:.]+Z$' THEN %(arrival)s::timestamptz END,
                        %(batch)s, %(event_id)s, %(event_type)s, %(version)s,
                        CASE WHEN %(etime)s ~ '^\\d{4}-\\d{2}-\\d{2}T[0-9:.]+Z$' THEN %(etime)s::timestamptz END,
                        %(raw)s, %(sha)s, %(err)s)
                ON CONFLICT (source_stream, source_position) DO NOTHING"""


def _row_by_row(conn: psycopg.Connection, path: Path) -> None:
    """The baseline loader: one INSERT per line, one transaction per file. Same parsing and hashing as
    ``ingest_file``; only the write path differs."""
    import hashlib

    with conn.transaction():
        for i, line in enumerate(path.read_text().splitlines(), start=1):
            arrival, event_id, event_type, version, event_time, err = _hints(line)
            conn.execute(ROW_INSERT, {"stream": SOURCE_STREAM, "pos": f"{path.name}:{i}", "arrival": arrival,
                                      "batch": f"{path.name}:baseline", "event_id": event_id,
                                      "event_type": event_type, "version": version, "etime": event_time,
                                      "raw": line, "sha": hashlib.sha256(line.encode()).hexdigest(), "err": err})


BRONZE_DIGEST = """SELECT count(*), md5(string_agg(source_position || record_sha256 || coalesce(event_id, '')
                                                  || coalesce(event_time::text, '') || coalesce(parse_error, ''),
                                                  ',' ORDER BY bronze_id))
                   FROM bronze.raw_events"""


def ingest_comparison(b: Bench) -> dict[str, Any]:
    """Baseline (row-by-row INSERT) against the implemented loader (COPY into a staging table) on the same files."""
    files = b.files_up_to(int(b.params["ingest_compare_lines"]))
    lines = sum(_line_count(p) for p in files)
    out: dict[str, Any] = {"files": len(files), "lines": lines}
    digests = {}
    for impl in ("row_by_row_insert", "copy_staging"):
        recreate(b.work_db)
        with connect(b.work_db) as conn:
            migrate(conn)
            clock = Clock.start()
            for path in files:
                _row_by_row(conn, path) if impl == "row_by_row_insert" else ingest_file(conn, path)
            measured = clock.stop()
            digests[impl] = list(conn.execute(BRONZE_DIGEST).fetchone() or [])
        out[impl] = {**measured, "lines_per_second": round(lines / measured["seconds"])}
    out["speedup"] = round(out["row_by_row_insert"]["seconds"] / out["copy_staging"]["seconds"], 1)
    out["checks"] = [check("Both loaders store identical Bronze rows", digests["row_by_row_insert"],
                           digests["copy_staging"])]
    drop(b.work_db)
    return out


def continuous(b: Bench) -> dict[str, Any]:
    """Arrival files one at a time, as the lite profile runs: load the file into Bronze, then process the backlog.

    Latency here is per file with no backlog: from the start of the file's load to the commit of its last Silver
    batch. The load experiment measures latency with a backlog."""
    recreate(b.db)
    chunk = int(b.params["chunk_size"])
    with connect(b.db) as conn:
        prepare(conn, b.data)
        before = pg_counters(conn)
        clock = Clock.start()
        per_file: list[tuple[int, float, float]] = []
        for path in b.files():
            t0 = time.perf_counter()
            result = ingest_file(conn, path)
            t1 = time.perf_counter()
            process(conn, b.registry, chunk_size=chunk)
            t2 = time.perf_counter()
            per_file.append((result.lines, t1 - t0, t2 - t1))
        measured = clock.stop()
        pg = pg_delta(conn, before)
        checks = silver_checks(conn, b)
        b.fingerprint = silver_fingerprint(conn)
        batches = batch_stats(conn)
    lines = sum(f[0] for f in per_file)
    weights = [f[0] for f in per_file]
    load_s, process_s = sum(f[1] for f in per_file), sum(f[2] for f in per_file)
    return {
        "chunk_size": chunk, "files": len(per_file), "lines": lines, **measured,
        "events_per_second": round(lines / measured["seconds"]),
        "bronze_load_lines_per_second": round(lines / load_s),
        "silver_rows_per_second": round(lines / process_s),
        "share_of_time_loading_bronze": round(load_s / (load_s + process_s), 3),
        "file_lines": percentiles([float(w) for w in weights]),
        "latency_arrival_to_silver_ms": percentiles([(f[1] + f[2]) * 1000 for f in per_file], weights),
        "latency_bronze_to_silver_ms": percentiles([f[2] * 1000 for f in per_file], weights),
        "batches": batches, "postgres": pg, "silver_fingerprint": b.fingerprint, "checks": checks,
    }


def _dbt_summary(stdout: str) -> dict[str, int] | None:
    import re

    m = re.search(r"Done\. PASS=(\d+) WARN=(\d+) ERROR=(\d+) SKIP=(\d+)", stdout)
    return dict(zip(("pass", "warn", "error", "skip"), (int(g) for g in m.groups()), strict=True)) if m else None


def gold(b: Bench) -> dict[str, Any]:
    """dbt build (every model and test) on the continuous run, then Gold against the truth manifest."""
    clock = Clock.start()
    proc = run_dbt("build", database=b.db)
    build = {**clock.stop(), "returncode": proc.returncode, "summary": _dbt_summary(proc.stdout)}
    if proc.returncode != 0:
        raise RuntimeError("dbt build failed:\n" + proc.stdout[-3000:])
    clock = Clock.start()
    proc = run_dbt("run", ["--select", "+mart_telecom_revenue_daily"], database=b.db)
    refresh = {**clock.stop(), "returncode": proc.returncode, "summary": _dbt_summary(proc.stdout)}
    with connect(b.db) as conn:
        rows = conn.execute("""SELECT business_date::text, channel, region, successful_recharges, gross_recharge_bdt
                               FROM gold.mart_telecom_revenue_daily WHERE successful_recharges > 0""").fetchall()
        revenue = {(d, ch, rg): (int(n), int(Decimal(a) * 100)) for d, ch, rg, n, a in rows}
        expected_revenue = {(r["business_date"], r["channel"], r["region"]): (r["success_count"], r["amount_poisha"])
                            for r in b.truth["recharge_revenue"]}
        stock = {(w, s): int(q) for w, s, q in conn.execute(
            "SELECT warehouse_id, sku, on_hand_qty FROM gold.mart_inventory_position")}
        expected_stock = {(r["warehouse_id"], r["sku"]): r["quantity"] for r in b.truth["inventory_on_hand"]}
        silver_total = scalar(conn, """SELECT coalesce(sum((payload ->> 'amount_bdt')::numeric(14,2)), 0)
                                       FROM silver.events WHERE event_type = 'telecom.recharge'
                                         AND payload ->> 'status' = 'SUCCESS'""")
        gold_total = scalar(conn, "SELECT coalesce(sum(gross_recharge_bdt), 0) FROM gold.mart_telecom_revenue_daily")
        flagged = {r[0] for r in conn.execute(
            "SELECT settlement_id FROM gold.fct_channel_settlement WHERE difference_bdt <> 0")}
        gold_rows = scalar(conn, """SELECT sum(n_live_tup) FROM pg_stat_user_tables WHERE schemaname = 'gold'""")
    injected = set()
    for raw in (b.data / "truth" / "anomalies.jsonl").read_text().splitlines():
        a = json.loads(raw)
        if a["anomaly_type"] == "SETTLEMENT_MISMATCH":
            injected.add(a["refs"]["settlement_id"])
    expected_total = sum(r["amount_poisha"] for r in b.truth["recharge_revenue"])
    return {
        "dbt_build": build, "gold_refresh_revenue_path": refresh, "gold_rows": int(gold_rows or 0),
        "gross_recharge_bdt": str(gold_total),
        "checks": [
            check("Gold daily revenue per date, channel and region equals truth to the poisha (cells differing)",
                  0, len(set(revenue.items()) ^ set(expected_revenue.items()))),
            check("Gold total recharge revenue equals truth (poisha)", expected_total, int(Decimal(gold_total) * 100)),
            check("Gold total equals the sum of successful recharges in Silver", str(silver_total), str(gold_total)),
            check("Gold stock per warehouse and SKU equals truth (positions differing)", 0,
                  len(set(stock.items()) ^ set(expected_stock.items()))),
            check("Settlement reconciliation flags exactly the injected mismatches", sorted(injected), sorted(flagged)),
            check("dbt build has no errors, warnings or skips", [0, 0, 0],
                  [build["summary"][k] for k in ("error", "warn", "skip")] if build["summary"] else None),
        ],
    }


def leak_hunter(b: Bench) -> dict[str, Any]:
    """One hunt over the whole workload, then the offline evaluation against the anomaly ledger."""
    with connect(b.db) as conn:
        clock = Clock.start()
        result = hunt(conn)
        measured = clock.stop()
        report = evaluate_leak_hunter(b.data, conn, record=False)
        cases = counts(conn, "SELECT status, count(*) FROM assurance.cases GROUP BY 1")
    scenarios = {rule: {k: r.get(k) for k in ("scenario", "positives", "confirmed_tp", "confirmed_fp",
                                               "precision_confirmed", "recall_confirmed", "missed")}
                 for rule, r in report["results"].items() if r.get("scenario")}
    return {**measured, "cases_by_status": cases, "rules": len(report["results"]), "scenarios": scenarios,
            "hunt": {k: v for k, v in vars(result).items() if isinstance(v, int | str | dict)},
            "checks": [check("Leak Hunter evaluation passes (no false confirmation, benign timing never confirmed)",
                             True, bool(report["passed"]))]}


QUERIES: list[tuple[str, str, str, str]] = [
    ("Recharge revenue by day, channel and region (whole period)",
     "Silver jsonb, aggregated at query time",
     """SELECT (event_time AT TIME ZONE 'Asia/Dhaka')::date, payload ->> 'channel', payload ->> 'region',
               count(*) FILTER (WHERE payload ->> 'status' = 'SUCCESS'),
               coalesce(sum((payload ->> 'amount_bdt')::numeric(14,2))
                        FILTER (WHERE payload ->> 'status' = 'SUCCESS'), 0)
        FROM silver.events WHERE event_type = 'telecom.recharge' GROUP BY 1, 2, 3""",
     """SELECT business_date, channel, region, successful_recharges, gross_recharge_bdt
        FROM gold.mart_telecom_revenue_daily"""),
    ("Usage by day, region and type (whole period)",
     "Silver jsonb, aggregated at query time",
     """SELECT (event_time AT TIME ZONE 'Asia/Dhaka')::date, payload ->> 'serving_region', payload ->> 'usage_type',
               count(*), count(DISTINCT payload ->> 'subscriber_id'),
               sum((payload ->> 'quantity')::numeric(14,2)), sum((payload ->> 'rated_amount_bdt')::numeric(14,2))
        FROM silver.events WHERE event_type = 'telecom.usage_record' GROUP BY 1, 2, 3""",
     """SELECT business_date, serving_region, usage_type, usage_records, distinct_subscribers, total_quantity,
               rated_bdt FROM gold.mart_telecom_usage_daily"""),
    ("One day's recharge revenue (a dashboard tile)",
     "Silver jsonb through the (event_type, event_time) index",
     """SELECT count(*), coalesce(sum((payload ->> 'amount_bdt')::numeric(14,2)), 0) FROM silver.events
        WHERE event_type = 'telecom.recharge' AND payload ->> 'status' = 'SUCCESS'
          AND event_time >= %(start)s AND event_time < %(end)s""",
     """SELECT coalesce(sum(successful_recharges), 0), coalesce(sum(gross_recharge_bdt), 0)
        FROM gold.mart_telecom_revenue_daily WHERE business_date = %(day)s"""),
]


def _normalise(rows: list[tuple[Any, ...]]) -> list[tuple[str, ...]]:
    return sorted(tuple(str(v) for v in r) for r in rows)


def sql_serving(b: Bench) -> dict[str, Any]:
    """The same answer from Silver at query time (baseline) and from the Gold mart dbt built (optimized)."""
    reps, warmup = int(b.params["sql_reps"]), 2
    day = date.fromisoformat(b.truth["generator_config"]["start_date"]) + timedelta(days=b.workload.days // 2)
    start = datetime.combine(day, datetime.min.time(), DHAKA)
    params = {"day": day, "start": start, "end": start + timedelta(days=1)}
    out: list[dict[str, Any]] = []
    with connect(b.db) as conn:
        for name, baseline_label, baseline, optimized in QUERIES:
            timings: dict[str, list[float]] = {}
            results = {}
            for label, query in (("baseline", baseline), ("optimized", optimized)):
                for _ in range(warmup):
                    conn.execute(query, params).fetchall()
                times = []
                for _ in range(reps):
                    t0 = time.perf_counter()
                    rows = conn.execute(query, params).fetchall()
                    times.append((time.perf_counter() - t0) * 1000)
                timings[label], results[label] = times, _normalise(rows)
            base, opt = percentiles(timings["baseline"]), percentiles(timings["optimized"])
            out.append({"query": name, "baseline": baseline_label, "optimized": "Gold mart built by dbt",
                        "rows": len(results["optimized"]), "baseline_ms": base, "optimized_ms": opt,
                        "speedup_p50": round(float(base["p50"] or 0) / float(opt["p50"] or 1), 1),
                        "checks": [check(f"Same result from both ({name})", True,
                                         results["baseline"] == results["optimized"])]})
        silver_rows = int(scalar(conn, "SELECT count(*) FROM silver.events"))
    return {"repetitions": reps, "warmup": warmup, "silver_rows": silver_rows, "queries": out,
            "checks": [c for q in out for c in q["checks"]]}


def time_machine(b: Bench) -> dict[str, Any]:
    """Preview a replay of a three-day telecom window from Bronze. Nothing was damaged, so nothing may change."""
    first = date.fromisoformat(b.truth["generator_config"]["start_date"])
    span = min(MAX_WINDOW, timedelta(days=b.workload.days))
    start = datetime.combine(first + timedelta(days=1 if b.workload.days > 3 else 0), datetime.min.time(), DHAKA)
    window = Window("telecom", start, start + span)
    with connect(b.db) as conn:
        clock = Clock.start()
        run = preview(conn, Operator("bench", "Benchmark run", "analyst"), window, b.registry)
        measured = clock.stop()
    summary = run["summary"]
    # NONE: a record with no Silver row of its own (a duplicate or a quarantined record), in both runs alike.
    changes = {k: v for k, v in summary.get("changes", {}).items() if k not in ("UNCHANGED", "NONE")}
    return {**measured, "window_hours": span.total_seconds() / 3600, "records": summary["records"],
            "records_per_second": round(summary["records"] / measured["seconds"]), "changes": summary["changes"],
            "checks": [check("Replay of an undamaged window changes, inserts or removes nothing", {}, changes),
                       check("Replay fingerprint equals Silver", run["silver_fingerprint"],
                             run["replay_fingerprint"])]}


def storage(b: Bench) -> dict[str, Any]:
    with connect(b.db) as conn:
        sizes: dict[str, int] = dict(conn.execute(
            """SELECT n, pg_total_relation_size(n::regclass) FROM unnest(ARRAY['bronze.raw_events', 'silver.events',
                   'silver.quarantine', 'ops.duplicate_events', 'ops.processing_runs']) n""").fetchall())
        gold_bytes = scalar(conn, """SELECT coalesce(sum(pg_total_relation_size(c.oid)), 0) FROM pg_class c
                                     JOIN pg_namespace n ON n.oid = c.relnamespace
                                     WHERE n.nspname = 'gold' AND c.relkind = 'r'""")
        db_bytes = scalar(conn, "SELECT pg_database_size(current_database())")
    lines = b.truth["lines_total"]
    files = sum(p.stat().st_size for p in b.files())
    return {"database_bytes": int(db_bytes), "table_bytes": {k: int(v) for k, v in sizes.items()},
            "gold_bytes": int(gold_bytes), "arrival_file_bytes": files,
            "bronze_bytes_per_line": round(sizes["bronze.raw_events"] / lines),
            "silver_bytes_per_event": round(sizes["silver.events"] / b.truth["unique_valid_events"]),
            "database_to_file_ratio": round(int(db_bytes) / files, 2)}


def drop_continuous(b: Bench) -> dict[str, Any]:
    """Free the disk before the backfill phase (it holds its own copies)."""
    drop(b.db)
    return {"dropped": b.db}


def bronze_template(b: Bench) -> dict[str, Any]:
    """Load every arrival file into Bronze without processing. Every backfill starts from a copy of this."""
    recreate(b.bronze_db)
    with connect(b.bronze_db) as conn:
        prepare(conn, b.data)
        clock = Clock.start()
        lines = sum(ingest_file(conn, p).lines for p in b.files())
        measured = clock.stop()
        conn.execute("CHECKPOINT")
        size = scalar(conn, "SELECT pg_database_size(current_database())")
    t0 = time.perf_counter()
    recreate(b.work_db, template=b.bronze_db)
    clone = round(time.perf_counter() - t0, 3)
    drop(b.work_db)
    return {**measured, "lines": lines, "lines_per_second": round(lines / measured["seconds"]),
            "database_bytes": int(size), "clone_seconds": clone}


def backfill(b: Bench) -> dict[str, Any]:
    """Rebuild Silver from the full Bronze backlog with the processor in its own process.

    Baseline: jsonschema-only validation (``reference``) at the default chunk size. Optimized: compiled validation,
    at several chunk sizes. Every run must produce the same Silver as the continuous run."""
    chunk_default = int(b.params["chunk_size"])
    configs = [("reference", chunk_default)] + [("compiled", int(c)) for c in b.params["chunks"]]
    runs = []
    for validator, chunk in configs:
        recreate(b.work_db, template=b.bronze_db)
        with connect(b.work_db) as conn:
            rows = int(scalar(conn, "SELECT count(*) FROM bronze.raw_events"))
            before = pg_counters(conn)
        worker = run_worker(b.work_db, chunk, str(b.log_dir / f"backfill-{validator}-{chunk}.log"), validator)
        label = f"{validator} validator, chunk {chunk}"
        with connect(b.work_db) as conn:
            pg = pg_delta(conn, before)
            batches = batch_stats(conn)
            classes = counts(conn, "SELECT arrival_class, count(*) FROM silver.events GROUP BY 1")
            checks = [check(f"Worker exited cleanly ({label})", 0, worker["returncode"]),
                      check(f"Backlog drained ({label})", 0, pending_rows(conn)),
                      fingerprint_check(conn, b, label) if b.fingerprint else
                      check(f"Silver holds every unique valid event once ({label})", b.truth["unique_valid_events"],
                            scalar(conn, "SELECT count(*) FROM silver.events")),
                      check(f"Technical duplicates logged, not loaded ({label})", b.truth["technical_duplicates"],
                            scalar(conn, "SELECT count(*) FROM ops.duplicate_events")),
                      check(f"Quarantine by reason ({label})", b.truth["expected_quarantine_by_reason"],
                            counts(conn, "SELECT reason_code, count(*) FROM silver.quarantine GROUP BY 1")),
                      check(f"Processor runs cover Bronze without gaps or overlaps ({label})", 0,
                            runs_are_contiguous(conn))]
        runs.append({"validator": validator, "chunk_size": chunk, "rows": rows, **worker,
                     "rows_per_second": round(rows / worker["seconds"]), **batches,
                     "arrival_classes": classes, "postgres": pg, "checks": checks})
        drop(b.work_db)
    baseline = runs[0]
    compiled = [r for r in runs if r["validator"] == "compiled"]
    best = max(compiled, key=lambda r: r["rows_per_second"])
    same_chunk = next(r for r in compiled if r["chunk_size"] == chunk_default) if any(
        r["chunk_size"] == chunk_default for r in compiled) else best
    smallest = min(compiled, key=lambda r: r["chunk_size"])
    b.capacity_rows_per_second = float(same_chunk["rows_per_second_processing_span"] or same_chunk["rows_per_second"])
    return {"runs": runs, "baseline": {"validator": "reference", "chunk_size": chunk_default},
            "best_chunk": best["chunk_size"],
            "speedup_validator": round(same_chunk["rows_per_second"] / baseline["rows_per_second"], 2),
            "speedup_best_vs_baseline": round(best["rows_per_second"] / baseline["rows_per_second"], 2),
            "speedup_best_vs_smallest_chunk": round(best["rows_per_second"] / smallest["rows_per_second"], 2),
            "capacity_rows_per_second": round(b.capacity_rows_per_second),
            "checks": [c for r in runs for c in r["checks"]]}


def recovery(b: Bench) -> dict[str, Any]:
    """SIGKILL the processor part-way through a backlog, restart it, and verify the result."""
    chunk, at = int(b.params["chunk_size"]), float(b.params["kill_at"])
    recreate(b.work_db, template=b.bronze_db)
    log_path = b.log_dir / "recovery.log"
    with connect(b.work_db) as conn:
        total = int(scalar(conn, "SELECT max(bronze_id) FROM bronze.raw_events"))
        env = {**os.environ, "PGDATABASE": b.work_db, "SIGNALFLOW_VALIDATOR": "compiled"}
        with log_path.open("a") as log:
            proc = subprocess.Popen([sys.executable, "-m", "signalflow.chaos.worker", "--chunk-size", str(chunk)],
                                    env=env, stdout=log, stderr=log)
        t_start = time.perf_counter()
        checkpoint = 0
        while proc.poll() is None:
            checkpoint = int(scalar(conn, "SELECT coalesce(max(last_bronze_id), 0) FROM ops.checkpoints") or 0)
            if checkpoint >= at * total:
                break
            time.sleep(0.05)
        killed = proc.poll() is None
        proc.send_signal(signal.SIGKILL)
        proc.wait()
        t_kill = time.perf_counter()
        checkpoint = int(scalar(conn, "SELECT coalesce(max(last_bronze_id), 0) FROM ops.checkpoints") or 0)
        past = int(scalar(conn, """SELECT (SELECT count(*) FROM silver.events WHERE bronze_id > %(c)s)
                                        + (SELECT count(*) FROM silver.quarantine WHERE bronze_id > %(c)s)
                                        + (SELECT count(*) FROM ops.duplicate_events WHERE bronze_id > %(c)s)""",
                          {"c": checkpoint}))
        backlog = pending_rows(conn)
    worker = run_worker(b.work_db, chunk, str(log_path))
    with connect(b.work_db) as conn:
        checks = [check("Killed while work remained", True, killed and 0 < checkpoint < total),
                  check("No Silver, quarantine or duplicate row past the checkpoint after the kill", 0, past),
                  check("Restarted worker exited cleanly", 0, worker["returncode"]),
                  check("Backlog drained after restart", 0, pending_rows(conn)),
                  check("Processor runs cover Bronze without gaps or overlaps", 0, runs_are_contiguous(conn)),
                  check("Technical duplicates logged once", b.truth["technical_duplicates"],
                        scalar(conn, "SELECT count(*) FROM ops.duplicate_events")),
                  check("Quarantine by reason", b.truth["expected_quarantine_by_reason"],
                        counts(conn, "SELECT reason_code, count(*) FROM silver.quarantine GROUP BY 1"))]
        if b.fingerprint:
            checks.append(fingerprint_check(conn, b, "after kill and restart"))
    drop(b.work_db)
    return {"chunk_size": chunk, "bronze_rows": total, "killed_after_seconds": round(t_kill - t_start, 3),
            "checkpoint_at_kill": checkpoint, "share_done_at_kill": round(checkpoint / total, 3),
            "backlog_at_kill": backlog, "restart_to_drained_seconds": worker["seconds"],
            "rows_per_second_after_restart": round(backlog / worker["seconds"]), "worker": worker, "checks": checks}


_CATEGORIES = (
    ("contract validation (jsonschema)", ("jsonschema", "referencing", "rpds", "jsonpointer")),
    ("contract validation (compiled)", ("fastjsonschema", "<string>:validate")),
    ("Postgres driver and waiting on the server", ("psycopg", "wait", "socket", "select.", "poll")),
    ("JSON parse and serialise", ("json/", "json\\", "loads", "dumps", "encode_basestring", "scanstring")),
    ("hashing", ("sha256", "_hashlib", "hexdigest")),
    ("timestamps", ("fromisoformat", "strptime", "datetime", "_strptime")),
    ("SignalFlow processor code", ("signalflow",)),
)


def _category(filename: str, func: str) -> str:
    text = f"{filename}:{func}"
    for name, needles in _CATEGORIES:
        if any(n in text for n in needles):
            return name
    return "other (interpreter, stdlib)"


def _profile_once(b: Bench, files: list[Path], validator: str) -> dict[str, Any]:
    registry = ContractRegistry.load(CONTRACTS_DIR, validator=validator)
    recreate(b.work_db)
    with connect(b.work_db) as conn:
        prepare(conn, b.data)
        lines = sum(ingest_file(conn, p).lines for p in files)
        profiler = cProfile.Profile()
        clock = Clock.start()
        profiler.enable()
        process(conn, registry, chunk_size=int(b.params["chunk_size"]))
        profiler.disable()
        measured = clock.stop()
    drop(b.work_db)
    stats = pstats.Stats(profiler)
    raw: dict[tuple[str, int, str], tuple[int, int, float, float, Any]] = stats.stats  # type: ignore[attr-defined]
    total = sum(v[2] for v in raw.values())
    by_category: dict[str, float] = {}
    for (filename, _, func), (_, _, tottime, _, _) in raw.items():
        cat = _category(filename, func)
        by_category[cat] = by_category.get(cat, 0.0) + tottime
    top = sorted(raw.items(), key=lambda kv: kv[1][2], reverse=True)[:12]
    return {"validator": validator, "lines": lines, **measured, "profiled_seconds": round(total, 3),
            "by_category": [{"category": c, "seconds": round(s, 3), "share": round(s / total, 3)}
                            for c, s in sorted(by_category.items(), key=lambda kv: -kv[1])],
            "top_functions": [{"function": f"{Path(f).name}:{line}:{fn}", "calls": v[1],
                               "seconds": round(v[2], 3), "share": round(v[2] / total, 3)}
                              for (f, line, fn), v in top]}


def profile(b: Bench) -> dict[str, Any]:
    """Where the processor spends its time: cProfile over draining the first files of the workload, once per
    validator."""
    files = b.files_up_to(int(b.params["profile_rows"]))
    return {"runs": [_profile_once(b, files, v) for v in ("reference", "compiled")],
            "note": "cProfile adds overhead to every Python call, so Python-heavy categories are overstated."}


# ------------------------------------------------------------------------------------------------- load

def attribute_latency(commits: list[tuple[int, int, datetime]],
                      runs: list[tuple[int, int, datetime]]) -> tuple[list[float], list[int]]:
    """Per-row latency in ms: the commit time of the processor batch that read the row minus the commit time of the
    Bronze load that wrote it. Both lists hold (exclusive lower bronze_id, inclusive upper bronze_id, commit time),
    sorted by id. Bronze ids of one producer are contiguous, so the row count of each pair is the range overlap.
    Returns (latency of each file/batch pair, rows in that pair)."""
    latencies: list[float] = []
    weights: list[int] = []
    i = 0
    for prev, top, committed in commits:
        while i < len(runs) and runs[i][1] <= prev:
            i += 1
        j = i
        while j < len(runs) and runs[j][0] < top:
            overlap = min(top, runs[j][1]) - max(prev, runs[j][0])
            if overlap > 0:
                latencies.append((runs[j][2] - committed).total_seconds() * 1000)
                weights.append(overlap)
            j += 1
    return latencies, weights


def _load_once(b: Bench, rate: float, files: list[Path], gold_freshness: bool) -> dict[str, Any]:
    recreate(b.work_db)
    with connect(b.work_db) as conn:
        prepare(conn, b.data)
    sizes = [_line_count(p) for p in files]
    commits: list[tuple[int, int, datetime]] = []      # (previous max bronze_id, new max, Postgres commit time)
    backlog: list[tuple[float, int]] = []
    done = threading.Event()
    errors: list[BaseException] = []

    def producer() -> None:
        try:
            with connect(b.work_db) as pconn:
                start, sent, prev = time.perf_counter(), 0, 0
                for path, n in zip(files, sizes, strict=True):
                    delay = start + sent / rate - time.perf_counter()
                    if delay > 0:
                        time.sleep(delay)
                    ingest_file(pconn, path)
                    row = pconn.execute("SELECT clock_timestamp(), max(bronze_id) FROM bronze.raw_events").fetchone()
                    assert row is not None
                    commits.append((prev, int(row[1]), row[0]))
                    prev, sent = int(row[1]), sent + n
        except BaseException as exc:  # surfaced in the main thread
            errors.append(exc)
        finally:
            done.set()

    def sampler(stop: threading.Event) -> None:
        with connect(b.work_db) as sconn:
            t0 = time.perf_counter()
            while not stop.is_set():
                backlog.append((round(time.perf_counter() - t0, 2), pending_rows(sconn)))
                stop.wait(0.25)

    stop_sampler = threading.Event()
    threads = [threading.Thread(target=producer), threading.Thread(target=sampler, args=(stop_sampler,))]
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    with connect(b.work_db) as conn:
        chunk = int(b.params["chunk_size"])
        while True:
            batches = process(conn, b.registry, chunk_size=chunk)
            if not batches:
                if done.is_set() and pending_rows(conn) == 0:
                    break
                time.sleep(0.05)   # idle poll interval of the consumer
        drained = conn.execute("SELECT clock_timestamp()").fetchone()
        stop_sampler.set()
        for t in threads:
            t.join()
        if errors:
            raise errors[0]
        elapsed = time.perf_counter() - t_start
        runs = conn.execute("""SELECT from_bronze_id, to_bronze_id, finished_at FROM ops.processing_runs
                               WHERE processor = %s ORDER BY to_bronze_id""", (PROCESSOR,)).fetchall()
        n_rows, lo, hi = conn.execute("SELECT count(*), min(bronze_id), max(bronze_id) FROM bronze.raw_events"
                                      ).fetchone() or (0, 0, 0)
        contiguous = runs_are_contiguous(conn)
        freshness = None
        if gold_freshness:
            proc = run_dbt("run", ["--select", "+mart_telecom_revenue_daily"], database=b.work_db)
            gold_done = conn.execute("SELECT clock_timestamp()").fetchone()
            if proc.returncode == 0 and gold_done:
                freshness = round((gold_done[0] - commits[-1][2]).total_seconds(), 3)
    drop(b.work_db)

    latencies, weights = attribute_latency(commits, [(int(r[0]), int(r[1]), r[2]) for r in runs])
    lines = sum(sizes)
    duration = (commits[-1][2] - commits[0][2]).total_seconds() if len(commits) > 1 else 0.0
    quarter = max(1, len(backlog) // 4)
    first_q = sum(v for _, v in backlog[:quarter]) / quarter if backlog else 0
    last_q = sum(v for _, v in backlog[-quarter:]) / quarter if backlog else 0
    return {
        "target_rows_per_second": round(rate), "files": len(files), "lines": lines,
        "achieved_arrival_rows_per_second": round(lines / duration) if duration else None,
        "seconds": round(elapsed, 2),
        "latency_bronze_to_silver_ms": percentiles(latencies, weights),
        "drain_after_last_arrival_seconds":
            round((drained[0] - commits[-1][2]).total_seconds(), 3) if drained else None,
        "backlog_rows": percentiles([float(v) for _, v in backlog]),
        "backlog_first_quarter_mean": round(first_q), "backlog_last_quarter_mean": round(last_q),
        "backlog_series": backlog[:: max(1, len(backlog) // 120)],
        "gold_freshness_seconds": freshness,
        "checks": [check("Every Bronze row processed exactly once", int(n_rows), sum(weights)),
                   check("Bronze ids contiguous (latency attribution exact)", int(n_rows),
                         int(hi or 0) - int(lo or 0) + 1),
                   check("Processor runs cover Bronze without gaps or overlaps", 0, contiguous)],
    }


def load(b: Bench) -> dict[str, Any]:
    """Arrivals at a fixed share of the measured processor capacity while the processor runs continuously.
    Measures Bronze-commit to Silver-commit latency per row, backlog over time, and (at the lowest load)
    the time from the last arrival until Gold includes it."""
    capacity = b.capacity_rows_per_second
    if not capacity:
        raise RuntimeError("load needs the processor capacity measured by the backfill experiment")
    out = []
    for k, share in enumerate(float(s) for s in b.params["load_shares"]):
        rate = capacity * share
        files = b.files_up_to(int(rate * float(b.params["load_seconds"])), minimum=3)
        out.append({"share_of_capacity": share, **_load_once(b, rate, files, gold_freshness=k == 0)})
    return {"capacity_rows_per_second": round(capacity), "consumer_idle_poll_ms": 50, "runs": out,
            "checks": [c for r in out for c in r["checks"]]}


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Decimal | date):
        return str(value)
    raise TypeError(type(value).__name__)


def to_json(value: Any) -> Any:
    return json.loads(json.dumps(value, default=_jsonable))
