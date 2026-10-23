"""Run a workload through the experiments, record the results, and clean up the benchmark databases."""

from __future__ import annotations

import json
import os
import shutil
import sys
import time
import traceback
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from signalflow import REPO_ROOT
from signalflow.db import migrate

from . import experiments as ex
from .measure import environment, max_rss_mb
from .workloads import WORKLOADS, bench_database_name, drop

# Order matters: the continuous run sets the reference fingerprint, the backfill measures the capacity the load
# experiment drives at, and the continuous database is dropped before the backfill phase to bound disk use.
EXPERIMENTS: list[tuple[str, Callable[[ex.Bench], dict[str, Any]]]] = [
    ("generation", ex.generation),
    ("ingest_comparison", ex.ingest_comparison),
    ("continuous", ex.continuous),
    ("gold", ex.gold),
    ("leak_hunter", ex.leak_hunter),
    ("sql_serving", ex.sql_serving),
    ("time_machine", ex.time_machine),
    ("storage", ex.storage),
    ("drop_continuous", ex.drop_continuous),
    ("bronze_template", ex.bronze_template),
    ("backfill", ex.backfill),
    ("recovery", ex.recovery),
    ("profile", ex.profile),
    ("load", ex.load),
]
REQUIRED = {"generation": set(), "continuous": {"generation"}, "gold": {"continuous"}, "leak_hunter": {"gold"},
            "sql_serving": {"gold"}, "time_machine": {"continuous"}, "storage": {"continuous"},
            "drop_continuous": set(), "ingest_comparison": {"generation"}, "bronze_template": {"generation"},
            "backfill": {"bronze_template"}, "recovery": {"bronze_template"}, "profile": {"generation"},
            "load": {"backfill"}}

DEFAULT_PARAMS: dict[str, Any] = {
    "chunk_size": 5000,               # the processor's default micro-batch
    "chunks": [500, 2000, 5000, 20000],
    "ingest_compare_lines": 20000,
    "sql_reps": 20,
    "kill_at": 0.4,
    "profile_rows": 50000,
    "load_shares": [0.5, 0.9],
    "load_seconds": 60,
}


def results_dir() -> Path:
    return Path(os.environ.get("SIGNALFLOW_BENCH_RESULTS", REPO_ROOT / "benchmarks" / "results"))


def headline(results: dict[str, Any]) -> dict[str, Any]:
    """The numbers the Benchmark Lab leads with, copied from the experiment results (never computed elsewhere)."""
    e = results
    h: dict[str, Any] = {}
    if "generation" in e:
        h["lines"] = e["generation"]["lines"]
    if "continuous" in e:
        c = e["continuous"]
        h["continuous_events_per_second"] = c["events_per_second"]
        h["continuous_latency_p95_ms"] = c["latency_arrival_to_silver_ms"]["p95"]
    if "backfill" in e:
        h["backfill_best_rows_per_second"] = max(r["rows_per_second"] for r in e["backfill"]["runs"])
        h["backfill_best_chunk"] = e["backfill"]["best_chunk"]
        h["backfill_speedup_vs_baseline"] = e["backfill"]["speedup_best_vs_baseline"]
        h["validator_speedup"] = e["backfill"]["speedup_validator"]
    if "load" in e and e["load"]["runs"]:
        low = e["load"]["runs"][0]
        h["load_share"] = low["share_of_capacity"]
        h["load_latency_ms"] = {k: low["latency_bronze_to_silver_ms"][k] for k in ("p50", "p95", "p99")}
        h["gold_freshness_seconds"] = low["gold_freshness_seconds"]
    if "recovery" in e:
        h["recovery_restart_to_drained_seconds"] = e["recovery"]["restart_to_drained_seconds"]
    if "gold" in e:
        h["dbt_build_seconds"] = e["gold"]["dbt_build"]["seconds"]
    if "sql_serving" in e:
        h["sql_speedups_p50"] = [q["speedup_p50"] for q in e["sql_serving"]["queries"]]
    if "ingest_comparison" in e:
        h["ingest_copy_speedup"] = e["ingest_comparison"]["speedup"]
    checks = [c for r in e.values() if isinstance(r, dict) for c in r.get("checks", [])]
    h["checks_passed"] = sum(c["passed"] for c in checks)
    h["checks_total"] = len(checks)
    return h


def run(workload_key: str, only: set[str] | None = None, params: dict[str, Any] | None = None,
        keep_data: bool = False, record: bool = True) -> dict[str, Any]:
    workload = WORKLOADS[workload_key]
    base = bench_database_name()
    selected = [name for name, _ in EXPERIMENTS if only is None or name in only]
    missing = {r for name in selected for r in REQUIRED[name]} - set(selected)
    if missing:
        raise ValueError(f"experiments {sorted(missing)} are needed by the selection")
    run_id = f"BENCH-{workload.key}-{datetime.now(UTC):%Y%m%dT%H%M%S}"
    out_dir = results_dir()
    log_dir = out_dir.parent / "logs" / run_id
    log_dir.mkdir(parents=True, exist_ok=True)
    b = ex.Bench(workload, db=base, bronze_db=bench_database_name(f"{base}_bronze"),
                 work_db=bench_database_name(f"{base}_work"), log_dir=log_dir,
                 params={**DEFAULT_PARAMS, **(params or {})})
    config = {"workload": workload.__dict__ | {"dataset_key": workload.dataset_key},
              "generator": workload.config().to_json(), "params": b.params, "experiments": selected,
              "log_level": os.environ.get("SIGNALFLOW_LOG_LEVEL", "INFO")}
    results: dict[str, Any] = {}
    record_row = {"status": "RUNNING", "error": None}
    with psycopg.connect(autocommit=True, application_name="signalflow-bench") as control:
        env = environment(control)
        if record:
            migrate(control)
            control.execute("""INSERT INTO ops.benchmark_runs (run_id, workload, config, environment, status)
                               VALUES (%s, %s, %s, %s, 'RUNNING')""",
                            (run_id, workload.key, Jsonb(config), Jsonb(env)))

        def save(phase: str | None) -> None:
            payload = ex.to_json({"experiments": results, "headline": headline(results), "phase": phase,
                                  "bench_process_max_rss_mb": max_rss_mb()})
            if record:
                control.execute("""UPDATE ops.benchmark_runs SET results = %s, status = %s, error = %s,
                                       finished_at = CASE WHEN %s <> 'RUNNING' THEN clock_timestamp() END
                                   WHERE run_id = %s""",
                                (Jsonb(payload), record_row["status"], record_row["error"], record_row["status"],
                                 run_id))
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / f"{run_id}.json").write_text(json.dumps(
                {"run_id": run_id, "workload": workload.key, "status": record_row["status"],
                 "error": record_row["error"], "config": config, "environment": env, **payload}, indent=2) + "\n")

        try:
            for name, fn in EXPERIMENTS:
                if name not in selected:
                    continue
                print(f"{datetime.now(UTC):%H:%M:%S} {run_id} {name}", file=sys.stderr, flush=True)
                save(name)
                t0 = time.perf_counter()
                results[name] = ex.to_json(fn(b))
                results[name]["experiment_seconds"] = round(time.perf_counter() - t0, 2)
            failed = [c["check"] for r in results.values() for c in r.get("checks", []) if not c["passed"]]
            record_row["status"] = "FAILED" if failed else "COMPLETED"
            record_row["error"] = f"{len(failed)} check(s) failed: " + "; ".join(failed) if failed else None
        except BaseException as exc:
            record_row["status"], record_row["error"] = "FAILED", "".join(
                traceback.format_exception_only(type(exc), exc)).strip()[:4000]
            print(traceback.format_exc()[-4000:], file=sys.stderr, flush=True)
            raise
        finally:
            save(None)
            for db in (b.work_db, b.bronze_db, b.db):
                drop(db)
            if not keep_data:
                shutil.rmtree(workload.directory(), ignore_errors=True)
    return {"run_id": run_id, "status": record_row["status"], "error": record_row["error"],
            "headline": headline(results), "results_file": str(out_dir / f"{run_id}.json")}


def list_runs(conn: psycopg.Connection, limit: int = 20) -> list[dict[str, Any]]:
    rows = conn.execute("""SELECT run_id, workload, status, started_at, finished_at, results -> 'headline'
                           FROM ops.benchmark_runs ORDER BY started_at DESC LIMIT %s""", (limit,)).fetchall()
    return [{"run_id": r[0], "workload": r[1], "status": r[2], "started_at": r[3], "finished_at": r[4],
             "headline": r[5]} for r in rows]
