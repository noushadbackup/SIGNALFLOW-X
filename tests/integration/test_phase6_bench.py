"""Phase 6 acceptance: the benchmark suite runs every experiment on the smallest workload and every check passes.

The suite's numbers depend on the machine, so this test does not assert any speed. It asserts that the run completes,
that each experiment recorded its measurements, that its correctness checks passed, and that the benchmark databases
and generated data are cleaned up.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest

from signalflow.db import migrate
from signalflow_bench.runner import run

from .conftest import temp_database

pytestmark = pytest.mark.integration

PARAMS = {"chunks": [1000, 5000], "ingest_compare_lines": 2000, "sql_reps": 3, "profile_rows": 3000,
          "load_seconds": 3, "load_shares": [0.5]}


@pytest.fixture(scope="module")
def result(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    root = tmp_path_factory.mktemp("bench")
    os.environ["SIGNALFLOW_BENCH_DATA"] = str(root / "data")
    os.environ["SIGNALFLOW_BENCH_RESULTS"] = str(root / "results")
    os.environ["SIGNALFLOW_BENCH_DB"] = "sfx_bench_test"
    try:
        with temp_database():
            with psycopg.connect(autocommit=True) as control:
                migrate(control)
            out = run("XS", params=PARAMS)
            with psycopg.connect(autocommit=True) as control:
                row = control.execute("SELECT status, results FROM ops.benchmark_runs WHERE run_id = %s",
                                      (out["run_id"],)).fetchone()
            with psycopg.connect(dbname="postgres", autocommit=True) as admin:
                leftover = [r[0] for r in admin.execute(
                    "SELECT datname FROM pg_database WHERE datname LIKE 'sfx_bench_test%'")]
            yield {"out": out, "row": row, "leftover": leftover, "root": root}
    finally:
        for key in ("SIGNALFLOW_BENCH_DATA", "SIGNALFLOW_BENCH_RESULTS", "SIGNALFLOW_BENCH_DB"):
            os.environ.pop(key, None)


def test_run_completes_with_every_check_passing(result: dict[str, Any]) -> None:
    assert result["out"]["status"] == "COMPLETED", result["out"]["error"]
    status, results = result["row"]
    assert status == "COMPLETED"
    checks = [c for e in results["experiments"].values() for c in e.get("checks", [])]
    assert len(checks) >= 40 and all(c["passed"] for c in checks)
    assert results["headline"]["checks_passed"] == len(checks)


def test_every_experiment_recorded_measurements(result: dict[str, Any]) -> None:
    exps = result["row"][1]["experiments"]
    assert exps["continuous"]["events_per_second"] > 0
    assert exps["continuous"]["latency_arrival_to_silver_ms"]["p99"] >= exps["continuous"][
        "latency_arrival_to_silver_ms"]["p50"] > 0
    assert [(r["validator"], r["chunk_size"]) for r in exps["backfill"]["runs"]] == [
        ("reference", 5000), ("compiled", 1000), ("compiled", 5000)]
    assert exps["recovery"]["checkpoint_at_kill"] < exps["recovery"]["bronze_rows"]
    assert exps["load"]["runs"][0]["latency_bronze_to_silver_ms"]["n"] == exps["load"]["runs"][0]["lines"]
    assert {r["validator"] for r in exps["profile"]["runs"]} == {"reference", "compiled"}
    assert all(q["baseline_ms"]["p50"] > 0 for q in exps["sql_serving"]["queries"])


def test_results_file_matches_the_record(result: dict[str, Any]) -> None:
    path = Path(result["out"]["results_file"])
    assert path.parent == result["root"] / "results"
    saved = json.loads(path.read_text())
    assert saved["headline"] == result["row"][1]["headline"]
    assert saved["environment"]["cpus"]


def test_benchmark_databases_and_data_are_removed(result: dict[str, Any]) -> None:
    assert result["leftover"] == []
    assert not any((result["root"] / "data").glob("*/arrivals"))
