"""Benchmark suite helpers: percentiles, latency attribution and the benchmark database guard."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from signalflow_bench.experiments import attribute_latency
from signalflow_bench.measure import percentiles
from signalflow_bench.workloads import WORKLOADS, bench_database_name


def test_percentiles_nearest_rank() -> None:
    p = percentiles([float(v) for v in range(1, 101)])
    assert (p["p50"], p["p95"], p["p99"], p["min"], p["max"], p["n"]) == (50, 95, 99, 1, 100, 100)


def test_percentiles_are_weighted_by_rows() -> None:
    assert percentiles([10.0, 20.0], [98, 2])["p99"] == 20
    assert percentiles([10.0, 20.0], [99, 1])["p99"] == 10
    assert percentiles([10.0, 20.0], [99, 1])["mean"] == pytest.approx(10.1)
    assert percentiles([])["p50"] is None


def test_latency_is_attributed_to_the_rows_of_each_file_and_batch() -> None:
    t0 = datetime(2026, 10, 1, tzinfo=UTC)
    s = timedelta(seconds=1)
    commits = [(0, 10, t0), (10, 25, t0 + 2 * s)]               # file 1: ids 1-10, file 2: ids 11-25
    runs = [(0, 5, t0 + s), (5, 20, t0 + 3 * s), (20, 25, t0 + 4 * s)]
    latencies, weights = attribute_latency(commits, runs)
    assert list(zip(latencies, weights, strict=True)) == [(1000.0, 5), (3000.0, 5), (1000.0, 10), (2000.0, 5)]
    assert sum(weights) == 25


def test_bench_database_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    assert bench_database_name("signalflow_bench_x") == "signalflow_bench_x"
    monkeypatch.setenv("PGDATABASE", "my_bench_demo")
    for name in ("signalflow", "signalflow_lab", "postgres", "analytics", "my_bench_demo", "bench; drop"):
        with pytest.raises(ValueError):
            bench_database_name(name)


def test_workloads_grow() -> None:
    sizes = [w.subscribers * w.days for w in WORKLOADS.values()]
    assert sizes == sorted(sizes) and len(set(sizes)) == len(sizes)
