"""Render recorded benchmark runs as Markdown tables (the Phase 6 report's results are pasted from this output).

    python -m signalflow_bench.report                 # latest completed run per workload in benchmarks/results
    python -m signalflow_bench.report --runs A.json B.json

Every number is copied from a results file; nothing is computed except ratios and unit conversions.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .runner import results_dir
from .workloads import WORKLOADS

ORDER = list(WORKLOADS)


def latest_runs(directory: Path) -> list[dict[str, Any]]:
    runs: dict[str, dict[str, Any]] = {}
    for path in sorted(directory.glob("BENCH-*.json")):
        run = json.loads(path.read_text())
        if run.get("status") == "COMPLETED":
            runs[run["workload"]] = run  # file names sort by start time, so the last one wins
    return sorted(runs.values(), key=lambda r: ORDER.index(r["workload"]) if r["workload"] in ORDER else 99)


def _n(v: Any, digits: int = 0) -> str:
    return "–" if v is None else f"{float(v):,.{digits}f}"


def _table(header: list[str], rows: list[list[str]]) -> str:
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    out += ["| " + " | ".join(r) + " |" for r in rows]
    return "\n".join(out)


def render(runs: list[dict[str, Any]]) -> str:
    if not runs:
        return "No completed runs."
    env = runs[-1]["environment"]
    parts = [f"Machine: {env['cpus']} vCPU ({env.get('cpu_model') or 'unknown'}), {env['memory_gb']} GB RAM, "
             f"{env['platform']}, Postgres {env['postgres']} "
             f"(shared_buffers {env['postgres_settings']['shared_buffers']}, "
             f"synchronous_commit {env['postgres_settings']['synchronous_commit']}), Python {env['python']}. "
             f"Commit {env['git_commit']}{' with local changes' if env.get('git_dirty') else ''}. "
             f"Containers running: {', '.join(env.get('containers_running') or []) or 'unknown'}.",
             "Runs: " + ", ".join(f"`{r['run_id']}`" for r in runs) + "."]

    def e(r: dict[str, Any], name: str) -> dict[str, Any]:
        return dict(r["experiments"].get(name) or {})

    rows = []
    for r in runs:
        c, bf, gd = e(r, "continuous"), e(r, "backfill"), e(r, "gold")
        ref = next(x for x in bf["runs"] if x["validator"] == "reference")
        comp = {x["chunk_size"]: x for x in bf["runs"] if x["validator"] == "compiled"}
        lat = c["latency_arrival_to_silver_ms"]
        rows.append([r["workload"], _n(r["headline"]["lines"]), _n(c["events_per_second"]),
                     f"{_n(lat['p50'])} / {_n(lat['p95'])} / {_n(lat['p99'])}", _n(ref["rows_per_second"]),
                     " · ".join(f"{k:,}: {_n(v['rows_per_second'])}" for k, v in sorted(comp.items())),
                     f"{bf['speedup_validator']}×", _n(gd["dbt_build"]["seconds"], 1),
                     f"{r['headline']['checks_passed']}/{r['headline']['checks_total']}"])
    parts.append("### Throughput and correctness by workload\n\n" + _table(
        ["Workload", "Lines", "Continuous events/s", "Continuous latency p50/p95/p99 (ms)",
         "Backfill jsonschema rows/s", "Backfill compiled rows/s by chunk", "Validator speed-up", "dbt build (s)",
         "Checks"], rows))

    rows = []
    for r in runs:
        for x in e(r, "load").get("runs", []):
            lat = x["latency_bronze_to_silver_ms"]
            rows.append([r["workload"], f"{int(x['share_of_capacity'] * 100)}%", _n(x["target_rows_per_second"]),
                         _n(x["achieved_arrival_rows_per_second"]), _n(lat["p50"]), _n(lat["p95"]), _n(lat["p99"]),
                         _n(x["backlog_rows"]["p95"]), _n(x["drain_after_last_arrival_seconds"], 2),
                         _n(x.get("gold_freshness_seconds"), 1)])
    parts.append("### Latency under paced load (Bronze commit to Silver commit, per row)\n\n" + _table(
        ["Workload", "Load", "Target rows/s", "Achieved rows/s", "p50 ms", "p95 ms", "p99 ms", "Backlog p95 (rows)",
         "Drain after last arrival (s)", "Gold freshness (s)"], rows))

    rows = []
    for r in runs:
        rc = e(r, "recovery")
        rows.append([r["workload"], f"{rc['share_done_at_kill']:.0%}", _n(rc["backlog_at_kill"]),
                     _n(rc["restart_to_drained_seconds"], 2), _n(rc["rows_per_second_after_restart"]),
                     "yes" if all(c["passed"] for c in rc["checks"]) else "NO"])
    parts.append("### Recovery after SIGKILL\n\n" + _table(
        ["Workload", "Killed at", "Backlog at kill", "Restart to drained (s)", "Rows/s after restart",
         "All checks passed"], rows))

    rows = []
    for r in runs:
        for qy in e(r, "sql_serving").get("queries", []):
            rows.append([r["workload"], qy["query"], f"{_n(qy['baseline_ms']['p50'], 1)} / "
                         f"{_n(qy['baseline_ms']['p95'], 1)}", f"{_n(qy['optimized_ms']['p50'], 2)} / "
                         f"{_n(qy['optimized_ms']['p95'], 2)}", f"{qy['speedup_p50']}×"])
    parts.append("### Serving queries: Silver at query time vs Gold mart (ms, p50 / p95)\n\n" + _table(
        ["Workload", "Query", "Silver", "Gold", "Speed-up (p50)"], rows))

    rows = []
    for r in runs:
        ic, bt = e(r, "ingest_comparison"), e(r, "bronze_template")
        rows.append([r["workload"], _n(ic["lines"]), _n(ic["row_by_row_insert"]["lines_per_second"]),
                     _n(ic["copy_staging"]["lines_per_second"]), f"{ic['speedup']}×",
                     _n(bt.get("lines_per_second")), _n(bt.get("clone_seconds"), 2)])
    parts.append("### Bronze loading\n\n" + _table(
        ["Workload", "Lines compared", "INSERT per line (lines/s)", "COPY (lines/s)", "Speed-up",
         "Full load, COPY (lines/s)", "Database copy for a backfill (s)"], rows))

    rows = []
    for r in runs:
        bf = e(r, "backfill")
        for x in bf["runs"]:
            pg = x["postgres"]
            rows.append([r["workload"], f"{x['validator']} / {x['chunk_size']:,}", _n(x["worker_cores"], 2),
                         _n(x["postgres_cores"], 2), _n(x.get("worker_peak_rss_mb")),
                         _n(pg["wal_bytes"] / x["rows"]), _n(pg["cache_hit_ratio"], 4),
                         _n(x["batch_ms"]["p50"]), _n(x["batch_ms"]["p95"])])
    parts.append("### Backfill resources\n\n" + _table(
        ["Workload", "Validator / chunk", "Worker cores", "Postgres cores", "Worker peak RSS (MB)", "WAL bytes per row",
         "Cache hit ratio", "Batch p50 (ms)", "Batch p95 (ms)"], rows))

    big = runs[-1]
    prof = e(big, "profile").get("runs", [])
    cats = sorted({c["category"] for p in prof for c in p["by_category"]})
    by = {p["validator"]: {c["category"]: c for c in p["by_category"]} for p in prof}
    rows = [[cat] + [f"{by[v][cat]['seconds']:.2f} s ({by[v][cat]['share']:.0%})" if cat in by.get(v, {}) else "–"
                     for v in ("reference", "compiled")] for cat in cats]
    parts.append(f"### Processor profile ({big['workload']}, {prof[0]['lines']:,} lines each, cProfile)\n\n"
                 + _table(["Category", "jsonschema", "compiled"], rows) if prof else "")

    rows = []
    for r in runs:
        st, lh, tm = e(r, "storage"), e(r, "leak_hunter"), e(r, "time_machine")
        rows.append([r["workload"], _n(st["database_bytes"] / 1024 / 1024), _n(st["bronze_bytes_per_line"]),
                     _n(st["silver_bytes_per_event"]), f"{st['database_to_file_ratio']}×", _n(lh["seconds"], 2),
                     f"{_n(tm['records'])} in {_n(tm['seconds'], 1)} s"])
    parts.append("### Storage, Leak Hunter and Time Machine\n\n" + _table(
        ["Workload", "Database (MB)", "Bronze bytes/line", "Silver bytes/event", "Database / files",
         "Leak Hunter run (s)", "Time Machine 72 h preview"], rows))
    return "\n\n".join(p for p in parts if p) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="signalflow-bench-report")
    parser.add_argument("--runs", nargs="*", help="results files (default: latest completed run per workload)")
    args = parser.parse_args(argv)
    runs = [json.loads(Path(p).read_text()) for p in args.runs] if args.runs else latest_runs(results_dir())
    print(render(runs))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
