"""Benchmark Lab page: recorded benchmark runs from ``ops.benchmark_runs``.

The page only displays what ``python -m signalflow_bench`` recorded. It computes nothing except ratios between
recorded numbers, and it labels every figure with the run and machine it came from.
"""

from __future__ import annotations

import json
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from signalflow.ui import data
from signalflow.ui.components import (
    CATEGORICAL,
    CRITICAL,
    GOOD,
    GRID,
    MUTED,
    SURFACE,
    WARNING,
    badge,
    chart,
    fmt_int,
    fmt_ts,
    header,
    html_table,
    q,
    source,
    style,
    tiles,
)

RUNS = """SELECT run_id, workload, status, error, config, environment, results, started_at, finished_at
          FROM ops.benchmark_runs ORDER BY started_at DESC LIMIT 50"""
RUN_STATE = {"COMPLETED": ("✓", GOOD, "Completed"), "FAILED": ("✕", CRITICAL, "Failed"),
             "RUNNING": ("◷", WARNING, "Running")}
CHECK_STATE = {"True": ("✓", GOOD, "Pass"), "False": ("✕", CRITICAL, "Fail")}
VALIDATOR_COLOR = {"reference": CATEGORICAL[1], "compiled": CATEGORICAL[0]}
EXPERIMENT_TITLES = {
    "ingest_comparison": "Bronze loader", "continuous": "Continuous run", "gold": "Gold (dbt)",
    "leak_hunter": "Leak Hunter", "sql_serving": "SQL serving", "time_machine": "Time Machine",
    "backfill": "Backfill", "recovery": "Recovery", "load": "Latency under load",
}


def _short(v: Any, limit: int = 70) -> str:
    text = v if isinstance(v, str) else json.dumps(v, default=str)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _num(v: Any, digits: int = 0, unit: str = "") -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "–"
    return f"{float(v):,.{digits}f}{unit}"


def _bytes(v: Any) -> str:
    if v is None:
        return "–"
    size = float(v)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:,.1f} {unit}" if unit != "B" else f"{size:,.0f} B"
        size /= 1024
    return f"{size:,.1f} GB"


def _exp(run: pd.Series, name: str) -> dict[str, Any]:
    return dict((run.results or {}).get("experiments", {}).get(name) or {})


def page() -> None:
    header("Platform", "Benchmark Lab",
           "Recorded runs of the real pipeline on generated workloads of several sizes. Every figure was measured "
           "during the run shown, and every result is checked against the generator's records or against another "
           "run of the same data.")
    if not data.table_exists("ops.benchmark_runs"):
        st.info("Apply the Phase 6 migration first: `signalflow migrate`.")
        return
    runs = q(RUNS)
    if runs.empty:
        st.info("No benchmark has been recorded on this database. Run one with `make bench WORKLOAD=S`.")
        return
    scaling(runs)
    st.markdown("### One run in detail")
    labels: dict[str, str] = {
        str(r.run_id): f"{r.run_id} · {fmt_int((r.results or {}).get('headline', {}).get('lines'))} lines · "
                       f"{str(r.status).lower()}" for r in runs.itertuples()}
    run_id = st.selectbox("Run", list(labels), format_func=lambda k: labels[k], label_visibility="collapsed")
    run = runs[runs.run_id == run_id].iloc[0]
    run_detail(run)


# ------------------------------------------------------------------------------------------------ scaling

def scaling(runs: pd.DataFrame) -> None:
    done = runs[runs.status == "COMPLETED"]
    latest = done.sort_values("started_at").groupby("workload").tail(1)
    rows = []
    for r in latest.itertuples():
        h = (r.results or {}).get("headline", {})
        bf = (r.results or {}).get("experiments", {}).get("backfill", {})
        ref: dict[str, Any] = next((x for x in bf.get("runs", []) if x["validator"] == "reference"), {})
        sql = (r.results or {}).get("experiments", {}).get("sql_serving", {}).get("queries", [])
        rows.append({"workload": r.workload, "lines": h.get("lines"), "env": r.environment,
                     "continuous": h.get("continuous_events_per_second"),
                     "reference": ref.get("rows_per_second"), "compiled": h.get("backfill_best_rows_per_second"),
                     "p95": (h.get("load_latency_ms") or {}).get("p95"), "dbt": h.get("dbt_build_seconds"),
                     "sql_base": sql[0]["baseline_ms"]["p50"] if sql else None,
                     "sql_gold": sql[0]["optimized_ms"]["p50"] if sql else None,
                     "checks": f"{h.get('checks_passed')} / {h.get('checks_total')}", "run": r.run_id})
    if not rows:
        st.info("No completed run yet.")
        return
    df = pd.DataFrame(rows).sort_values("lines")
    machines = {json.dumps({k: e.get(k) for k in ("cpus", "memory_gb", "postgres")}, sort_keys=True)
                for e in df.env}
    st.markdown("### Scaling across workloads")
    st.caption("The latest completed run of each workload. "
               + ("All on the same machine: " + _machine(df.env.iloc[0]) + "." if len(machines) == 1 else
                  "Runs come from different machines, so their numbers are not directly comparable."))
    html_table(pd.DataFrame({
        "Workload": df.workload, "Arrival lines": [fmt_int(v) for v in df.lines],
        "Continuous (events/s)": [_num(v) for v in df.continuous],
        "Backfill, jsonschema (rows/s)": [_num(v) for v in df.reference],
        "Backfill, compiled (rows/s)": [_num(v) for v in df.compiled],
        "p95 latency at 50% load": [_num(v, 0, " ms") for v in df.p95],
        "dbt build": [_num(v, 1, " s") for v in df.dbt], "Checks": df.checks, "Run": df.run}),
        numeric={"Arrival lines", "Continuous (events/s)", "Backfill, jsonschema (rows/s)",
                 "Backfill, compiled (rows/s)", "p95 latency at 50% load", "dbt build"})
    if len(df) < 2:
        st.caption("Scaling needs at least two workloads. Run `make bench WORKLOAD=M` as well.")
        source("ops.benchmark_runs")
        return
    a, b = st.columns(2)
    with a:
        st.markdown("#### Throughput by workload size")
        fig = go.Figure()
        for col, name, color in (("compiled", "Backfill, compiled validation", CATEGORICAL[0]),
                                 ("reference", "Backfill, jsonschema validation", CATEGORICAL[1]),
                                 ("continuous", "Continuous, file by file", CATEGORICAL[2])):
            fig.add_scatter(x=df.lines, y=df[col].astype(float), mode="lines+markers", name=name,
                            line={"color": color, "width": 2},
                            marker={"size": 9, "line": {"color": SURFACE, "width": 2}},
                            hovertemplate="%{x:,} lines<br>%{y:,.0f} rows/s<extra>%{fullData.name}</extra>")
        fig.update_xaxes(type="log", title="arrival lines (log scale)", title_font={"color": MUTED})
        fig.update_yaxes(rangemode="tozero")
        chart(style(fig, height=300, y_title="rows per second"))
        compiled = df.compiled.astype(float)
        spread = (compiled.max() - compiled.min()) / compiled.max()
        st.caption(f"Compiled-validation backfill throughput varies by {spread:.0%} between the smallest and largest "
                   f"workload ({fmt_int(df.lines.min())} to {fmt_int(df.lines.max())} lines). Flat lines mean "
                   "processing cost grows linearly with volume on this machine; it says nothing about other "
                   "hardware.")
    with b:
        st.markdown("#### Work that grows with the data")
        fig = go.Figure()
        fig.add_scatter(x=df.lines, y=df.sql_base.astype(float), mode="lines+markers",
                        name="Revenue query on Silver (p50)", line={"color": CATEGORICAL[1], "width": 2},
                        marker={"size": 9, "line": {"color": SURFACE, "width": 2}},
                        hovertemplate="%{x:,} lines<br>%{y:,.1f} ms<extra>%{fullData.name}</extra>")
        fig.add_scatter(x=df.lines, y=df.sql_gold.astype(float), mode="lines+markers",
                        name="Same query on the Gold mart (p50)", line={"color": CATEGORICAL[0], "width": 2},
                        marker={"size": 9, "line": {"color": SURFACE, "width": 2}},
                        hovertemplate="%{x:,} lines<br>%{y:,.2f} ms<extra>%{fullData.name}</extra>")
        fig.update_xaxes(type="log", title="arrival lines (log scale)", title_font={"color": MUTED})
        fig.update_yaxes(type="log")
        chart(style(fig, height=300, y_title="ms (log scale)"))
        st.caption("dbt rebuilds every Gold table in full, so Gold freshness costs the dbt build time in the table "
                   "above, which grows with the data. Queries on Gold stay fast because they read pre-aggregated "
                   "rows.")
    source("ops.benchmark_runs")


def _machine(env: dict[str, Any]) -> str:
    return (f"{env.get('cpus')} vCPU ({env.get('cpu_model') or 'unknown CPU'}), {env.get('memory_gb')} GB RAM, "
            f"Postgres {env.get('postgres')}, Python {env.get('python')}, commit {env.get('git_commit')}"
            + (" with local changes" if env.get("git_dirty") else ""))


# -------------------------------------------------------------------------------------------- run detail

def run_detail(run: pd.Series) -> None:
    res = run.results or {}
    h = res.get("headline", {})
    st.markdown(f'<div class="sf-meta">{badge(RUN_STATE, run.status)} &nbsp; workload {run.workload} · started '
                f'{fmt_ts(run.started_at, "%d %b %Y %H:%M")} Dhaka · {_machine(run.environment)}</div>',
                unsafe_allow_html=True)
    if run.status == "RUNNING":
        st.info(f"Running: {res.get('phase') or 'starting'}. Results appear as each experiment finishes.")
    if run.error:
        st.error(run.error)
    load_low = h.get("load_latency_ms") or {}
    tiles([
        ("Arrival lines", fmt_int(h.get("lines")), f"workload {run.workload}"),
        ("Continuous throughput", _num(h.get("continuous_events_per_second"), 0, "/s"), "load + process, file by file"),
        ("Backfill throughput", _num(h.get("backfill_best_rows_per_second"), 0, "/s"),
         f"compiled validation, chunk {h.get('backfill_best_chunk', '–')}"),
        (f"Latency p95 at {int(float(h.get('load_share') or 0) * 100)}% load", _num(load_low.get("p95"), 0, " ms"),
         f"p50 {_num(load_low.get('p50'), 0, ' ms')} · p99 {_num(load_low.get('p99'), 0, ' ms')}"),
        ("Recovery", _num(h.get("recovery_restart_to_drained_seconds"), 1, " s"), "restart to backlog drained"),
        ("Checks passed", f"{h.get('checks_passed', 0)} / {h.get('checks_total', 0)}", "against generator records"),
    ])
    optimisations(run)
    latency(run)
    profile_view(run)
    recovery_view(run)
    downstream(run)
    checks_view(run)
    methodology(run)
    source("ops.benchmark_runs", f"benchmarks/results/{run.run_id}.json")


def optimisations(run: pd.Series) -> None:
    st.markdown("#### Baseline against optimized")
    bf = _exp(run, "backfill")
    if bf.get("runs"):
        st.markdown("##### Silver processor: rebuild Silver from the full Bronze backlog")
        df = pd.DataFrame(bf["runs"])
        labels = [f"{'jsonschema' if v == 'reference' else 'compiled'} · chunk {c:,}"
                  for v, c in zip(df.validator, df.chunk_size, strict=True)]
        fig = go.Figure()
        for validator, name in (("reference", "jsonschema validation (baseline)"),
                                ("compiled", "compiled validation (implemented)")):
            mask = df.validator == validator
            fig.add_bar(x=df.rows_per_second[mask].astype(float), y=[lbl for lbl, m in zip(labels, mask, strict=True)
                                                                       if m],
                        orientation="h", name=name, marker_color=VALIDATOR_COLOR[validator],
                        customdata=df[mask][["seconds", "worker_cores", "postgres_cores"]].values,
                        hovertemplate="%{y}<br>%{x:,.0f} rows/s · %{customdata[0]:.1f} s<br>worker %{customdata[1]} "
                                      "cores · Postgres %{customdata[2]} cores<extra></extra>")
        fig.update_yaxes(autorange="reversed", gridcolor=SURFACE, categoryorder="array", categoryarray=labels)
        fig.update_xaxes(showgrid=True, gridcolor=GRID, title="rows per second", title_font={"color": MUTED})
        a, b = st.columns([3, 2])
        with a:
            chart(style(fig, height=280))
        with b:
            base = df.iloc[0]
            best = df[df.validator == "compiled"].sort_values("rows_per_second").iloc[-1]
            html_table(pd.DataFrame({
                "Run": ["Baseline", "Best"], "Validation": ["jsonschema", "compiled"],
                "Chunk": [fmt_int(base.chunk_size), fmt_int(best.chunk_size)],
                "Rows/s": [_num(base.rows_per_second), _num(best.rows_per_second)],
                "Worker cores": [_num(base.worker_cores, 2), _num(best.worker_cores, 2)],
                "Postgres cores": [_num(base.postgres_cores, 2), _num(best.postgres_cores, 2)]}),
                numeric={"Chunk", "Rows/s", "Worker cores", "Postgres cores"})
            st.caption(f"Compiled validation at the same chunk size: {bf.get('speedup_validator')}× the baseline. "
                       f"Chunk size alone (smallest to best, compiled): {bf.get('speedup_best_vs_smallest_chunk')}×. "
                       "Every run produced the same Silver as the continuous run (see the checks below).")
    ic = _exp(run, "ingest_comparison")
    sq = _exp(run, "sql_serving")
    a, b = st.columns([2, 3])
    with a:
        if ic:
            st.markdown("##### Bronze loader")
            html_table(pd.DataFrame({
                "Loader": ["INSERT per line (baseline)", "COPY into staging (implemented)"],
                "Lines/s": [_num(ic["row_by_row_insert"]["lines_per_second"]),
                            _num(ic["copy_staging"]["lines_per_second"])],
                "Seconds": [_num(ic["row_by_row_insert"]["seconds"], 2), _num(ic["copy_staging"]["seconds"], 2)]}),
                numeric={"Lines/s", "Seconds"})
            st.caption(f"{fmt_int(ic['lines'])} lines in {ic['files']} files, one transaction per file in both. "
                       f"COPY is {ic['speedup']}× faster and stores identical rows.")
    with b:
        if sq.get("queries"):
            st.markdown("##### Serving queries: Silver at query time against the Gold mart")
            html_table(pd.DataFrame({
                "Query": [x["query"] for x in sq["queries"]],
                "Silver p50 / p95": [f"{_num(x['baseline_ms']['p50'], 1)} / {_num(x['baseline_ms']['p95'], 1)} ms"
                                     for x in sq["queries"]],
                "Gold p50 / p95": [f"{_num(x['optimized_ms']['p50'], 2)} / {_num(x['optimized_ms']['p95'], 2)} ms"
                                   for x in sq["queries"]],
                "Speed-up": [f"{x['speedup_p50']}×" for x in sq["queries"]],
                "Same answer": [badge(CHECK_STATE, str(x["checks"][0]["passed"])) for x in sq["queries"]]}),
                numeric={"Silver p50 / p95", "Gold p50 / p95", "Speed-up"}, raw={"Same answer"})
            st.caption(f"{sq['repetitions']} timed repetitions after {sq['warmup']} warm-up runs, over "
                       f"{fmt_int(sq['silver_rows'])} Silver events. The one-day query uses the (event_type, "
                       "event_time) index, so Silver stays usable for narrow ranges.")


def latency(run: pd.Series) -> None:
    ld = _exp(run, "load")
    cont = _exp(run, "continuous")
    if not ld.get("runs") and not cont:
        return
    st.markdown("#### Latency and backlog")
    rows = []
    if cont:
        lat = cont["latency_arrival_to_silver_ms"]
        rows.append({"Scenario": "File by file, no backlog (arrival → Silver)", "Arrival rate": "as fast as possible",
                     "p50": _num(lat["p50"], 0, " ms"), "p95": _num(lat["p95"], 0, " ms"),
                     "p99": _num(lat["p99"], 0, " ms"), "Backlog p95": "–", "Gold freshness": "–"})
    for r in ld.get("runs", []):
        lat = r["latency_bronze_to_silver_ms"]
        rows.append({"Scenario": f"{int(r['share_of_capacity'] * 100)}% of capacity (Bronze commit → Silver commit)",
                     "Arrival rate": f"{fmt_int(r['achieved_arrival_rows_per_second'])}/s "
                                     f"(target {fmt_int(r['target_rows_per_second'])})",
                     "p50": _num(lat["p50"], 0, " ms"), "p95": _num(lat["p95"], 0, " ms"),
                     "p99": _num(lat["p99"], 0, " ms"), "Backlog p95": f"{fmt_int(r['backlog_rows']['p95'])} rows",
                     "Gold freshness": _num(r.get("gold_freshness_seconds"), 1, " s")})
    html_table(pd.DataFrame(rows), numeric={"p50", "p95", "p99", "Backlog p95", "Gold freshness"})
    if ld.get("runs"):
        st.caption(f"Capacity = the processor's measured backfill rate ({fmt_int(ld['capacity_rows_per_second'])} "
                   "rows/s). Arrivals are paced file by file while the processor polls every "
                   f"{ld['consumer_idle_poll_ms']} ms when idle. Latency is weighted per row. Gold freshness = last "
                   "arrival committed until the revenue mart includes it (drain plus dbt run).")
        fig = go.Figure()
        for i, r in enumerate(ld["runs"]):
            series = r.get("backlog_series") or []
            fig.add_scatter(x=[p[0] for p in series], y=[p[1] for p in series], mode="lines",
                            name=f"{int(r['share_of_capacity'] * 100)}% of capacity",
                            line={"color": CATEGORICAL[i], "width": 2},
                            hovertemplate="%{x:.1f} s<br>%{y:,} rows waiting<extra>%{fullData.name}</extra>")
        fig.update_xaxes(title="seconds since the first arrival", title_font={"color": MUTED})
        chart(style(fig, height=260, y_title="rows waiting in Bronze"))


def profile_view(run: pd.Series) -> None:
    pr = _exp(run, "profile")
    if not pr.get("runs"):
        return
    st.markdown("#### Where the processor's time goes")
    cats = sorted({c["category"] for r in pr["runs"] for c in r["by_category"]})
    fig = go.Figure()
    for r in pr["runs"]:
        by = {c["category"]: c["seconds"] for c in r["by_category"]}
        name = "jsonschema validation (baseline)" if r["validator"] == "reference" else "compiled validation"
        fig.add_bar(y=cats, x=[by.get(c, 0) for c in cats], orientation="h", name=name,
                    marker_color=VALIDATOR_COLOR[r["validator"]],
                    hovertemplate="%{y}<br>%{x:.2f} s<extra>%{fullData.name}</extra>")
    fig.update_layout(barmode="group")
    fig.update_yaxes(autorange="reversed", gridcolor=SURFACE)
    fig.update_xaxes(showgrid=True, gridcolor=GRID, title="seconds of profiled time", title_font={"color": MUTED})
    a, b = st.columns([3, 2])
    with a:
        chart(style(fig, height=320))
    compiled = next((r for r in pr["runs"] if r["validator"] == "compiled"), pr["runs"][-1])
    reference = next((r for r in pr["runs"] if r["validator"] == "reference"), None)
    with b:
        top = compiled["by_category"][0]
        bf: dict[str, Any] = next((x for x in _exp(run, "backfill").get("runs", []) if x["validator"] == "compiled"),
                                  {})
        lines = [f"Profiled {fmt_int(compiled['lines'])} lines per run."]
        if reference:
            ref_top = reference["by_category"][0]
            lines.append(f"With jsonschema, {ref_top['category']} took {ref_top['share']:.0%} of the processor's "
                         "time.")
        lines.append(f"With compiled validation the largest share is {top['category']} ({top['share']:.0%}).")
        if bf:
            lines.append(f"In the backfill the worker used {bf.get('worker_cores')} CPU cores and Postgres "
                         f"{bf.get('postgres_cores')} cores. The processor is one Python process, so it cannot use "
                         "more than one core; a second processor would need a partitioned checkpoint.")
        st.markdown(" ".join(lines))
        st.caption(pr.get("note", ""))
        html_table(pd.DataFrame({"Function (compiled run)": [f["function"] for f in compiled["top_functions"][:8]],
                                 "Share": [f"{f['share']:.1%}" for f in compiled["top_functions"][:8]]}),
                   numeric={"Share"})


def recovery_view(run: pd.Series) -> None:
    rc = _exp(run, "recovery")
    if not rc:
        return
    st.markdown("#### Recovery after SIGKILL")
    tiles([
        ("Killed at", f"{rc['share_done_at_kill']:.0%}", f"checkpoint {fmt_int(rc['checkpoint_at_kill'])} of "
                                                           f"{fmt_int(rc['bronze_rows'])}"),
        ("Backlog at kill", fmt_int(rc["backlog_at_kill"]), "rows after the checkpoint"),
        ("Restart to drained", _num(rc["restart_to_drained_seconds"], 2, " s"), "new process, from the checkpoint"),
        ("Rate after restart", _num(rc["rows_per_second_after_restart"], 0, "/s"), "rows per second"),
    ])
    st.caption("The processor ran in its own OS process and was killed with SIGKILL, so nothing could flush or clean "
               "up. Its restart is manual here; a supervisor (Airflow, systemd, Kubernetes) would add its own "
               "detection delay.")


def downstream(run: pd.Series) -> None:
    gd, lh, tm, stg = (_exp(run, n) for n in ("gold", "leak_hunter", "time_machine", "storage"))
    if not any((gd, lh, tm, stg)):
        return
    st.markdown("#### Gold, Leak Hunter, Time Machine and storage")
    items = []
    if gd:
        summary = gd["dbt_build"].get("summary") or {}
        items.append(("dbt build", _num(gd["dbt_build"]["seconds"], 1, " s"),
                      f"{summary.get('pass', '–')} models and tests, full rebuild"))
        items.append(("Revenue path refresh", _num(gd["gold_refresh_revenue_path"]["seconds"], 1, " s"),
                      "dbt run +mart_telecom_revenue_daily"))
    if lh:
        items.append(("Leak Hunter run", _num(lh["seconds"], 2, " s"), f"{lh['rules']} rules over all of Silver"))
    if tm:
        items.append(("Time Machine preview", _num(tm["records_per_second"], 0, "/s"),
                      f"{fmt_int(tm['records'])} records, {tm['window_hours']:.0f} h window"))
    if stg:
        items.append(("Database size", _bytes(stg["database_bytes"]),
                      f"{stg['database_to_file_ratio']}× the arrival files"))
        items.append(("Silver per event", _bytes(stg["silver_bytes_per_event"]), "with indexes"))
    tiles(items)


def checks_view(run: pd.Series) -> None:
    exps = (run.results or {}).get("experiments", {})
    rows = [(EXPERIMENT_TITLES.get(name, name), c) for name, r in exps.items() for c in r.get("checks", [])]
    if not rows:
        return
    failed = sum(not c["passed"] for _, c in rows)
    st.markdown("#### Correctness checks")
    st.caption(f"{len(rows) - failed} of {len(rows)} passed. Expected values come from the generator's records "
               "(written when the data was generated, never read by the pipeline) or from the continuous run of "
               "the same data.")
    with st.expander("All checks", expanded=bool(failed)):
        html_table(pd.DataFrame({
            "Experiment": [e for e, _ in rows], "Check": [c["check"] for _, c in rows],
            "Expected": [_short(c["expected"]) for _, c in rows], "Observed": [_short(c["observed"]) for _, c in rows],
            "Result": [badge(CHECK_STATE, str(c["passed"])) for _, c in rows]}), raw={"Result"})


def methodology(run: pd.Series) -> None:
    with st.expander("Method and limits"):
        params = (run.config or {}).get("params", {})
        st.markdown(f"""
* **Data:** generated by the SignalFlow generator with the workload's configuration
  (`{json.dumps((run.config or {}).get("workload", {}), default=str)}`), including duplicates, late arrivals,
  schema breaks and injected business anomalies. Volume is the number of arrival lines actually processed.
* **Isolation:** each experiment runs in a dedicated benchmark database (`signalflow_bench*`), created and dropped by
  the suite. The demo database only receives the run record.
* **Continuous:** each arrival file is loaded into Bronze and the backlog processed before the next file, as the
  lite profile does. Latency is per line, from the start of the file's load to the commit of its last Silver batch.
* **Backfill:** the Silver processor runs as its own OS process over a copy of a fully loaded Bronze. Chunk sizes
  {params.get("chunks")}; baseline is jsonschema-only validation at chunk {params.get("chunk_size")}.
* **Load:** arrivals are paced at {params.get("load_shares")} of the measured capacity for about
  {params.get("load_seconds")} s each while the processor polls. Latency per row is the Silver commit time of its
  batch minus the Bronze commit time of its file, both read from the Postgres clock.
* **Recovery:** the worker is killed with SIGKILL after about {int(float(params.get("kill_at", 0)) * 100)}% of the
  backlog, then restarted; the result must equal the continuous run.
* **Limits:** one machine, one Postgres instance with default settings, all components on the same host. The
  streaming profile (Kafka, Spark) and the lake are not part of this suite. Numbers describe this machine only.
""")
