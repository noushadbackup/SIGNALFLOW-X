"""Chaos Lab page: run a predefined incident in the disposable lab database, follow its phases, read its report.

Runs and reports come from ``ops.chaos_runs`` in the control tower's database; the incident itself runs in the lab
database (``SIGNALFLOW_LAB_DB``), which the runner rebuilds and which is the only database it modifies.
"""

from __future__ import annotations

import json
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from signalflow.chaos import lab
from signalflow.chaos.incidents import INCIDENTS
from signalflow.db import connect
from signalflow.security import PermissionDenied
from signalflow.ui import auth, data
from signalflow.ui.components import (
    CATEGORICAL,
    CRITICAL,
    GOOD,
    MUTED,
    WARNING,
    badge,
    chart,
    fmt_ts,
    header,
    html_table,
    q,
    qp,
    source,
    style,
    tiles,
)

RUN_STATE = {"PASSED": ("✓", GOOD, "Passed"), "FAILED": ("✕", CRITICAL, "Failed"),
             "ERROR": ("!", CRITICAL, "Error"), "RUNNING": ("◷", WARNING, "Running")}
CHECK_STATE = {"True": ("✓", GOOD, "Pass"), "False": ("✕", CRITICAL, "Fail")}
RUNS = """SELECT run_id, incident, title, status, phase, operator, operator_role, started_at, finished_at,
                 extract(epoch FROM finished_at - started_at) AS seconds
          FROM ops.chaos_runs ORDER BY started_at DESC LIMIT 30"""
REPORT = "SELECT report FROM ops.chaos_runs WHERE run_id = %(run_id)s"


def _short(v: Any, limit: int = 90) -> str:
    text = v if isinstance(v, str) else json.dumps(v, default=str)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def page() -> None:
    header("Trust", "Chaos Lab",
           "Five predefined incidents, each injected into a lab database rebuilt from a deterministic synthetic "
           "dataset, observed, recovered and verified against a clean run of the same data.")
    if not data.table_exists("ops.chaos_runs"):
        st.info("Apply the Phase 5 migration first: `signalflow migrate`.")
        return
    runs = q(RUNS)
    passed = int((runs.status == "PASSED").sum()) if not runs.empty else 0
    tiles([("Incident runs", f"{len(runs):,}", "latest 30"),
           ("Passed verification", f"{passed:,}", "all checks equal to the clean run"),
           ("Failed or errored", f"{int(runs.status.isin(['FAILED', 'ERROR']).sum()) if not runs.empty else 0:,}",
            "report says which check"),
           ("Lab database", lab.lab_database_name(), "the only database an incident modifies")])

    st.markdown("#### Incidents")
    html_table(pd.DataFrame([{"": code, "Incident": i.title, "Expected behaviour": " · ".join(i.expected_behaviour),
                              "Recovery": i.recovery_kind} for code, i in INCIDENTS.items()]))
    run_panel()

    st.markdown("#### Runs")
    if runs.empty:
        st.caption("No incident has been run against this database yet.")
        return
    html_table(pd.DataFrame({
        "Run": runs.run_id, "Incident": runs.incident + " · " + runs.title,
        "Status": [badge(RUN_STATE, s) for s in runs.status],
        "Operator": runs.operator + " (" + runs.operator_role + ")",
        "Started": [fmt_ts(t, "%d %b %H:%M:%S") for t in runs.started_at],
        "Seconds": [f"{s:,.1f}" if pd.notna(s) else "–" for s in runs.seconds],
    }), numeric={"Seconds"}, raw={"Status"})
    source("ops.chaos_runs")
    choice = st.selectbox("Incident report", runs.run_id.tolist(),
                          format_func=lambda r: f"{r} · {runs.set_index('run_id').status[r]}")
    rep = qp(REPORT, (("run_id", choice),))
    if not rep.empty:
        report_view(rep.report.iloc[0] if isinstance(rep.report.iloc[0], dict) else json.loads(rep.report.iloc[0]))


def run_panel() -> None:
    op = auth.current()
    with st.container(border=True):
        st.markdown("##### Run an incident")
        if op is None or not op.can("chaos.run"):
            st.caption("Running an incident needs an operator account (sidebar). Each run and each recovery is "
                       "authorised again in the database layer and written to the audit log.")
            return
        with st.form("chaos_run"):
            code = st.selectbox("Incident", list(INCIDENTS), format_func=lambda c: f"{c} · {INCIDENTS[c].title}")
            go_ = st.form_submit_button("Rebuild the lab and run it")
        if go_:
            from signalflow.chaos.runner import run

            with st.spinner(f"Incident {code}: rebuilding the lab, injecting, recovering, verifying…"):
                try:
                    with connect() as control:
                        report = run(control, code, op)
                except PermissionDenied as exc:
                    st.error(str(exc))
                    return
            st.cache_data.clear()
            (st.success if report["status"] == "PASSED" else st.error)(
                f"{report['run_id']}: {report['status']}")


def report_view(r: dict[str, Any]) -> None:
    st.markdown(f"##### {r['incident']} · {r['title']}  {badge(RUN_STATE, r['status'])}", unsafe_allow_html=True)
    lab_info = r.get("lab", {})
    tiles([("Operator", r["operator"]["username"], r["operator"]["role"]),
           ("Dataset", r["dataset"]["key"], "deterministic generator config"),
           ("Lab build", f"{lab_info.get('seconds', 0):,.1f} s",
            f"{lab_info.get('held_back_records', 0):,} records held back for the incident"),
           ("Checks", f"{sum(c['passed'] for c in r.get('verification', []))} / {len(r.get('verification', []))}",
            "passed")])
    if r.get("error"):
        st.error(f"{r['error']['type']}: {r['error']['message']}")

    st.markdown("###### Verification against the clean run")
    checks = r.get("verification", [])
    if checks:
        html_table(pd.DataFrame({"Check": [c["name"] for c in checks],
                                 "Expected": [_short(c["expected"]) for c in checks],
                                 "Actual": [_short(c["actual"]) for c in checks],
                                 "Result": [badge(CHECK_STATE, str(bool(c["passed"]))) for c in checks]}),
                   raw={"Result"})
    st.markdown("###### Timeline")
    tl = pd.DataFrame(r.get("timeline", []))
    if not tl.empty and "seconds" in tl:
        fig = go.Figure(go.Bar(y=tl.phase, x=tl.seconds, orientation="h", marker_color=CATEGORICAL[0],
                               hovertemplate="%{y}: %{x:.2f} s<extra></extra>"))
        fig = style(fig, height=max(200, 26 * len(tl) + 60), legend=False)
        fig.update_yaxes(autorange="reversed", gridcolor="rgba(0,0,0,0)")
        fig.update_xaxes(title="seconds", showgrid=True)
        chart(fig)
    alerts = r.get("alerts", [])
    st.markdown("###### Diagnostic alerts raised during the incident")
    if alerts:
        html_table(pd.DataFrame({"Rule": [a["rule"] for a in alerts], "Subject": [a["subject"] for a in alerts],
                                 "Severity": [a["severity"] for a in alerts],
                                 "Summary": [a["summary"] for a in alerts]}))
    else:
        st.caption("No diagnostic alert fired during this incident. A stock discrepancy (E) is reported through "
                   "Leak Hunter cases, not alerts; see the evidence below.")
    for title, key in (("What was injected", "injected"), ("What was observed", "observed"),
                       ("Evidence", "evidence"), ("Recovery", "recovery")):
        with st.expander(title):
            st.json(r.get(key, {}), expanded=False)
    st.caption(" ".join(r.get("notes", [])))
    st.markdown(f'<span style="color:{MUTED};font-size:12px">Expected behaviour: '
                f'{" · ".join(r.get("expected_behaviour", []))}</span>', unsafe_allow_html=True)
