"""Revenue Leak Hunter page: discrepancy trends, affected business units, estimated exposure, time to clear, and a
case inspector with the evidence rows, the raw source records and the case history. Analysts act on cases here."""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from signalflow.assurance import cases as case_actions
from signalflow.assurance.rules import BY_ID, HORIZON
from signalflow.db import connect
from signalflow.security import PermissionDenied
from signalflow.ui import auth, data
from signalflow.ui.components import (
    CASE_STATE,
    GRID,
    MUTED,
    SURFACE,
    badge,
    case_state,
    chart,
    fmt_bdt,
    fmt_int,
    fmt_ts,
    header,
    html_table,
    provenance,
    q,
    qp,
    source,
    style,
    tiles,
)

DOMAINS = {"All": None, "Telecom": "telecom", "Manufacturing": "mfg"}
STATE_ORDER = ["CONFIRMED_BY_RULE", "SUSPICIOUS", "PENDING", "RESOLVED_BY_ANALYST", "CLEARED_BY_DATA"]


def _span(td: timedelta) -> str:
    """15 min, 2 h, 36 h, 3 d, 5 d 4 h: rule windows as an analyst reads them."""
    minutes = int(td.total_seconds() // 60)
    if minutes == 0:
        return "immediately"
    if minutes < 60:
        return f"{minutes} min"
    hours = minutes // 60
    if hours <= 48:
        return f"{hours} h"
    days, rest = divmod(hours, 24)
    return f"{days} d" + (f" {rest} h" if rest else "")


def _hours(v: object) -> str:
    return "–" if v is None or pd.isna(v) else f"{float(v):,.1f} h"  # type: ignore[arg-type]


def page() -> None:
    header("Trust", "Revenue Leak Hunter",
           "Ten reconciliation rules over committed Silver. A gap is pending while it is within normal processing "
           "time, suspicious after that, and confirmed by rule only once the late-data horizon has passed.")
    if not data.table_exists("assurance.hunter_runs") or q(data.LEAK_LAST_RUN).empty:
        st.info("The Leak Hunter has not run yet. `signalflow run` hunts after every arrival file; on the streaming "
                "database run `signalflow assurance hunt` (Airflow does it every 10 minutes).")
        return
    last = q(data.LEAK_LAST_RUN).iloc[0]
    st.markdown(f'<div class="sf-meta">Run #{int(last.run_id)} at {fmt_ts(last.finished_at)} Dhaka · telecom clock '
                f'{fmt_ts(last.telecom_clock)} · manufacturing clock {fmt_ts(last.mfg_clock)} · ages are measured '
                f'on these event-time clocks, not on the wall clock</div>', unsafe_allow_html=True)

    t = q(data.LEAK_TOTALS).iloc[0]
    tiles([
        ("Confirmed by rule", fmt_int(t.confirmed), f"{fmt_bdt(t.confirmed_exposure)} estimated exposure"),
        ("Suspicious", fmt_int(t.suspicious), f"{fmt_bdt(t.suspicious_exposure)} at risk if not cleared"),
        ("Pending", fmt_int(t.pending), "within normal processing time"),
        ("Cleared by data", fmt_int(t.auto_cleared), f"median {_hours(t.median_hours_to_clear)} of stream time"),
        ("Resolved by analysts", fmt_int(t.analyst_resolved), "with a note and a resolution code"),
    ])
    provenance("estimate")

    f1, f2, f3 = st.columns([1.2, 2.2, 1.6])
    domain_label = f1.segmented_control("Domain", list(DOMAINS), default="All", key="lh_domain") or "All"
    domain = DOMAINS[domain_label]
    states = f2.multiselect("State", STATE_ORDER, default=STATE_ORDER[:3], key="lh_states",
                            format_func=lambda s: CASE_STATE[s][2])
    rules = q(data.LEAK_RULES)
    rule_options = ["All rules"] + [r for r in rules.rule_id if domain is None or BY_ID[r].domain == domain]
    rule_pick = f3.selectbox("Rule", rule_options, key="lh_rule",
                             format_func=lambda r: r if r == "All rules" else f"{r} · {BY_ID[r].title}")
    rule = None if rule_pick == "All rules" else rule_pick

    left, right = st.columns([1.25, 1])
    with left:
        st.markdown("#### Discrepancy trend by business date")
        trend = q(data.LEAK_TREND)
        if domain:
            trend = trend[trend.domain == domain]
        trend = trend[trend.state.isin(states)].groupby(["business_date", "state"], as_index=False)["cases"].sum()
        fig = go.Figure()
        for s in reversed(STATE_ORDER):
            d = trend[trend.state == s]
            if len(d):
                fig.add_bar(x=d.business_date.astype(str), y=d.cases.astype(float), name=CASE_STATE[s][2],
                            marker_color=CASE_STATE[s][1],
                            hovertemplate="%{x}<br>%{fullData.name}: %{y:,} cases<extra></extra>")
        fig.update_layout(barmode="stack", legend={"traceorder": "reversed"})
        chart(style(fig, height=320, y_title="cases").update_xaxes(categoryorder="category ascending"))
        st.caption("Business date of the transaction that anchors each case, for the states selected above. Most "
                   "cases clear by themselves: their counterpart was still in transit when the hunter ran.")
        source("assurance.cases")
    with right:
        st.markdown("#### Open exposure by business unit")
        units = q(data.LEAK_BY_UNIT)
        if domain:
            units = units[units.domain == domain]
        if units.empty:
            st.info("No suspicious or confirmed case in this selection.")
        else:
            order = (units.groupby("business_unit")["exposure_bdt"].sum().astype(float).sort_values().index.tolist())
            fig = go.Figure()
            for s in ("CONFIRMED_BY_RULE", "SUSPICIOUS"):
                d = units[units.status == s].set_index("business_unit").reindex(order)
                fig.add_bar(y=order, x=d.exposure_bdt.astype(float).fillna(0), orientation="h", name=CASE_STATE[s][2],
                            marker_color=CASE_STATE[s][1], customdata=d.cases.fillna(0),
                            hovertemplate="%{y}<br>%{fullData.name}: ৳%{x:,.2f} · %{customdata:,} cases<extra></extra>")
            fig.update_layout(barmode="stack")
            fig.update_xaxes(showgrid=True, gridcolor=GRID, title="BDT (estimate)", title_font={"color": MUTED})
            fig.update_yaxes(gridcolor=SURFACE)
            chart(style(fig, height=max(320, 34 * len(order) + 90)))
        source("assurance.cases")

    st.markdown("#### Rules")
    shown = rules if domain is None else rules[rules.domain == domain]
    lateness = q(data.OBSERVED_LATENESS).set_index("stream")
    html_table(pd.DataFrame({
        "Rule": shown.rule_id, "What it checks": shown.title,
        "Pending until": [_span(BY_ID[r].expected_within) for r in shown.rule_id],
        "Confirmed after": [_span(BY_ID[r].confirm_after) for r in shown.rule_id],
        "Pending": [fmt_int(v) for v in shown.pending], "Suspicious": [fmt_int(v) for v in shown.suspicious],
        "Confirmed": [fmt_int(v) for v in shown.confirmed], "Cleared by data": [fmt_int(v) for v in shown.auto_cleared],
        "Open exposure": [fmt_bdt(v) for v in shown.exposure_bdt],
        "Time to clear (median / max)": [f"{_hours(a)} / {_hours(b)}" if not pd.isna(a) else "–"
                                         for a, b in zip(shown.median_hours_to_clear, shown.max_hours_to_clear,
                                                         strict=True)],
    }), numeric={"Pending", "Suspicious", "Confirmed", "Cleared by data", "Open exposure",
                 "Time to clear (median / max)"})
    notes = []
    for stream, horizon in HORIZON.items():
        if stream in lateness.index:
            observed = float(lateness.loc[stream, "max_lateness_hours"])
            ok = observed < horizon.total_seconds() / 3600
            notes.append(f"{stream}: largest lateness observed {observed:.1f} h, horizon "
                         f"{horizon.total_seconds() / 3600:.0f} h {'✓' if ok else '✕ — raise the horizon'}")
    st.caption("Time to clear is measured on the stream clock from first detection to the arrival of the missing "
               "counterpart. Late-data horizon check · " + " · ".join(notes))
    source("assurance.rules", "assurance.cases", "silver.events (lateness)", "sql/assurance/*.sql")

    st.markdown("#### Cases")
    params = (("domain", domain), ("states", states), ("rule", rule))
    cases = qp(data.LEAK_CASES, params)
    if cases.empty:
        st.info("No case matches the filters.")
    else:
        view = pd.DataFrame({
            "Case": cases.case_id,
            "State": [CASE_STATE[case_state(s, r)][0] + " " + CASE_STATE[case_state(s, r)][2]
                      for s, r in zip(cases.status, cases.resolution, strict=True)],
            "Business unit": cases.business_unit, "Business date": cases.business_date.astype(str),
            "Exposure (BDT)": cases.exposure_bdt.astype(float), "Summary": cases.summary,
            "Assignee": cases.assignee.fillna(""),
        })
        picked = st.dataframe(view, hide_index=True, use_container_width=True, on_select="rerun",
                              selection_mode="single-row", key="lh_cases", height=300,
                              column_config={"Exposure (BDT)": st.column_config.NumberColumn(format="৳ %.2f"),
                                             "Summary": st.column_config.TextColumn(width="large")})
        rows = list(picked["selection"]["rows"]) if picked else []
        st.caption(f"{len(cases):,} cases shown (most severe first). Select a row to inspect it.")
        inspect(str(cases.case_id.iloc[rows[0] if rows else 0]))

    evaluation_panel()


def inspect(case_id: str) -> None:
    case = data.query(data.LEAK_CASE, {"case_id": case_id})
    if case.empty:
        return
    c = case.iloc[0]
    rule = BY_ID[c.rule_id]
    state = case_state(c.status, c.resolution)
    evidence = c.evidence if isinstance(c.evidence, dict) else json.loads(c.evidence)
    st.markdown(f"#### Case {case_id}")
    st.markdown(f"{badge(CASE_STATE, state)} &nbsp; **{rule.rule_id} · {rule.title}** &nbsp; "
                f"<span style='color:{MUTED}'>{c.business_unit} · version {int(c.version)}</span>",
                unsafe_allow_html=True)
    st.markdown(c.summary)
    tiles([
        ("Estimated exposure", fmt_bdt(c.exposure_bdt), rule.exposure_kind.replace("_", " ").lower()),
        ("Anchor (event time)", fmt_ts(c.anchor_time), "windows are measured from here"),
        ("First detected", fmt_ts(c.first_detected_as_of), "stream clock"),
        ("Confirmed", fmt_ts(c.confirmed_as_of), "stream clock") if c.confirmed_as_of is not None and not
        pd.isna(c.confirmed_as_of) else ("Confirms at", fmt_ts(c.anchor_time + rule.confirm_after),
                                          "if still unmatched"),
    ])

    a, b = st.columns([1.3, 1])
    with a:
        st.markdown("##### Evidence")
        tx = pd.DataFrame(evidence.get("transactions", []))
        if len(tx):
            cols = [col for col in ("role", "event_type", "id", "event_time", "amount_bdt", "quantity", "status")
                    if col in tx.columns]
            show = tx[cols].copy()
            if "event_time" in show:
                show["event_time"] = [fmt_ts(pd.Timestamp(v), "%d %b %H:%M:%S") for v in show.event_time]
            html_table(show.fillna(""), numeric={"amount_bdt", "quantity"})
        facts = evidence.get("facts", {})
        if facts:
            st.caption(" · ".join(f"{k.replace('_', ' ')}: {v}" for k, v in facts.items()))
        ids = [t["event_id"] for t in evidence.get("transactions", []) if t.get("event_id")]
        if ids:
            with st.expander("Raw source records (Bronze, byte for byte)"):
                raw = data.query(data.EVIDENCE_RAW, {"ids": ids})
                for r in raw.itertuples():
                    st.caption(f"{r.event_type} · {r.source_stream} {r.source_position} · arrived "
                               f"{fmt_ts(r.arrival_time, '%d %b %H:%M:%S')} · {r.arrival_class}")
                    st.code(r.raw_record, language="json", wrap_lines=True)
        source(f"sql/assurance/{rule.sql_file}", "silver.events", "bronze.raw_events")
    with b:
        st.markdown("##### History")
        hist = data.query(data.LEAK_CASE_EVENTS, {"case_id": case_id})
        html_table(pd.DataFrame({
            "When": [fmt_ts(r.as_of) + " (stream)" if r.as_of is not None and not pd.isna(r.as_of)
                     else fmt_ts(r.at) for r in hist.itertuples()],
            "Action": hist.action.str.replace("_", " ").str.lower(),
            "State": [f"{x or '–'} → {y or '–'}".replace("_", " ").lower()
                      for x, y in zip(hist.from_status, hist.to_status, strict=True)],
            "By": hist.actor, "Note": hist.note.fillna(""),
        }))
        source("assurance.case_events")
        actions(c)


def actions(c: Any) -> None:
    op = auth.current()
    st.markdown("##### Act on this case")
    if op is None or not op.can("case.note"):
        st.caption("Sign in as an analyst (sidebar) to add notes, assign or resolve. Viewers can read everything.")
        return
    case_id, version = str(c.case_id), int(c.version)

    def run(fn: Any, *args: Any) -> None:
        try:
            with connect() as conn:
                fn(conn, op, case_id, *args)
        except (PermissionDenied, case_actions.CaseConflict, ValueError, KeyError) as exc:
            st.error(str(exc))
            return
        st.cache_data.clear()
        st.rerun()

    tab_note, tab_assign, tab_resolve = st.tabs(["Note", "Assign", "Resolve / reopen"])
    with tab_note, st.form(f"note_{case_id}", clear_on_submit=True):
        note = st.text_area("Note", placeholder="What you checked and what you found")
        if st.form_submit_button("Add note"):
            run(case_actions.add_note, note)
    with tab_assign, st.form(f"assign_{case_id}"):
        who = st.text_input("Assignee", value=c.assignee or op.username)
        if st.form_submit_button("Assign"):
            run(case_actions.assign, who or None, version)
    with tab_resolve:
        if c.status == "RESOLVED" and c.resolution != "AUTO_CLEARED":
            with st.form(f"reopen_{case_id}"):
                note = st.text_input("Why reopen")
                if st.form_submit_button("Reopen"):
                    run(case_actions.reopen, note, version)
        elif c.status == "RESOLVED":
            st.caption("Cleared by data: the missing counterpart arrived. Nothing to resolve.")
        else:
            with st.form(f"resolve_{case_id}"):
                resolution = st.selectbox("Resolution", case_actions.RESOLUTIONS,
                                          format_func=lambda r: r.replace("_", " ").title())
                note = st.text_area("Evidence and outcome (required)")
                if st.form_submit_button("Resolve"):
                    run(case_actions.resolve, resolution, note, version)


def evaluation_panel() -> None:
    st.markdown("#### Detection quality (offline evaluation)")
    ev = q(data.LEAK_LAST_EVALUATION) if data.table_exists("assurance.evaluation_runs") else pd.DataFrame()
    if ev.empty:
        st.caption("No evaluation recorded for this database. Run `python -m signalflow_eval.leak_hunter --data <dir>` "
                   "after a pipeline run to compare cases with the generator's injected scenarios.")
        return
    e = ev.iloc[0]
    results: dict[str, Any] = e.results if isinstance(e.results, dict) else json.loads(e.results)
    benign: dict[str, Any] = e.benign_checks if isinstance(e.benign_checks, dict) else json.loads(e.benign_checks)
    dataset = e.dataset if isinstance(e.dataset, dict) else json.loads(e.dataset)
    st.caption(f"Evaluated {fmt_ts(e.created_at)} on seed {dataset['seed']}, {dataset['days']} days. The scenario "
               "labels are read only by the separate evaluation package, never by the rules.")
    rows = []
    for rid, r in results.items():
        if r.get("scenario") is None:
            rows.append({"Rule": rid, "Scenario": "none (delay detector)", "Injected": "–", "Confirmed": fmt_int(
                r["confirmed"]), "False confirmations": "–", "Precision": "–", "Recall": "–",
                "Not yet confirmable": "–", "Missed": "–", "Cleared by data": fmt_int(r["auto_cleared"])})
            continue
        rows.append({
            "Rule": rid, "Scenario": r["scenario"].replace("_", " ").lower(), "Injected": fmt_int(r["positives"]),
            "Confirmed": fmt_int(r["confirmed_tp"]), "False confirmations": fmt_int(r["confirmed_fp"]),
            "Precision": "–" if r["precision_confirmed"] is None else f"{r['precision_confirmed']:.0%}",
            "Recall": "–" if r["recall_confirmed"] is None else f"{r['recall_confirmed']:.0%}",
            "Not yet confirmable": fmt_int(r["open_positives"]), "Missed": fmt_int(r["missed"]),
            "Cleared by data": fmt_int(r["auto_cleared"])})
    html_table(pd.DataFrame(rows), numeric={"Injected", "Confirmed", "False confirmations", "Precision", "Recall",
                                            "Not yet confirmable", "Missed", "Cleared by data"})
    delayed = benign.get("DELAYED_BILLING", {})
    late_pay = benign.get("DELAYED_DEALER_PAYMENT", {})
    ok = e.passed
    st.markdown(
        f"{badge({'1': ('✓', CASE_STATE['CLEARED_BY_DATA'][1], 'Passed'), '0': ('✕', CASE_STATE['CONFIRMED_BY_RULE'][1], 'Failed')}, '1' if ok else '0')}"
        f" &nbsp; Delayed billing: {delayed.get('scenarios', 0)} injected, {delayed.get('cases_suspicious_at_some_point', 0)} "
        f"seen as suspicious, {delayed.get('confirmed', 0)} confirmed · late dealer payments: "
        f"{late_pay.get('scenarios', 0)} injected, {late_pay.get('confirmed', 0)} confirmed · cases confirmed and later "
        f"cleared by data: {benign.get('confirmed then cleared by data', 0)}", unsafe_allow_html=True)
    st.caption("Recall counts only confirmed cases; scenarios whose horizon had not passed at the end of the data are "
               "'not yet confirmable'. MFG-05 'missed' are invoices not yet due.")
    source("assurance.evaluation_runs (written by signalflow_eval)")
