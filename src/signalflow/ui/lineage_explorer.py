"""Lineage Explorer page: what depends on a source, what a KPI depends on, what a stopped job leaves stale, and what
each firing alert is likely to affect.

The graph is ``signalflow.lineage.graph.build()``: derived from the contract registry, the topic router, the dbt
manifest, the rule SQL and the dashboard code, at table level. Freshness and alerts are read live from Postgres.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st

from signalflow.db import connect
from signalflow.lineage import graph as lg
from signalflow.lineage.catalog import JOBS, KPIS, Profile
from signalflow.lineage.status import alert_impacts, freshness
from signalflow.ui.components import fmt_int, fmt_ts, header, html_table, tiles

KIND_ORDER = ("source", "event", "topic", "bronze", "silver", "table", "model", "rule", "kpi", "dashboard")
DRAW_LIMIT = 70  # beyond this a drawing is a hairball; the tables below list every node


@st.cache_resource(show_spinner="Building the lineage graph from contracts, dbt, rules and dashboard code…")
def _graph() -> lg.Graph:
    return lg.build()


def _fresh_text(f: dict[str, Any] | None) -> str:
    if not f:
        return "–"
    parts = []
    if "rows" in f:
        parts.append(f"{fmt_int(f['rows'])} rows")
    for key, label in (("data_through", "data through"), ("max_event_time", "events to"),
                       ("last_ingested", "ingested"), ("last_hunt", "hunted"), ("watermark", "watermark")):
        if f.get(key) is not None:
            v = f[key]
            parts.append(f"{label} {fmt_ts(v, '%d %b %H:%M') if isinstance(v, datetime) else v}")
    if "open_cases" in f:
        parts.append(f"{fmt_int(f['open_cases'])} open cases")
    return " · ".join(parts)


def page() -> None:
    header("Platform", "Lineage and impact",
           "Which KPIs and dashboards depend on a source, a table or a job, and what a figure depends on when it "
           "looks wrong. Derived from the contract registry, Kafka topic router, dbt manifest, Leak Hunter rule SQL "
           "and dashboard code; nothing is drawn by hand.")
    g = _graph()
    kinds = {k: len(g.of_kind(k)) for k in KIND_ORDER}
    tiles([("Nodes", fmt_int(len(g.nodes)), ", ".join(f"{v} {k}" for k, v in kinds.items() if k in
                                                       ("event", "model", "kpi", "dashboard"))),
           ("Edges", fmt_int(len(g.edges)), "each names the job that moves the data"),
           ("Jobs", fmt_int(len(JOBS)), "lite and stream profiles"),
           ("Granularity", "Table", "column-level lineage is not captured")])
    profile: Profile = st.radio("Profile", ["lite", "stream"], horizontal=True,
                                format_func=lambda p: {"lite": "lite (files, Python processor)",
                                                       "stream": "stream (Kafka, Debezium, Spark)"}[p])
    with connect() as conn:
        fresh = freshness(conn, g)
        alerts = alert_impacts(conn, g, profile)
    tabs = st.tabs(["Impact of a change", "Root cause of a figure", "A job stops", f"Firing alerts ({len(alerts)})",
                    "KPI catalogue"])
    with tabs[0]:
        node = node_picker(g, "impact", "event:telecom.recharge", ("source", "event", "topic", "bronze", "silver",
                                                                   "table", "model", "rule"))
        reach = lg.downstream(g, node, profile)
        reach_view(g, reach, profile, fresh, start=node, forward=True)
    with tabs[1]:
        node = node_picker(g, "upstream", "kpi:gross_recharge_revenue", ("kpi", "dashboard", "model", "rule"))
        reach = lg.upstream(g, node, profile)
        reach_view(g, reach, profile, fresh, start=node, forward=False)
    with tabs[2]:
        jobs = [j for j in JOBS if profile in JOBS[j].profiles]
        job = st.selectbox("Job", jobs, index=jobs.index("silver_processor" if profile == "lite" else "spark_silver"),
                           format_func=lambda j: f"{JOBS[j].label} · {j}")
        st.caption(JOBS[job].detail)
        reach = lg.job_failure(g, job, profile)
        reach_view(g, reach, profile, fresh, start=None, forward=True)
    with tabs[3]:
        alerts_view(g, alerts)
    with tabs[4]:
        html_table(pd.DataFrame({"KPI": [k.label for k in KPIS], "Domain": [k.domain for k in KPIS],
                                 "Computed from": [f"{k.model}.{k.column}" for k in KPIS],
                                 "Definition": [k.definition for k in KPIS],
                                 "Freshness": [_fresh_text(fresh.get(k.model if ':' in k.model else f'model:{k.model}'))
                                               for k in KPIS]}))
    st.caption(" ".join(g.notes) + " Operator tools (Chaos Lab, Time Machine, this page) are not modelled as "
               "dashboards.")


def node_picker(g: lg.Graph, key: str, default: str, kinds: tuple[str, ...]) -> str:
    c1, c2 = st.columns([1, 3])
    kind = c1.selectbox("Kind", kinds, index=kinds.index(g.nodes[default].kind), key=f"{key}_kind")
    options = [n.id for n in g.of_kind(kind)]
    node = c2.selectbox("Node", options, index=options.index(default) if default in options else 0,
                        format_func=lambda n: g.nodes[n].label if g.nodes[n].label != n else n, key=f"{key}_node")
    n = g.nodes[node]
    st.caption(f"{n.detail} Source of this node: {n.evidence}." if n.detail else f"Source of this node: {n.evidence}.")
    return node


def reach_view(g: lg.Graph, reach: dict[str, lg.Reach], profile: Profile, fresh: dict[str, dict[str, Any]],
               start: str | None, forward: bool) -> None:
    others = {n: r for n, r in reach.items() if n != start}
    if not others:
        st.info("Nothing in this profile depends on it." if forward else "It has no upstream dependency here.")
        return
    summary = lg.impact_summary(g, others)
    def count(*kinds: str) -> str:
        return fmt_int(sum(len(summary.get(k, [])) for k in kinds))

    stored = ("Stored data", count("topic", "bronze", "silver", "table"), "topics, Bronze, Silver and ops tables")
    if forward:
        tiles([stored, ("dbt models", count("model"), "staging and Gold"), ("KPIs", count("kpi"), "business figures"),
               ("Dashboards", count("dashboard"), "control tower pages and Superset charts")])
    else:
        tiles([("Sources and event types", count("source", "event"), "where the data starts"), stored,
               ("dbt models", count("model"), "staging and Gold"), ("Rules", count("rule"), "Leak Hunter rules")])
    if len(reach) <= DRAW_LIMIT:
        hot = [n for n, r in reach.items() if r.distance == 0]  # the start, or the nodes a stopped job feeds
        st.graphviz_chart(lg.to_dot(g, reach, profile, highlight=hot, start=start, edge_labels=len(reach) <= 25),
                          use_container_width=True)
        if len(reach) > 25:
            st.caption("Job names are left off the edges at this size; the table lists the jobs on each path.")
    else:
        st.caption(f"{len(reach)} nodes: too many to draw legibly. Every one is listed below.")
    ends = [n for k in (("kpi", "dashboard") if forward else ("source", "event")) for n in summary.get(k, [])]
    if ends:
        st.markdown("###### " + ("Affected KPIs and dashboards" if forward else "Root sources"))
        html_table(pd.DataFrame({
            "Node": [g.nodes[n].label for n in ends], "Kind": [g.nodes[n].kind for n in ends],
            "Shortest path": [" → ".join(others[n].path if forward else tuple(reversed(others[n].path)))
                              for n in ends],
            "Jobs on the path": [", ".join(others[n].jobs) for n in ends]}))
    with st.expander(f"All {len(others)} {'dependent' if forward else 'upstream'} nodes, with live freshness"):
        rows = sorted(others, key=lambda n: (KIND_ORDER.index(g.nodes[n].kind), n))
        html_table(pd.DataFrame({"Node": rows, "Kind": [g.nodes[n].kind for n in rows],
                                 "Hops": [others[n].distance for n in rows],
                                 "Freshness": [_fresh_text(fresh.get(n)) for n in rows]}), numeric={"Hops"})


def alerts_view(g: lg.Graph, alerts: list[Any]) -> None:
    if not alerts:
        st.caption("No diagnostic alert is firing in this database.")
        return
    for a in alerts:
        al = a.alert
        kpis, dashboards = a.affected(g, "kpi"), a.affected(g, "dashboard")
        with st.expander(f"{al['severity']} · {al['rule']} · {al['subject']} — {len(kpis)} KPIs, "
                         f"{len(dashboards)} dashboards"):
            st.write(al["summary"])
            where = (f"Points at {', '.join(a.starts)}" if a.starts else
                     f"Says the job {a.job} has stopped" if a.job else
                     "The subject does not identify a node, so no impact is guessed")
            st.caption(f"{where}. First seen {fmt_ts(al['first_seen'], '%d %b %H:%M')}, last seen "
                       f"{fmt_ts(al['last_seen'], '%d %b %H:%M')}.")
            if kpis or dashboards:
                html_table(pd.DataFrame({"Likely affected": [g.nodes[n].label for n in kpis + dashboards],
                                         "Kind": [g.nodes[n].kind for n in kpis + dashboards],
                                         "Hops": [a.reach[n].distance for n in kpis + dashboards]}), numeric={"Hops"})
