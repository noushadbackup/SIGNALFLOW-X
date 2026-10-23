"""SignalFlow X control tower (Streamlit). Run with `make ui`.

Every number on screen comes from a query in `signalflow.ui.data`; sections whose backing feature is not built yet
say which phase delivers them instead of showing sample figures.
"""

from __future__ import annotations

import html
from collections.abc import Callable

import pandas as pd
import plotly.graph_objects as go
import psycopg
import streamlit as st

from signalflow.ui import auth, chaos_lab, data, leak_hunter, lineage_explorer, time_machine
from signalflow.ui.components import (
    ARRIVAL_COLOR,
    CATEGORICAL,
    CATEGORY_COLOR,
    CHANNEL_COLOR,
    CRITICAL,
    CSS,
    FACTORY_COLOR,
    GOOD,
    GRID,
    INK_2,
    KPI_BADGE,
    MUTED,
    RISK_BADGE,
    SURFACE,
    badge,
    chart,
    fmt_bdt,
    fmt_int,
    fmt_ts,
    header,
    html_table,
    missing_gold,
    provenance,
    q,
    source,
    style,
    tiles,
)

# --------------------------------------------------------------------------------------------------- pages


def page_overview() -> None:
    header("Control tower", "Enterprise overview",
           "Health of the shared platform and the headline KPIs for both businesses, read live from Postgres.")
    c = q(data.PIPELINE_COUNTS).iloc[0]
    streams = q(data.STREAMS).set_index("stream")
    tiles([
        ("Bronze records", fmt_int(c.bronze_rows), "raw, append-only"),
        ("Silver events", fmt_int(c.silver_events), "validated, deduplicated"),
        ("Quarantined", fmt_int(c.quarantined), "kept with a reason code"),
        ("Duplicates suppressed", fmt_int(c.duplicates), "same event_id and hash"),
        ("Pending in bronze", fmt_int(c.pending), "not yet processed"),
        ("Last micro-batch", fmt_ts(c.last_batch_at, "%H:%M"), fmt_ts(c.last_batch_at, "%d %b %Y") + " · Dhaka"),
    ])
    if missing_gold():
        return

    left, right = st.columns([1.15, 1])
    with left:
        st.markdown("#### Telecom recharge revenue by day")
        rev = q(data.REVENUE_DAILY)
        fig = go.Figure()
        for ch, color in CHANNEL_COLOR.items():
            d = rev[rev.channel == ch]
            fig.add_bar(x=d.business_date.astype(str), y=d.gross_bdt.astype(float), name=ch.replace("_", " ").title(),
                        marker_color=color, hovertemplate="%{x}<br>%{fullData.name}: ৳%{y:,.2f}<extra></extra>")
        fig.update_layout(barmode="stack")
        chart(style(fig, y_title="BDT"))
        source("gold.mart_telecom_revenue_daily")
    with right:
        st.markdown("#### KPI finality by business day")
        ds = q(data.DAY_STATUS)
        view = pd.DataFrame({
            "Day (Dhaka)": ds.business_date.astype(str),
            "Status": [badge(KPI_BADGE, s) for s in ds.kpi_status],
            "Gross revenue": [fmt_bdt(v) for v in ds.gross_bdt],
            "Success rate": [f"{float(v):.2%}" for v in ds.success_rate],
        })
        html_table(view, numeric={"Gross revenue", "Success rate"}, raw={"Status"})
        st.markdown("")
        st.caption("Final means the stream watermark has passed the end of the day. Restated means an event arrived "
                   "beyond the 6 h allowed lateness after the day was final, so the figure changed (ADR-0004).")
        wm = pd.DataFrame({
            "Stream": streams.index,
            "Event-time clock": [fmt_ts(v) for v in streams.max_event_time],
            "Watermark": [fmt_ts(v) for v in streams.watermark],
            "Allowed lateness": streams.allowed_lateness,
        })
        html_table(wm)
        source("ops.stream_state")

    st.markdown("#### Revenue assurance")
    if not data.table_exists("assurance.cases") or q(data.LEAK_LAST_RUN).empty:
        st.caption("The Leak Hunter has not run on this database yet.")
        return
    t = q(data.LEAK_TOTALS).iloc[0]
    tiles([
        ("Leak cases confirmed by rule", fmt_int(t.confirmed), f"{fmt_bdt(t.confirmed_exposure)} estimated exposure"),
        ("Suspicious", fmt_int(t.suspicious), f"{fmt_bdt(t.suspicious_exposure)} at risk"),
        ("Pending", fmt_int(t.pending), "within normal processing time"),
        ("Cleared by data", fmt_int(t.auto_cleared), "delays that resolved themselves"),
    ])
    st.caption("Exposure is a rule-based estimate, not an audited loss. Details, evidence and actions: Revenue Leak "
               "Hunter.")


def page_telecom() -> None:
    header("Digital twin", "Telecom operations",
           "The operator's current state, rebuilt from processed events: what happened in the last 24 hours of stream "
           "time, and the reconciled daily KPIs from the gold marts.")
    if missing_gold():
        return
    clock = q(data.STREAMS).set_index("stream")
    if "telecom" in clock.index:
        st.markdown(f'<div class="sf-meta">State as of the telecom event-time clock: '
                    f'{fmt_ts(clock.loc["telecom", "max_event_time"], "%d %b %Y %H:%M")} Dhaka. Turn on live refresh in '
                    f'the sidebar while the stream runs.</div>', unsafe_allow_html=True)
    ds = q(data.DAY_STATUS)
    subs = q(data.ACTIVE_SUBSCRIBERS)
    latest = subs[subs.business_date == subs.business_date.max()]
    tiles([
        ("Gross recharge", fmt_bdt(ds.gross_bdt.sum()), f"{len(ds)} business days"),
        ("Successful recharges", fmt_int(ds.successful.sum()), f"{fmt_int(ds.failed.sum())} failed"),
        ("Active subscribers", fmt_int(latest.active.sum()), f"as of {latest.business_date.max()}"),
        ("Data served", f"{q(data.USAGE_BY_REGION).data_gb.astype(float).sum():,.1f} GB", "all regions"),
    ])
    st.markdown("#### Last 24 hours of stream time")
    provenance("fact")
    h = q(data.TWIN_TELECOM_LAST24H)
    hour = pd.to_datetime(h.hour, utc=True).dt.tz_convert("Asia/Dhaka")
    a, b = st.columns(2)
    with a:
        st.markdown("##### Recharges per hour")
        fig = go.Figure()
        fig.add_bar(x=hour, y=h.recharges.astype(float), name="Successful", marker_color=CATEGORICAL[0],
                    hovertemplate="%{x|%d %b %H:00}<br>successful: %{y:,}<extra></extra>")
        fig.add_bar(x=hour, y=h.failed.astype(float), name="Failed", marker_color=CATEGORICAL[1],
                    hovertemplate="%{x|%d %b %H:00}<br>failed: %{y:,}<extra></extra>")
        fig.update_layout(barmode="stack")
        chart(style(fig, height=260, y_title="recharges").update_xaxes(type="date", tickformat="%H:%M"))
    with b:
        st.markdown("##### Mobile data served per hour")
        fig = go.Figure(go.Scatter(x=hour, y=h.data_gb.astype(float), mode="lines+markers", name="Data",
                                   line={"color": CATEGORICAL[2], "width": 2},
                                   marker={"size": 8, "line": {"color": SURFACE, "width": 2}},
                                   hovertemplate="%{x|%d %b %H:00}<br>%{y:,.2f} GB<extra></extra>"))
        chart(style(fig, height=260, legend=False, y_title="GB").update_xaxes(type="date", tickformat="%H:%M"))
    source("silver.events (telecom, event time within 24 h of the stream clock)")

    a, b = st.columns(2)
    with a:
        st.markdown("#### Payment success rate by channel")
        provenance("derived")
        ch = q(data.TWIN_CHANNEL_SUCCESS)
        html_table(pd.DataFrame({"Channel": ch.channel.str.replace("_", " ").str.title(),
                                 "Attempts": [fmt_int(v) for v in ch.attempts],
                                 "Successful": [fmt_int(v) for v in ch.successful],
                                 "Success rate": [f"{float(v):.2%}" for v in ch.success_rate]}),
                   numeric={"Attempts", "Successful", "Success rate"})
        source("gold.mart_telecom_revenue_daily")
    with b:
        st.markdown("#### Bundle revenue by package (captured payments)")
        provenance("derived")
        pk = q(data.TWIN_PACKAGE_REVENUE)
        fig = go.Figure(go.Bar(x=pk.revenue_bdt.astype(float), y=pk.package_name, orientation="h",
                               marker_color=CATEGORICAL[0], customdata=pk.activations,
                               hovertemplate="%{y}: ৳%{x:,.2f} · %{customdata:,} activations<extra></extra>"))
        fig.update_yaxes(autorange="reversed", gridcolor=SURFACE)
        fig.update_xaxes(showgrid=True, gridcolor=GRID, title="BDT", title_font={"color": MUTED})
        chart(style(fig, legend=False, height=260))
        source("gold.fct_package_purchase", "gold.dim_package")

    a, b = st.columns(2)
    with a:
        st.markdown("#### Recharge revenue by region")
        reg = q(data.REVENUE_BY_REGION)
        fig = go.Figure(go.Bar(x=reg.gross_bdt.astype(float), y=reg.region, orientation="h", marker_color=CATEGORICAL[0],
                               hovertemplate="%{y}: ৳%{x:,.2f}<extra></extra>"))
        fig.update_yaxes(autorange="reversed", gridcolor=SURFACE)
        fig.update_xaxes(showgrid=True, gridcolor=GRID, title="BDT", title_font={"color": MUTED})
        chart(style(fig, legend=False))
        source("gold.mart_telecom_revenue_daily")
    with b:
        st.markdown("#### Data usage by serving region")
        u = q(data.USAGE_BY_REGION)
        fig = go.Figure(go.Bar(x=u.data_gb.astype(float), y=u.serving_region, orientation="h",
                               marker_color=CATEGORICAL[0], hovertemplate="%{y}: %{x:,.2f} GB<extra></extra>"))
        fig.update_yaxes(autorange="reversed", gridcolor=SURFACE)
        fig.update_xaxes(showgrid=True, gridcolor=GRID, title="GB", title_font={"color": MUTED})
        chart(style(fig, legend=False))
        source("gold.mart_telecom_usage_daily")

    st.markdown("#### Recharge outcomes by channel, last three business days")
    rev = q(data.REVENUE_DAILY)
    rev = rev[rev.business_date.isin(sorted(rev.business_date.unique())[-3:])]
    view = pd.DataFrame({
        "Day": rev.business_date.astype(str), "Channel": rev.channel.str.replace("_", " ").str.title(),
        "Gross": [fmt_bdt(v) for v in rev.gross_bdt], "Successful": [fmt_int(v) for v in rev.successful],
        "Failed": [fmt_int(v) for v in rev.failed], "KPI status": [badge(KPI_BADGE, s) for s in rev.kpi_status],
    })
    html_table(view, numeric={"Gross", "Successful", "Failed"}, raw={"KPI status"})
    source("gold.mart_telecom_revenue_daily")


def page_manufacturing() -> None:
    header("Digital twin", "Manufacturing and dealer network",
           "Inventory, shipments, production, dealer sales and receivables, rebuilt from processed ERP, WMS and MES "
           "events.")
    if missing_gold():
        return
    clock = q(data.STREAMS).set_index("stream")
    if "mfg" in clock.index:
        st.markdown(f'<div class="sf-meta">State as of the manufacturing event-time clock: '
                    f'{fmt_ts(clock.loc["mfg", "max_event_time"], "%d %b %Y %H:%M")} Dhaka.</div>',
                    unsafe_allow_html=True)
    inv = q(data.INVENTORY)
    invoices = q(data.INVOICES)
    sales = q(data.MFG_SALES)
    at_risk = inv[inv.stockout_risk.isin(["OUT_OF_STOCK", "HIGH"])]
    tiles([
        ("Dealer orders", fmt_int(sales.orders.sum()), f"{fmt_int(sales.backordered.sum())} backordered"),
        ("Ordered value", fmt_bdt(sales.ordered_bdt.sum()), "all categories"),
        ("SKU-warehouse at risk", fmt_int(len(at_risk)), f"of {len(inv)} positions"),
        ("Outstanding receivables", fmt_bdt(invoices.outstanding_bdt.sum()), f"{fmt_int(invoices.invoices.sum())} invoices"),
    ])
    a, b = st.columns(2)
    with a:
        st.markdown("#### Ordered value by category")
        fig = go.Figure()
        for cat, color in CATEGORY_COLOR.items():
            d = sales[sales.category == cat]
            fig.add_bar(x=d.business_date.astype(str), y=d.ordered_bdt.astype(float), name=cat.replace("_", " ").title(),
                        marker_color=color, hovertemplate="%{x}<br>%{fullData.name}: ৳%{y:,.2f}<extra></extra>")
        fig.update_layout(barmode="group")
        chart(style(fig, y_title="BDT"))
        source("gold.mart_mfg_sales_daily")
    with b:
        st.markdown("#### Units produced by factory")
        prod = q(data.PRODUCTION)
        fig = go.Figure()
        for fac, color in FACTORY_COLOR.items():
            d = prod[prod.factory_id == fac]
            fig.add_bar(x=d.business_date.astype(str), y=d.units.astype(float), name=fac, marker_color=color,
                        customdata=d.disruption.fillna("none"),
                        hovertemplate="%{x}<br>%{fullData.name}: %{y:,} units<br>disruption: %{customdata}<extra></extra>")
        for r in prod[prod.disruption.notna()].itertuples():
            fig.add_annotation(x=str(r.business_date), y=0, text=f"{r.factory_id}: {r.disruption.replace('_', ' ').lower()}",
                               showarrow=True, arrowcolor=MUTED, xshift=-24, ax=-40, ay=-90, bgcolor=SURFACE,
                               font={"color": INK_2, "size": 11})
        fig.update_layout(barmode="group")
        chart(style(fig, y_title="units"))
        source("gold.mart_mfg_production_daily")

    st.markdown("#### Shipment progress by warehouse")
    provenance("derived")
    sh = q(data.TWIN_SHIPMENTS)
    fig = go.Figure()
    for col, label, color in (("delivered", "Delivered", CATEGORICAL[0]), ("in_transit", "In transit", CATEGORICAL[3]),
                              ("delayed", "In transit > 60 h", CRITICAL)):
        fig.add_bar(x=sh.warehouse_id, y=sh[col].astype(float), name=label, marker_color=color,
                    hovertemplate="%{x}<br>%{fullData.name}: %{y:,} shipments<extra></extra>")
    fig.update_layout(barmode="stack")
    chart(style(fig, height=260, y_title="shipments"))
    st.caption("Delivery normally takes 18 to 60 hours after dispatch; older open shipments are flagged as delayed. "
               f"Average transit so far: {', '.join(f'{w} {float(t):.0f} h' for w, t in zip(sh.warehouse_id, sh.avg_transit_hours, strict=True) if not pd.isna(t))}.")
    source("gold.fct_shipment", "ops.stream_state")

    st.markdown("#### Inventory risk (lowest days of cover first)")
    provenance("projection")
    top = inv.head(12)
    view = pd.DataFrame({
        "Warehouse": top.warehouse_id, "SKU": top.sku, "Category": top.category.str.replace("_", " ").str.title(),
        "On hand": [fmt_int(v) for v in top.on_hand_qty], "7-day outbound": [fmt_int(v) for v in top.outbound_7d_qty],
        "Days of cover": ["–" if pd.isna(v) else f"{float(v):.1f}" for v in top.days_of_cover],
        "Risk": [badge(RISK_BADGE, s) for s in top.stockout_risk],
        "Unapproved adj.": [fmt_int(v) for v in top.unauthorized_adjustments],
    })
    html_table(view, numeric={"On hand", "7-day outbound", "Days of cover", "Unapproved adj."}, raw={"Risk"})
    source("gold.mart_inventory_position")

    st.markdown("#### Invoice settlement")
    view = pd.DataFrame({"Status": invoices.settlement_status.str.replace("_", " ").str.title(),
                         "Invoices": [fmt_int(v) for v in invoices.invoices],
                         "Invoiced": [fmt_bdt(v) for v in invoices.invoiced_bdt],
                         "Outstanding": [fmt_bdt(v) for v in invoices.outstanding_bdt]})
    html_table(view, numeric={"Invoices", "Invoiced", "Outstanding"})
    source("gold.fct_invoice")


def page_pipeline() -> None:
    header("Trust", "Pipeline observatory",
           "What happened to every record: accepted, quarantined, deduplicated or late, and how each micro-batch ran.")
    c = q(data.PIPELINE_COUNTS).iloc[0]
    runs = q(data.PROCESSING_RUNS)
    dur = runs.duration_ms.astype(float)
    tiles([
        ("Micro-batches", fmt_int(len(runs)), "one transaction each"),
        ("Batch p50", f"{dur.quantile(.5):,.0f} ms" if len(dur) else "–", "processing time"),
        ("Batch p95", f"{dur.quantile(.95):,.0f} ms" if len(dur) else "–", "processing time"),
        ("Accepted share", f"{int(c.silver_events) / max(int(c.bronze_rows), 1):.2%}", "silver ÷ bronze"),
    ])
    st.caption("Batch timings come from ops.processing_runs on this machine. The controlled benchmark suite with "
               "baseline vs optimised runs is Phase 6.")

    a, b = st.columns(2)
    with a:
        st.markdown("#### Record outcomes")
        bronze = max(int(c.bronze_rows), 1)
        outcomes = [("Accepted into silver", c.silver_events), ("Duplicate suppressed", c.duplicates),
                    ("Quarantined", c.quarantined), ("Pending in bronze", c.pending)]
        html_table(pd.DataFrame({"Outcome": [o for o, _ in outcomes],
                                 "Records": [fmt_int(v) for _, v in outcomes],
                                 "Share of bronze": [f"{int(v) / bronze:.2%}" for _, v in outcomes]}),
                   numeric={"Records", "Share of bronze"})
        st.caption(f"{fmt_int(c.bronze_rows)} bronze records in total. Every one ends in exactly one outcome.")
        source("silver.events", "ops.duplicate_events", "silver.quarantine", "ops.checkpoints")
    with b:
        st.markdown("#### Quarantine by reason")
        qr = q(data.QUARANTINE_BY_REASON)
        fig = go.Figure(go.Bar(x=qr.records.astype(float), y=qr.reason_code, orientation="h",
                               marker_color=CATEGORICAL[1], hovertemplate="%{y}: %{x}<extra></extra>"))
        fig.update_yaxes(autorange="reversed", gridcolor=SURFACE)
        fig.update_xaxes(showgrid=True, gridcolor=GRID, dtick=1)
        chart(style(fig, legend=False, height=260))
        source("silver.quarantine")

    a, b = st.columns(2)
    with a:
        st.markdown("#### Arrival class by stream")
        ac = q(data.ARRIVAL_CLASSES)
        fig = go.Figure()
        for cls, color in ARRIVAL_COLOR.items():
            d = ac[ac.arrival_class == cls]
            fig.add_bar(x=d.stream, y=d.events.astype(float), name=cls.replace("_", " ").title(), marker_color=color,
                        hovertemplate="%{x}<br>%{fullData.name}: %{y:,}<extra></extra>")
        fig.update_layout(barmode="group")
        fig.update_yaxes(type="log", title="events (log scale)")
        chart(style(fig, height=300))
        source("silver.events")
    with b:
        st.markdown("#### KPI restatements")
        rs = q(data.RESTATEMENTS)
        if rs.empty:
            st.info("No business day has been restated.")
        else:
            html_table(pd.DataFrame({"Stream": rs.stream, "Business day": rs.business_date.astype(str),
                                     "Late events": [fmt_int(v) for v in rs.late_event_count],
                                     "Last detected": [fmt_ts(v) for v in rs.last_detected_at]}),
                       numeric={"Late events"})
        source("ops.kpi_restatements")

    st.markdown("#### Micro-batch durations")
    fig = go.Figure(go.Scatter(x=runs.run_id, y=dur, mode="lines+markers", line={"color": CATEGORICAL[0], "width": 2},
                               marker={"size": 8, "line": {"color": SURFACE, "width": 2}},
                               customdata=runs.rows_read, hovertemplate="run %{x}<br>%{y:,.0f} ms · %{customdata:,} rows"
                               "<extra></extra>"))
    fig.update_xaxes(title="run id")
    chart(style(fig, legend=False, height=260, y_title="ms"))
    source("ops.processing_runs")

    st.markdown("#### Quarantined records")
    qs = q(data.QUARANTINE_SAMPLE)
    st.dataframe(qs, hide_index=True, use_container_width=True,
                 column_config={"details": st.column_config.TextColumn("details", width="large")})
    source("silver.quarantine", "bronze.raw_events")


def page_lakehouse() -> None:
    header("Platform", "Lakehouse",
           "Committed Bronze, Silver and ops state published to Iceberg tables; Gold rebuilt by dbt on Trino and "
           "checked row by row against Postgres Gold.")
    try:
        tables = lake_cached("tables")
    except Exception as exc:  # Trino not running: say so instead of showing anything
        st.info(f"The lake is not reachable ({type(exc).__name__}). Start it with `docker compose --profile lake up` "
                "and publish with `signalflow lake publish`.")
        return
    runs = q(data.LAKE_PUBLISH_RUNS) if data.table_exists("ops.lake_publish_runs") else pd.DataFrame()
    check = q(data.LAKE_LAST_CHECK) if data.table_exists("ops.lake_differential_checks") else pd.DataFrame()
    identical = int(check.identical.sum()) if len(check) else 0
    tiles([
        ("Iceberg tables", fmt_int(len(tables)), "bronze · silver · ops · ref"),
        ("Snapshots", fmt_int(tables.snapshots.sum()), "time-travel points kept"),
        ("Last publication", fmt_ts(runs.published_at.max()) if len(runs) else "–", "Dhaka time"),
        ("Gold models identical", f"{identical} / {len(check)}" if len(check) else "not checked yet",
         f"check {fmt_ts(check.checked_at.max())}" if len(check) else "run signalflow lake compare"),
    ])

    a, b = st.columns([3, 2])
    with a:
        st.markdown("#### Silver events: snapshot history")
        snaps = lake_cached("silver.events")
        labels = [f"{fmt_ts(t, '%H:%M:%S')} · {op}" for t, op in zip(snaps.committed_at, snaps.operation, strict=True)]
        op_color = {"append": CATEGORICAL[0], "replace": CATEGORICAL[1], "overwrite": CATEGORICAL[2]}
        fig = go.Figure()
        for op, color in op_color.items():
            mask = snaps.operation == op
            if mask.any():
                fig.add_bar(x=[lbl for lbl, m in zip(labels, mask, strict=True) if m],
                            y=snaps.total_records[mask].astype(float), name=op, marker_color=color,
                            customdata=snaps.added_records[mask], hovertemplate="%{x}<br>total %{y:,} rows · "
                            "added %{customdata:,}<extra>%{fullData.name}</extra>")
        fig.update_xaxes(categoryorder="array", categoryarray=labels)
        chart(style(fig, height=300, y_title="rows after commit"))
        st.caption("Append = a publication of newly committed rows (its high-water mark is in the snapshot summary). "
                   "Replace = compaction: files rewritten, row count unchanged.")
        source("lake.silver.\"events$snapshots\" (Trino)")
    with b:
        st.markdown("#### Differential check (last run)")
        if check.empty:
            st.info("No check recorded yet. Run `signalflow lake compare`.")
        else:
            ok = {True: ("✓", GOOD, "Identical"), False: ("✕", CRITICAL, "Differs")}
            html_table(pd.DataFrame({
                "Gold model": check.model, "Postgres rows": [fmt_int(v) for v in check.rows_postgres],
                "Lake rows": [fmt_int(v) for v in check.rows_lake],
                "Result": [badge({str(k): v for k, v in ok.items()}, str(bool(x))) for x in check.identical]}),
                numeric={"Postgres rows", "Lake rows"}, raw={"Result"})
        source("ops.lake_differential_checks")

    st.markdown("#### Published tables")
    html_table(pd.DataFrame({
        "Table": tables.table_name, "Mode": tables["mode"], "Rows": [fmt_int(v) for v in tables.rows],
        "Snapshots": [fmt_int(v) for v in tables.snapshots], "Last commit": [fmt_ts(v) for v in tables.last_commit]}),
        numeric={"Rows", "Snapshots"})
    source("Iceberg metadata tables via Trino")

    if len(runs):
        st.markdown("#### Recent publication runs")
        shown = runs.head(20)
        html_table(pd.DataFrame({
            "At": [fmt_ts(v, "%d %b %H:%M:%S") for v in shown.published_at], "Table": shown.table_name,
            "Rows written": [fmt_int(v) for v in shown.rows_written],
            "High-water mark": [f"{fmt_int(a)} → {fmt_int(b)}" if a is not None and not pd.isna(a) else "–"
                                for a, b in zip(shown.hwm_before, shown.hwm_after, strict=True)],
            "Note": [s or "" for s in shown.skipped], "Seconds": [f"{float(v):.2f}" for v in shown.seconds]}),
            numeric={"Rows written", "Seconds"})
        source("ops.lake_publish_runs")


@st.cache_data(ttl=30, show_spinner=False)
def lake_cached(what: str) -> pd.DataFrame:
    if what == "tables":
        return data.lake_tables()
    layer, table = what.split(".")
    return data.lake_snapshots(layer, table)


def planned(phase: str, title: str, what: list[str]) -> Callable[[], None]:
    def render() -> None:
        header("Planned", title, "Not built yet. This page appears with real data when its phase ships.")
        items = "".join(f"<li>{html.escape(w)}</li>" for w in what)
        st.markdown(f'<div class="sf-card"><span class="sf-phase">{phase}</span><ul style="margin-top:12px">{items}'
                    f"</ul></div>", unsafe_allow_html=True)
    render.__name__ = "planned_" + title.lower().replace(" ", "_")
    return render


def main() -> None:
    st.set_page_config(page_title="SignalFlow X", page_icon=":material/hub:", layout="wide")
    st.markdown(CSS, unsafe_allow_html=True)
    try:
        data.query("SELECT 1")
    except psycopg.OperationalError as exc:
        st.error(f"Cannot reach Postgres: {exc}. Start it with `make up` and check the PG* variables in .env.")
        st.stop()

    pages = {
        "Operate": [st.Page(page_overview, title="Overview", icon=":material/dashboard:", default=True),
                    st.Page(page_telecom, title="Telecom", url_path="telecom", icon=":material/cell_tower:"),
                    st.Page(page_manufacturing, title="Manufacturing", url_path="manufacturing", icon=":material/factory:")],
        "Trust": [st.Page(leak_hunter.page, title="Revenue Leak Hunter", url_path="leak-hunter", icon=":material/search_insights:"),
                  st.Page(page_pipeline, title="Pipeline observatory", url_path="pipeline", icon=":material/monitor_heart:"),
                  st.Page(chaos_lab.page, title="Chaos Lab", url_path="chaos-lab", icon=":material/science:"),
                  st.Page(time_machine.page, title="Time Machine", url_path="time-machine", icon=":material/history:")],
        "Platform": [st.Page(page_lakehouse, title="Lakehouse", url_path="lakehouse", icon=":material/database:"),
                     st.Page(lineage_explorer.page, title="Lineage", url_path="lineage", icon=":material/account_tree:")],
        "Planned": [
            st.Page(planned("Phase 6", "Benchmarks", [
                "p50 / p95 / p99 latency and throughput, baseline vs optimised", "Methodology and hardware recorded"]),
                title="Benchmarks", url_path="benchmarks", icon=":material/speed:"),
        ],
    }
    nav = st.navigation(pages)
    with st.sidebar:
        st.markdown("**SignalFlow X**")
        st.caption("Enterprise data nervous system · synthetic data · Postgres + Iceberg lake")
        st.caption(f"Rendered {pd.Timestamp.now(tz='Asia/Dhaka').strftime('%d %b %Y %H:%M')} Dhaka")
        st.toggle("Live refresh (every 15 s)", key="live", help="Re-query the database while the stream runs. "
                  "Pages show processed events only; nothing on screen is simulated in the browser.")
        auth.sidebar_panel()
    if st.session_state.get("live"):
        st.fragment(run_every="15s")(nav.run)()
    else:
        nav.run()


main()
