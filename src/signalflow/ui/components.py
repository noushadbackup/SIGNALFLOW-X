"""Shared look and helpers for the control tower pages: palette, tiles, tables, charts and cached queries."""

from __future__ import annotations

import html

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from signalflow.ui import data

# Validated light palette (dataviz skill reference instance). Categorical hues are assigned to entities in a fixed
# order and never cycled; status colours are reserved for state and always ship with an icon and a label.
CATEGORICAL = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
GOOD, WARNING, SERIOUS, CRITICAL = "#0ca30c", "#fab219", "#ec835a", "#d03b3b"
INK, INK_2, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#fcfcfb"

CHANNEL_COLOR = dict(zip(["MOBILE_WALLET", "RETAIL_AGENT", "BANK_CARD", "INTERNET_BANKING"], CATEGORICAL, strict=False))
CATEGORY_COLOR = dict(zip(["SMARTPHONE", "TELEVISION", "REFRIGERATOR", "AIR_CONDITIONER", "WASHING_MACHINE", "FAN"],
                          CATEGORICAL, strict=False))
FACTORY_COLOR = {"FAC-01": CATEGORICAL[0], "FAC-02": CATEGORICAL[1]}
ARRIVAL_COLOR = {"ON_TIME": CATEGORICAL[0], "LATE_WITHIN_WATERMARK": CATEGORICAL[3],
                 "LATE_BEYOND_WATERMARK": CATEGORICAL[7]}

KPI_BADGE = {"FINAL": ("✓", GOOD, "Final"), "PROVISIONAL": ("○", WARNING, "Provisional"),
             "RESTATED": ("↻", SERIOUS, "Restated")}
RISK_BADGE = {"OUT_OF_STOCK": ("✕", CRITICAL, "Out of stock"), "HIGH": ("▲", SERIOUS, "High"),
              "MEDIUM": ("◆", WARNING, "Medium"), "LOW": ("✓", GOOD, "Low"),
              "NO_RECENT_DEMAND": ("–", MUTED, "No recent demand")}

CSS = f"""
<style>
  .block-container {{ padding-top: 2.2rem; max-width: 1280px; }}
  h1, h2, h3 {{ letter-spacing: -0.01em; color: {INK}; }}
  .sf-eyebrow {{ color: {MUTED}; font-size: .78rem; text-transform: uppercase; letter-spacing: .08em; margin-bottom: .1rem; }}
  .sf-sub {{ color: {INK_2}; margin-top: -.4rem; margin-bottom: 1.2rem; }}
  .sf-tiles {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin: .4rem 0 1.2rem; }}
  .sf-tile {{ background: {SURFACE}; border: 1px solid {GRID}; border-radius: 10px; padding: 14px 16px; }}
  .sf-tile .l {{ color: {INK_2}; font-size: .8rem; }}
  .sf-tile .v {{ color: {INK}; font-size: 1.65rem; font-weight: 600; font-variant-numeric: tabular-nums; line-height: 1.25; }}
  .sf-tile .n {{ color: {MUTED}; font-size: .74rem; margin-top: 2px; }}
  .sf-badge {{ display: inline-flex; align-items: center; gap: 6px; font-size: .8rem; color: {INK}; white-space: nowrap; }}
  .sf-dot {{ width: 9px; height: 9px; border-radius: 50%; display: inline-block; }}
  table.sf {{ width: 100%; border-collapse: collapse; border: none !important; font-size: .86rem; font-variant-numeric: tabular-nums; }}
  table.sf th, table.sf td {{ border-left: none !important; border-right: none !important; border-top: none !important; background: transparent; }}
  table.sf th {{ text-align: left; color: {INK_2}; font-weight: 500; border-bottom: 1px solid {GRID}; padding: 7px 10px; }}
  table.sf td {{ border-bottom: 1px solid #efeee9; padding: 7px 10px; color: {INK}; }}
  table.sf td.num, table.sf th.num {{ text-align: right; }}
  .sf-source {{ color: {MUTED}; font-size: .74rem; margin-top: -.3rem; margin-bottom: 1rem; }}
  .sf-card {{ background: {SURFACE}; border: 1px solid {GRID}; border-radius: 10px; padding: 18px 20px; }}
  .sf-prov {{ display: inline-block; border: 1px solid {GRID}; border-radius: 4px; padding: 0 6px; font-size: .7rem;
              color: {INK_2}; background: #fff; text-transform: uppercase; letter-spacing: .04em; margin-right: 6px; }}
  .sf-prov-note {{ color: {MUTED}; font-size: .74rem; }}
  .sf-meta {{ color: {INK_2}; font-size: .82rem; margin: -.6rem 0 .8rem; }}
  .sf-phase {{ display: inline-block; border: 1px solid {GRID}; border-radius: 999px; padding: 2px 10px; font-size: .78rem;
              color: {INK_2}; background: #fff; }}
</style>
"""


@st.cache_data(ttl=10, show_spinner=False)
def q(sql: str) -> pd.DataFrame:
    return data.query(sql)


@st.cache_data(ttl=10, show_spinner=False)
def qp(sql: str, params: tuple[tuple[str, object], ...]) -> pd.DataFrame:
    """Cached parameterised query (params as a hashable tuple of pairs)."""
    return data.query(sql, dict(params))


def fmt_int(v: object) -> str:
    return "–" if v is None or pd.isna(v) else f"{int(v):,}"  # type: ignore[call-overload]


def fmt_bdt(v: object) -> str:
    return "–" if v is None or pd.isna(v) else f"৳{float(v):,.2f}"  # type: ignore[arg-type]


def fmt_ts(v: object, pattern: str = "%d %b %H:%M") -> str:
    if v is None or pd.isna(v):
        return "–"
    return str(pd.Timestamp(v).tz_convert("Asia/Dhaka").strftime(pattern))


def badge(mapping: dict[str, tuple[str, str, str]], key: str) -> str:
    icon, color, label = mapping.get(key, ("•", MUTED, key))
    return (f'<span class="sf-badge"><span class="sf-dot" style="background:{color}"></span>'
            f'{html.escape(icon)} {html.escape(label)}</span>')


def header(eyebrow: str, title: str, sub: str) -> None:
    st.markdown(f'<div class="sf-eyebrow">{eyebrow}</div>', unsafe_allow_html=True)
    st.markdown(f"## {title}")
    st.markdown(f'<div class="sf-sub">{sub}</div>', unsafe_allow_html=True)


def tiles(items: list[tuple[str, str, str]]) -> None:
    cells = "".join(f'<div class="sf-tile"><div class="l">{html.escape(label)}</div><div class="v">{value}</div>'
                    f'<div class="n">{html.escape(note)}</div></div>' for label, value, note in items)
    st.markdown(f'<div class="sf-tiles">{cells}</div>', unsafe_allow_html=True)


def source(*tables: str) -> None:
    st.markdown(f'<div class="sf-source">Source: {", ".join(f"<code>{t}</code>" for t in tables)}</div>',
                unsafe_allow_html=True)


def html_table(df: pd.DataFrame, numeric: set[str] | None = None, raw: set[str] | None = None) -> None:
    numeric, raw = numeric or set(), raw or set()
    head = "".join(f'<th class="{"num" if c in numeric else ""}">{html.escape(c)}</th>' for c in df.columns)
    rows = []
    for _, r in df.iterrows():
        cells = "".join(f'<td class="{"num" if c in numeric else ""}">{r[c] if c in raw else html.escape(str(r[c]))}</td>'
                        for c in df.columns)
        rows.append(f"<tr>{cells}</tr>")
    st.markdown(f'<table class="sf"><thead><tr>{head}</tr></thead><tbody>{"".join(rows)}</tbody></table>',
                unsafe_allow_html=True)


def style(fig: go.Figure, height: int = 320, legend: bool = True, y_title: str | None = None) -> go.Figure:
    fig.update_layout(
        height=height, margin={"l": 8, "r": 8, "t": 36, "b": 8}, paper_bgcolor=SURFACE, plot_bgcolor=SURFACE,
        font={"color": INK_2, "size": 12}, bargap=0.35, bargroupgap=0.08, showlegend=legend,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0, "title": None, "traceorder": "normal", "font": {"color": INK_2}},
        hoverlabel={"bgcolor": "#ffffff", "bordercolor": GRID, "font": {"color": INK}},
    )
    fig.update_xaxes(showgrid=False, linecolor=GRID, tickcolor=GRID)
    if fig.data and fig.data[0].orientation != "h" and fig.data[0].type == "bar":
        fig.update_xaxes(type="category")
    fig.update_yaxes(gridcolor=GRID, zeroline=True, zerolinecolor=GRID, title=y_title, title_font={"color": MUTED})
    fig.update_traces(selector={"type": "bar"}, marker_line_color=SURFACE, marker_line_width=2,
                      marker_cornerradius=4)
    return fig


def chart(fig: go.Figure) -> None:
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def missing_gold() -> bool:
    if data.table_exists("gold.mart_telecom_revenue_daily"):
        return False
    st.warning("Gold tables are not built yet. Run `make run` (generate → bronze → silver → dbt) and reload.")
    return True

# Leak Hunter case states. Status colours are reserved for state; each badge also carries an icon and a label.
CASE_STATE = {
    "PENDING": ("◷", MUTED, "Pending"),
    "SUSPICIOUS": ("▲", WARNING, "Suspicious"),
    "CONFIRMED_BY_RULE": ("■", CRITICAL, "Confirmed by rule"),
    "CLEARED_BY_DATA": ("✓", GOOD, "Cleared by data"),
    "RESOLVED_BY_ANALYST": ("✓", CATEGORICAL[0], "Resolved by analyst"),
}

# What kind of number a panel shows (requirement: distinguish facts, derived metrics and projections).
PROVENANCE = {
    "fact": ("Stored fact", "events and records as received, after validation"),
    "derived": ("Derived metric", "computed from stored facts by a documented query or dbt model"),
    "projection": ("Projection", "a simple forward estimate from recent history, not an observation"),
    "estimate": ("Estimate", "a rule-based estimate of money at risk, not an audited figure"),
}


def case_state(status: str, resolution: object) -> str:
    if status != "RESOLVED":
        return status
    return "CLEARED_BY_DATA" if resolution == "AUTO_CLEARED" else "RESOLVED_BY_ANALYST"


def provenance(kind: str) -> None:
    label, meaning = PROVENANCE[kind]
    st.markdown(f'<span class="sf-prov" title="{html.escape(meaning)}">{html.escape(label)}</span>'
                f'<span class="sf-prov-note">{html.escape(meaning)}</span>', unsafe_allow_html=True)
