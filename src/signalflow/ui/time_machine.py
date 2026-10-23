"""Data Time Machine page: inspect an event-time window, replay it from Bronze in isolation, compare, promote, undo.

Every action goes through ``signalflow.timemachine.replay`` (and ``lake`` for the Iceberg copies), which authorises
the operator in the database layer and writes the audit log; this page only collects the window and shows results.
The page can work on the control tower's database or on the Chaos Lab database, which is the only place a Silver
defect can be simulated.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg
import streamlit as st
from psycopg.rows import dict_row

from signalflow.chaos import lab
from signalflow.security import PermissionDenied
from signalflow.timemachine import defects
from signalflow.timemachine import replay as tm
from signalflow.ui import auth, data
from signalflow.ui.components import (
    CRITICAL,
    GOOD,
    MUTED,
    WARNING,
    badge,
    fmt_bdt,
    fmt_int,
    fmt_ts,
    header,
    html_table,
    tiles,
)

DHAKA = ZoneInfo("Asia/Dhaka")
REPLAY_STATE = {"PREVIEWED": ("○", WARNING, "Previewed"), "PROMOTED": ("✓", GOOD, "Promoted"),
                "UNDONE": ("↺", MUTED, "Undone"), "DISCARDED": ("✕", MUTED, "Discarded")}
CHECK_STATE = {"True": ("✓", GOOD, "Pass"), "False": ("✕", CRITICAL, "Fail"), "None": ("–", MUTED, "Skipped")}
CHANGE_STATE = {"CHANGED": ("≠", WARNING, "Changed"), "INSERT": ("+", GOOD, "Insert"),
                "REMOVE": ("−", CRITICAL, "Remove")}
RUNS = """SELECT replay_id, stream, window_start, window_end, status, requested_by, created_at, promoted_by,
                 undone_by, summary -> 'changes' AS changes
          FROM ops.replay_runs ORDER BY created_at DESC LIMIT 30"""
EXTENT = """SELECT min(event_time) AS first, max(event_time) AS last FROM silver.events WHERE stream = %(stream)s"""
CHANGED_ROWS = """
    SELECT bronze_id, event_id, change, outcome,
           coalesce(row ->> 'event_type', current_row ->> 'event_type') AS event_type,
           current_row -> 'payload' ->> 'amount_bdt' AS amount_now, row -> 'payload' ->> 'amount_bdt' AS amount_replay,
           current_row ->> 'arrival_class' AS class_now, row ->> 'arrival_class' AS class_replay, reason
    FROM ops.replay_events WHERE replay_id = %(replay_id)s AND change IN ('CHANGED', 'INSERT', 'REMOVE')
    ORDER BY bronze_id LIMIT 60"""


def _connect(database: str) -> psycopg.Connection:
    return psycopg.connect(dbname=database, autocommit=True, application_name="signalflow-ui")


def _df(database: str, sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    with _connect(database) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params or {})
        return pd.DataFrame(cur.fetchall())


def _databases() -> list[str]:
    main = os.environ.get("PGDATABASE", "signalflow")
    out = [main]
    try:
        name = lab.lab_database_name()
        with _connect(main) as conn:
            if conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone():
                out.append(name)
    except (ValueError, psycopg.Error):
        pass
    return out


def _dhaka(t: Any) -> str:
    return fmt_ts(t.astimezone(DHAKA), "%d %b %H:%M")


def _short(v: Any, limit: int = 70) -> str:
    text = v if isinstance(v, str) else json.dumps(v, default=str)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def page() -> None:
    header("Trust", "Data Time Machine",
           "Replay an event-time window from Bronze with the current contracts and lateness rules, in isolation from "
           "Silver. Compare counts, money and duplicates before anything changes; promote in one transaction; undo "
           "exactly; rebuild only the Gold models downstream.")
    if not data.table_exists("ops.replay_runs"):
        st.info("Apply the Phase 5 migration first: `signalflow migrate`.")
        return
    dbs = _databases()
    database = st.radio("Database", dbs, horizontal=True,
                        format_func=lambda d: f"{d} (Chaos Lab)" if d != dbs[0] else f"{d} (control tower)")
    runs = _df(database, RUNS)
    counts = runs.status.value_counts() if not runs.empty else pd.Series(dtype=int)
    tiles([("Replays", fmt_int(len(runs)), "latest 30 in this database"),
           ("Promoted", fmt_int(counts.get("PROMOTED", 0)), "applied to Silver in one transaction"),
           ("Undone", fmt_int(counts.get("UNDONE", 0)), "restored from the archive"),
           ("Awaiting review", fmt_int(counts.get("PREVIEWED", 0)), "previewed, Silver untouched")])

    left, right = st.columns([3, 2])
    with left:
        window = window_form(database)
    with right:
        actions_panel(database, window)
    if window is not None and st.session_state.get("tm_inspect") == (database, window.stream, window.start):
        inspect_view(database, window)

    st.markdown("#### Replays")
    if runs.empty:
        st.caption("No replay has been previewed in this database yet.")
        return
    html_table(pd.DataFrame({
        "Replay": runs.replay_id, "Status": [badge(REPLAY_STATE, s) for s in runs.status],
        "Window (Asia/Dhaka)": [f"{_dhaka(a)} → {_dhaka(b)}"
                                for a, b in zip(runs.window_start, runs.window_end, strict=True)],
        "Changes": [", ".join(f"{k} {v}" for k, v in (c or {}).items()) for c in runs.changes],
        "Requested by": runs.requested_by,
        "Created": [fmt_ts(t, "%d %b %H:%M:%S") for t in runs.created_at],
    }), raw={"Status"})
    ids = runs.replay_id.tolist()
    chosen = st.session_state.get("tm_replay")
    choice = st.selectbox("Replay", ids, index=ids.index(chosen) if chosen in ids else 0,
                          format_func=lambda r: f"{r} · {runs.set_index('replay_id').status[r]}")
    with _connect(database) as conn:
        run = tm.get(conn, choice)
    replay_view(database, run)


def window_form(database: str) -> tm.Window | None:
    with st.container(border=True):
        st.markdown("##### Window")
        c1, c2, c3 = st.columns([1, 1.4, 1])
        stream = c1.selectbox("Stream", ["telecom", "mfg"])
        extent = _df(database, EXTENT, {"stream": stream})
        if extent.empty or pd.isna(extent["last"].iloc[0]):
            st.caption("This stream has no Silver events in this database.")
            return None
        last = extent["last"].iloc[0].astimezone(DHAKA)
        first = extent["first"].iloc[0].astimezone(DHAKA)
        day = c2.date_input("Start (Asia/Dhaka, 00:00)", value=last.date() - timedelta(days=1),
                            min_value=first.date(), max_value=last.date())
        hours = c3.number_input("Hours", min_value=1, max_value=int(tm.MAX_WINDOW.total_seconds() // 3600),
                                value=24, step=1)
        start = datetime.combine(day, time(0), DHAKA)
        window = tm.Window(stream, start, start + timedelta(hours=int(hours)))
        st.caption(f"Silver holds {stream} events from {first:%d %b %H:%M} to {last:%d %b %H:%M} (Asia/Dhaka). "
                   f"A window is at most {int(tm.MAX_WINDOW.total_seconds() // 86400)} days so a person can review "
                   "the preview.")
        if st.button("Inspect the window"):
            st.session_state["tm_inspect"] = (database, window.stream, window.start)
        return window


def actions_panel(database: str, window: tm.Window | None) -> None:
    op = auth.current()
    with st.container(border=True):
        st.markdown("##### Replay")
        if op is None or not op.can("replay.preview"):
            st.caption("Previewing, promoting and undoing need an operator account (sidebar). Each action is "
                       "authorised again in the database layer and written to the audit log.")
            return
        if window is None:
            return
        if st.button("Preview a replay of this window", type="primary"):
            with st.spinner("Replaying the window from Bronze…"):
                try:
                    with _connect(database) as conn:
                        run = tm.preview(conn, op, window)
                except (tm.ReplayError, PermissionDenied) as exc:
                    st.error(str(exc))
                    return
            st.session_state["tm_replay"] = run["replay_id"]
            st.rerun()
        if database != _databases()[0] and op.can("chaos.run"):
            st.markdown("###### Lab only: simulate a faulty processor release")
            kind = st.selectbox("Defect", list(defects.KINDS), format_func=lambda k: f"{k}: {defects.KINDS[k]}")
            rows = st.number_input("Rows", min_value=1, max_value=200, value=20)
            if st.button("Damage Silver in this window"):
                try:
                    with _connect(database) as conn:
                        res = defects.inject(conn, op, window, kind, int(rows))
                except (ValueError, PermissionDenied) as exc:
                    st.error(str(exc))
                    return
                st.warning(f"{len(res['event_ids'])} Silver rows damaged ({kind}). Bronze is untouched: preview a "
                           "replay to see the repair.")


def inspect_view(database: str, window: tm.Window) -> None:
    with _connect(database) as conn:
        result = tm.inspect_window(conn, window)
    st.markdown("#### What the window holds now")
    types = pd.DataFrame(result["event_types"])
    if types.empty:
        st.caption("No Bronze or Silver records in this window.")
        return
    html_table(pd.DataFrame({
        "Event type": types.event_type, "Bronze": [fmt_int(v) for v in types.bronze_rows],
        "Silver": [fmt_int(v) for v in types.silver_rows], "Quarantined": [fmt_int(v) for v in types.quarantined],
        "Duplicates": [fmt_int(v) for v in types.duplicates], "Late": [fmt_int(v) for v in types.late],
        "Amount (BDT)": [fmt_bdt(v) if v is not None and pd.notna(v) else "–" for v in types.amount_bdt],
    }), numeric={"Bronze", "Silver", "Quarantined", "Duplicates", "Late", "Amount (BDT)"})
    samples = pd.DataFrame(result["samples"])
    if not samples.empty:
        with st.expander(f"First {len(samples)} raw Bronze records of the window"):
            html_table(pd.DataFrame({
                "Bronze id": samples.bronze_id, "Event type": samples.event_type, "State": samples.state,
                "Arrived": [fmt_ts(t, "%d %b %H:%M:%S") for t in samples.arrival_time],
                "Raw record": [_short(r, 110) for r in samples.raw_record]}))


def replay_view(database: str, run: dict[str, Any]) -> None:
    s = run["summary"]
    st.markdown(f"##### {run['replay_id']}  {badge(REPLAY_STATE, run['status'])}", unsafe_allow_html=True)
    changes = s.get("changes", {})
    outcomes = ", ".join(f"{k.lower()} {v:,}" for k, v in s.get("outcomes", {}).items())
    tiles([("Bronze records replayed", fmt_int(run["bronze_rows"]), outcomes),
           ("Unchanged", fmt_int(changes.get("UNCHANGED", 0)), "replay equals Silver"),
           ("To change", fmt_int(sum(changes.get(k, 0) for k in ("CHANGED", "INSERT", "REMOVE"))),
            " · ".join(f"{k.lower()} {changes[k]:,}" for k in ("CHANGED", "INSERT", "REMOVE") if changes.get(k))
            or "nothing to repair"),
           ("Business dates touched", fmt_int(len(s.get("changed_business_dates", []))),
            ", ".join(s.get("changed_business_dates", [])) or "none")])
    action_buttons(database, run)

    left, right = st.columns(2)
    with left:
        st.markdown("###### Preview checks")
        checks = list(run.get("verification") or []) + list(s.get("post_checks", []))
        if checks:
            html_table(pd.DataFrame({"Check": [c["name"] for c in checks],
                                     "Expected": [_short(c["expected"]) for c in checks],
                                     "Actual": [_short(c["actual"]) for c in checks],
                                     "Result": [badge(CHECK_STATE, str(c["passed"])) for c in checks]}),
                       raw={"Result"})
    with right:
        st.markdown("###### Silver metrics, current vs replay")
        diff = [m for m in run["metrics_diff"] if m["changed"]]
        if diff:
            html_table(pd.DataFrame({"Business date": [m["business_date"] for m in diff],
                                     "Event type": [m["event_type"] for m in diff],
                                     "Events now": [fmt_int(m["events_current"]) for m in diff],
                                     "Events replay": [fmt_int(m["events_replay"]) for m in diff],
                                     "Amount now": [fmt_bdt(m["amount_current"]) for m in diff],
                                     "Amount replay": [fmt_bdt(m["amount_replay"]) for m in diff]}),
                       numeric={"Events now", "Events replay", "Amount now", "Amount replay"})
        else:
            st.caption(f"All {len(run['metrics_diff']):,} business-date × event-type cells are identical.")

    rows = _df(database, CHANGED_ROWS, {"replay_id": run["replay_id"]})
    if not rows.empty:
        st.markdown("###### Rows the replay changes (first 60)")
        html_table(pd.DataFrame({
            "Bronze id": rows.bronze_id, "Event id": [str(e)[:8] + "…" for e in rows.event_id],
            "Change": [badge(CHANGE_STATE, c) for c in rows.change], "Event type": rows.event_type,
            "Amount now": [fmt_bdt(v) if v else "–" for v in rows.amount_now],
            "Amount replay": [fmt_bdt(v) if v else "–" for v in rows.amount_replay],
            "Arrival now": rows.class_now.fillna("–"), "Arrival replay": rows.class_replay.fillna("–"),
        }), numeric={"Amount now", "Amount replay"}, raw={"Change"})

    if run.get("gold_after") is not None:
        st.markdown("###### Gold, before the promotion vs now")
        gd = tm.gold_diff(run["gold_before"], run["gold_after"])
        if gd:
            html_table(pd.DataFrame(gd).rename(columns={"metric": "Metric", "business_date": "Business date",
                                                        "before": "Before", "after": "After"}),
                       numeric={"Before", "After"})
        else:
            st.caption("Gold figures for the touched dates equal the preview's snapshot (expected after an undo, "
                       "or when the defect never reached Gold).")
        st.caption(f"dbt models rebuilt (downstream of the changed event types, from the lineage graph): "
                   f"{', '.join(s.get('dbt_selected', [])) or 'none'}")
    if run.get("lake"):
        with st.expander("Iceberg copies (snapshot ids before and after the sync)"):
            st.json(run["lake"], expanded=False)
    people = [f"requested by {run['requested_by']} at {fmt_ts(run['created_at'], '%d %b %H:%M:%S')}"]
    if run.get("promoted_by"):
        people.append(f"promoted by {run['promoted_by']} at {fmt_ts(run['promoted_at'], '%d %b %H:%M:%S')}")
    if run.get("undone_by"):
        people.append(f"undone by {run['undone_by']} at {fmt_ts(run['undone_at'], '%d %b %H:%M:%S')}")
    text = "; ".join(people)
    st.caption(text[0].upper() + text[1:] + ". Each step is in ops.audit_log.")


def action_buttons(database: str, run: dict[str, Any]) -> None:
    op = auth.current()
    if op is None:
        return
    status = run["status"]
    rid = run["replay_id"]
    is_main = database == _databases()[0]
    cols = st.columns(4)
    try:
        if status == "PREVIEWED" and op.can("replay.promote") and cols[0].button("Promote and rebuild Gold",
                                                                                  type="primary"):
            with st.spinner("Promoting in one transaction, then rebuilding the downstream Gold models…"), \
                    _connect(database) as conn:
                tm.promote(conn, op, rid)
                tm.rebuild_gold(conn, rid, database)
            st.rerun()
        if status == "PREVIEWED" and cols[1].button("Discard"):
            with _connect(database) as conn:
                tm.discard(conn, op, rid)
            st.rerun()
        if status == "PROMOTED" and op.can("replay.undo") and cols[0].button("Undo and rebuild Gold"):
            with st.spinner("Restoring the archived rows, then rebuilding Gold…"), _connect(database) as conn:
                tm.undo(conn, op, rid)
                tm.rebuild_gold(conn, rid, database)
            st.rerun()
        if status in ("PROMOTED", "UNDONE") and _gold_rebuilt_after(run) != status and cols[1].button("Rebuild Gold"):
            with st.spinner("Rebuilding the downstream Gold models…"), _connect(database) as conn:
                tm.rebuild_gold(conn, rid, database)
            st.rerun()
        lake = run.get("lake") or {}
        if is_main and status == "PROMOTED" and "promote" not in lake and op.can("replay.promote") and \
                cols[2].button("Sync the Iceberg copies"):
            lake_action(database, rid, "sync")
        if is_main and status == "UNDONE" and "promote" in lake and "undo" not in lake and op.can("replay.undo") and \
                cols[2].button("Roll back the Iceberg copies"):
            lake_action(database, rid, "undo")
    except tm.ProcessorRunning as exc:
        st.error(f"A Silver processor is running: {exc}. Stop it, then try again.")
    except (tm.ReplayError, PermissionDenied) as exc:
        st.error(str(exc))


def _gold_rebuilt_after(run: dict[str, Any]) -> str | None:
    """The status after which Gold was last rebuilt (a failed rebuild leaves the promotion without one)."""
    value = run["summary"].get("gold_rebuilt_after")
    return str(value) if value else None


def lake_action(database: str, replay_id: str, action: str) -> None:
    from signalflow.lakehouse.settings import LakeSettings
    from signalflow.timemachine import lake

    with st.spinner("Writing to Iceberg through Trino…"), _connect(database) as conn:
        settings = LakeSettings.from_env()
        if action == "sync":
            lake.sync(settings, conn, replay_id)
        else:
            lake.undo_lake(settings, conn, replay_id)
    st.rerun()
