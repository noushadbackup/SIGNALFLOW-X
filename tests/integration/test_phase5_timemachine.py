"""Phase 5 acceptance: the Data Time Machine repairs a damaged Silver window from Bronze, and undoes it exactly.

A lab database is built from a deterministic dataset; its clean metrics are the reference. A defect damages Silver
(the way a faulty processor release would), the replay previews the repair without touching Silver, promotion
applies it in one transaction, Gold is rebuilt and reconciled, and undo restores the damaged state bit for bit.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path

import psycopg
import pytest

from signalflow.chaos import lab
from signalflow.security import Operator, PermissionDenied
from signalflow.timemachine import defects
from signalflow.timemachine import replay as R

pytestmark = pytest.mark.integration
DATASET = lab.LabDataset(seed=7, days=3, subscribers=600, dealers=100)
OPERATOR = Operator("tm.operator", "TM Operator", "operator")
ANALYST = Operator("tm.analyst", "TM Analyst", "analyst")


@pytest.fixture(scope="module")
def lab_db(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    os.environ["SIGNALFLOW_LAB_DATA"] = str(tmp_path_factory.mktemp("tmlab"))
    name = f"sfx_lab_tm_{uuid.uuid4().hex[:8]}"
    yield name
    lab.drop_database(name)
    os.environ.pop("SIGNALFLOW_LAB_DATA", None)


@pytest.fixture
def clean(lab_db: str, tmp_path: Path) -> Iterator[tuple[psycopg.Connection, dict[str, object]]]:
    lab.build(lab_db, DATASET, tmp_path / "work")
    with lab.lab_connect(lab_db) as conn:
        yield conn, lab.metrics(conn)


def window(conn: psycopg.Connection, stream: str) -> R.Window:
    end = conn.execute("SELECT max(event_time) FROM silver.events WHERE stream = %s", (stream,)).fetchone()[0]
    return R.Window(stream, end - timedelta(hours=40), end - timedelta(hours=16))


def test_inflated_amounts_are_repaired_reconciled_and_undone(clean: tuple[psycopg.Connection, dict]) -> None:
    conn, reference = clean
    w = window(conn, "telecom")
    damage = defects.inject(conn, OPERATOR, w, "inflate_amounts", rows=20)
    lab.dbt_lab(conn.info.dbname)
    damaged = lab.metrics(conn)
    assert damaged["gold"]["revenue"] != reference["gold"]["revenue"]

    preview = R.preview(conn, ANALYST, w)  # an analyst may preview
    assert preview["summary"]["changes"]["CHANGED"] == 20 and "INSERT" not in preview["summary"]["changes"]
    assert all(c["passed"] for c in preview["verification"]), preview["verification"]
    changed = {str(e) for (e,) in conn.execute(
        "SELECT event_id FROM ops.replay_events WHERE replay_id = %s AND change = 'CHANGED'", (preview["replay_id"],))}
    assert changed == set(damage["event_ids"])
    assert lab.metrics(conn) == damaged  # a preview changes nothing

    with pytest.raises(PermissionDenied):
        R.promote(conn, ANALYST, preview["replay_id"])
    promoted = R.promote(conn, OPERATOR, preview["replay_id"])
    assert promoted["summary"]["applied"] == {"silver.events": 20}
    gold = R.rebuild_gold(conn, preview["replay_id"])
    assert all(c["passed"] for c in gold["checks"]), gold["checks"]
    assert {d["metric"] for d in gold["gold_diff"]} == {"gross_recharge_bdt"}
    repaired = lab.metrics(conn)
    assert repaired["silver"] == reference["silver"] and repaired["gold"] == reference["gold"]

    R.undo(conn, OPERATOR, preview["replay_id"])
    R.rebuild_gold(conn, preview["replay_id"])
    restored = lab.metrics(conn)
    assert restored["silver"] == damaged["silver"] and restored["gold"] == damaged["gold"]
    actions = conn.execute("SELECT actor, action, outcome FROM ops.audit_log WHERE target_id = %s ORDER BY audit_id",
                           (preview["replay_id"],)).fetchall()
    assert actions == [("tm.analyst", "replay.preview", "OK"), ("tm.analyst", "replay.promote", "DENIED"),
                       ("tm.operator", "replay.promote", "OK"), ("tm.operator", "replay.undo", "OK")]


def test_deleted_events_are_reinserted_with_their_original_arrival_class(
        clean: tuple[psycopg.Connection, dict]) -> None:
    conn, reference = clean
    w = window(conn, "mfg")
    before = {str(e): (c, n) for e, c, n in conn.execute(
        "SELECT event_id, arrival_class, lateness_seconds FROM silver.events WHERE stream = 'mfg'")}
    damage = defects.inject(conn, OPERATOR, w, "drop_events", rows=15)
    preview = R.preview(conn, OPERATOR, w)
    assert preview["summary"]["changes"]["INSERT"] == 15
    R.promote(conn, OPERATOR, preview["replay_id"])
    after = {str(e): (c, n) for e, c, n in conn.execute(
        "SELECT event_id, arrival_class, lateness_seconds FROM silver.events WHERE event_id = ANY(%s::uuid[])",
        (damage["event_ids"],))}
    assert after == {e: before[e] for e in damage["event_ids"]}  # lateness reproduced from the batch clock
    R.rebuild_gold(conn, preview["replay_id"])
    assert lab.metrics(conn)["silver"] == reference["silver"]


def test_promotion_is_refused_when_silver_changed_after_the_preview_or_a_processor_runs(
        clean: tuple[psycopg.Connection, dict]) -> None:
    conn, _ = clean
    w = window(conn, "telecom")
    defects.inject(conn, OPERATOR, w, "inflate_amounts", rows=5)
    preview = R.preview(conn, OPERATOR, w)
    with psycopg.connect(dbname=conn.info.dbname) as other:  # a processor holding its checkpoint lock
        other.execute("SELECT 1 FROM ops.checkpoints WHERE processor = 'silver.lite' FOR UPDATE")
        with pytest.raises(R.ProcessorRunning):
            R.promote(conn, OPERATOR, preview["replay_id"])
    conn.execute("""UPDATE silver.events SET lateness_seconds = lateness_seconds + 1
                    WHERE event_id = (SELECT event_id FROM silver.events WHERE stream = 'telecom'
                                      AND event_time >= %s AND event_time < %s LIMIT 1)""", (w.start, w.end))
    with pytest.raises(R.ReplayError, match="changed in this window since the preview"):
        R.promote(conn, OPERATOR, preview["replay_id"])
    assert R.get(conn, preview["replay_id"])["status"] == "PREVIEWED"
    assert R.discard(conn, OPERATOR, preview["replay_id"])["status"] == "DISCARDED"


def test_guards_viewer_window_and_defects_outside_the_lab(clean: tuple[psycopg.Connection, dict]) -> None:
    conn, _ = clean
    w = window(conn, "telecom")
    with pytest.raises(PermissionDenied):
        R.preview(conn, Operator("tm.viewer", "Viewer", "viewer"), w)
    with pytest.raises(R.ReplayError):
        R.Window("telecom", w.start, w.start + timedelta(days=4))
    with pytest.raises(R.ReplayError):
        R.Window("telecom", w.start.replace(tzinfo=None), w.end)
    with psycopg.connect(autocommit=True) as not_a_lab, pytest.raises(ValueError):
        defects.inject(not_a_lab, OPERATOR, w, "drop_events")
