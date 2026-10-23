"""Phase 5 lake acceptance: a promoted replay is synced to Iceberg, time travel shows the damaged version, and undo
rolls the Iceberg tables back to their pre-sync snapshots."""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterator
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

from signalflow.chaos import lab
from signalflow.lakehouse import admin
from signalflow.security import Operator
from signalflow.timemachine import defects
from signalflow.timemachine import lake as tm_lake
from signalflow.timemachine import replay as R

from .conftest import LakeEnv, require_lake_infra

pytestmark = pytest.mark.lake
OPERATOR = Operator("tm.operator", "TM Operator", "operator")


@pytest.fixture
def lab_lake(tmp_path: Path) -> Iterator[LakeEnv]:
    base = require_lake_infra()
    tag = uuid.uuid4().hex[:8]
    os.environ["SIGNALFLOW_LAB_DATA"] = str(tmp_path / "data")
    env = LakeEnv(replace(base, namespace_prefix=f"t{tag}_"), f"sfx_lab_lake_{tag}", tmp_path)
    try:
        yield env
    finally:
        admin.drop_namespaces(env.settings)
        lab.drop_database(env.database)
        os.environ.pop("SIGNALFLOW_LAB_DATA", None)


def _amounts(env: LakeEnv, snapshot: int | None, ids: list[str]) -> dict[str, str]:
    where = "event_id IN (" + ", ".join(f"'{i}'" for i in ids) + ")"
    rows = (tm_lake.as_of(env.settings, "silver.events", snapshot, where) if snapshot is not None else
            tm_lake.as_of(env.settings, "silver.events", tm_lake._snapshot(env.settings, "silver.events") or 0, where))
    return {r[0]: json.loads(r[9])["amount_bdt"] for r in rows}  # columns as published: event_id ... payload


def test_replay_is_synced_to_iceberg_and_undone_by_snapshot_rollback(lab_lake: LakeEnv) -> None:
    env = lab_lake
    dataset = lab.LabDataset(seed=7, days=3, subscribers=600, dealers=100)
    lab.build(env.database, dataset, env.log_dir / "work", dbt=False)
    with lab.lab_connect(env.database) as conn:
        end = conn.execute("SELECT max(event_time) FROM silver.events WHERE stream = 'telecom'").fetchone()[0]
        w = R.Window("telecom", end - timedelta(hours=40), end - timedelta(hours=16))
        damage = defects.inject(conn, OPERATOR, w, "inflate_amounts", rows=10)
        ids = damage["event_ids"]
        damaged_pg = {str(e): p["amount_bdt"] for e, p in conn.execute(
            "SELECT event_id, payload FROM silver.events WHERE event_id = ANY(%s::uuid[])", (ids,))}

        env.publish()  # the damaged rows reach the lake, as they would have before anyone noticed
        assert _amounts(env, None, ids) == damaged_pg

        preview = R.preview(conn, OPERATOR, w)
        R.promote(conn, OPERATOR, preview["replay_id"])
        synced = tm_lake.sync(env.settings, conn, preview["replay_id"])
        assert synced["verified_after_promote"] == {"silver.events": True}
        fixed_pg = {str(e): p["amount_bdt"] for e, p in conn.execute(
            "SELECT event_id, payload FROM silver.events WHERE event_id = ANY(%s::uuid[])", (ids,))}
        assert fixed_pg != damaged_pg and _amounts(env, None, ids) == fixed_pg
        snap = synced["promote"]["silver.events"]
        assert snap["before"] != snap["after"]
        assert _amounts(env, snap["before"], ids) == damaged_pg  # time travel: the lake as it was

        R.undo(conn, OPERATOR, preview["replay_id"])
        undone = tm_lake.undo_lake(env.settings, conn, preview["replay_id"])
        assert undone["undo"]["silver.events"]["method"] == "rollback_to_snapshot"
        assert undone["verified_after_undo"] == {"silver.events": True}
        assert tm_lake._snapshot(env.settings, "silver.events") == snap["before"]
        assert _amounts(env, None, ids) == damaged_pg

        env.publish()  # the publisher accepts the lake after the rollback: same instance, same high-water mark
        assert tm_lake.verify(env.settings, conn, preview["replay_id"]) == {"silver.events": True}
