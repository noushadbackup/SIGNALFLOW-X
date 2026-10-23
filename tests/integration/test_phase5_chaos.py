"""Phase 5 acceptance: every Chaos Lab incident runs end to end and passes its own verification.

Each incident rebuilds a lab database from the same deterministic dataset, injects its fault, recovers and compares
the result with a clean run of that dataset. The test then checks the report's key facts directly, so a passing
report cannot hide a wrong verification.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest

from signalflow.chaos import lab
from signalflow.chaos.runner import run
from signalflow.db import migrate
from signalflow.security import Operator, PermissionDenied

from .conftest import temp_database

pytestmark = pytest.mark.integration
DATASET = lab.LabDataset(seed=7, days=3, subscribers=600, dealers=100)
OPERATOR = Operator("chaos.tester", "Chaos Tester", "operator")


@pytest.fixture(scope="module")
def env(tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[psycopg.Connection, str]]:
    os.environ["SIGNALFLOW_LAB_DATA"] = str(tmp_path_factory.mktemp("lab"))
    lab_db = f"sfx_lab_{uuid.uuid4().hex[:8]}"
    with temp_database(), psycopg.connect(autocommit=True) as control:
        migrate(control)
        yield control, lab_db
        lab.drop_database(lab_db)
    os.environ.pop("SIGNALFLOW_LAB_DATA", None)


def run_incident(env: tuple[psycopg.Connection, str], code: str) -> dict[str, Any]:
    control, lab_db = env
    report = run(control, code, OPERATOR, DATASET, lab_db)
    failed = [c for c in report.get("verification", []) if not c["passed"]]
    assert report["status"] == "PASSED", (report.get("error"), failed)
    stored = control.execute("SELECT status, report ->> 'run_id' FROM ops.chaos_runs WHERE run_id = %s",
                             (report["run_id"],)).fetchone()
    assert stored == ("PASSED", report["run_id"])
    return report


def test_a_duplicate_storm_is_absorbed(env: tuple[psycopg.Connection, str]) -> None:
    r = run_incident(env, "A")
    assert r["injected"]["duplicates"] == r["injected"]["records_sent"] and r["injected"]["accepted"] == 0
    assert any(a["rule"] == "DUPLICATE_SURGE" for a in r["alerts"])


def test_b_late_surge_is_accepted_restated_and_reconciled(env: tuple[psycopg.Connection, str]) -> None:
    r = run_incident(env, "B")
    classes = r["observed"]["accepted_by_arrival_class"]
    assert classes.get("LATE_BEYOND_WATERMARK", 0) > 0 and sum(classes.values()) == r["injected"]["accepted"]
    assert r["observed"]["restated_business_dates"]


def test_c_schema_break_is_quarantined_alerted_and_fixed(env: tuple[psycopg.Connection, str]) -> None:
    r = run_incident(env, "C")
    reasons = r["observed"]["quarantined_by_reason"]
    assert set(reasons) == {"CONTRACT_VIOLATION", "ENVELOPE_VIOLATION", "UNPARSEABLE", "UNSUPPORTED_SCHEMA_VERSION"}
    assert r["observed"]["reached_silver"] == 0
    assert any(a["rule"] == "CONTRACT_VIOLATIONS" and a["details"]["sample_bronze_ids"] for a in r["alerts"])


def test_d_killed_processor_resumes_to_the_clean_result(env: tuple[psycopg.Connection, str]) -> None:
    r = run_incident(env, "D")
    assert r["injected"]["killed"] and r["injected"]["exit_code"] == -9
    assert r["observed"]["bronze_backlog"] > 0 and r["observed"]["silver_rows_beyond_checkpoint"] == 0
    assert r["recovery"]["resumed_from_bronze_id"] == r["injected"]["checkpoint_at_kill"]


def test_e_stock_conflict_is_flagged_uncertain_and_resolved_with_audit(env: tuple[psycopg.Connection, str]) -> None:
    r = run_incident(env, "E")
    assert set(r["observed"]["position_status"].values()) == {"UNCERTAIN"}
    assert len(r["observed"]["cases"]) == 2 and r["recovery"]["resolved_by"] == OPERATOR.username
    with lab.lab_connect(env[1]) as conn:
        history = conn.execute(
            "SELECT count(*) FROM assurance.case_events WHERE case_id = ANY(%s) AND action = 'RESOLVED'",
            (r["recovery"]["cases_resolved"],)).fetchone()
    assert history == (2,)


def test_viewer_cannot_run_an_incident_and_the_attempt_is_audited(env: tuple[psycopg.Connection, str]) -> None:
    control, lab_db = env
    with pytest.raises(PermissionDenied):
        run(control, "A", Operator("view.only", "View Only", "viewer"), DATASET, lab_db)
    assert control.execute("""SELECT count(*) FROM ops.audit_log WHERE actor = 'view.only' AND action = 'chaos.run'
                              AND outcome = 'DENIED'""").fetchone() == (1,)


def test_lab_refuses_protected_database_names() -> None:
    for name in ("signalflow", "signalflow_stream", "postgres", "analytics"):
        with pytest.raises(ValueError):
            lab.lab_database_name(name)
    assert lab.lab_database_name("signalflow_lab") == "signalflow_lab"
    assert Path(lab.LabDataset().directory).name == lab.LabDataset().key
