"""Phase 4 acceptance: the Revenue Leak Hunter on a real PostgreSQL, evaluated against the scenario ledger.

The dataset is long enough (11 days) for every rule's late-data horizon to pass for most scenarios, including
MFG-05, whose window starts at an invoice's due date. The hunter runs after every arrival file, as it would on a
schedule against the streaming database, so delayed transactions are seen as open gaps before they arrive.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
import pytest

from signalflow.assurance import cases as case_actions
from signalflow.assurance import hunter as hunter_mod
from signalflow.assurance.hunter import HuntResult, hunt
from signalflow.assurance.rules import BY_ID
from signalflow.generator import GeneratorConfig, generate
from signalflow.pipeline import ingest_and_process, prepare
from signalflow.security import PermissionDenied, add_operator, authenticate
from signalflow_eval.leak_hunter import SCENARIOS, evaluate, load_cases, load_ledger, match_key

from .conftest import temp_database

pytestmark = pytest.mark.integration
CONFIG = GeneratorConfig(seed=11, days=11, subscribers=500, dealers=120)
CHUNK = 100_000
DHAKA = ZoneInfo("Asia/Dhaka")


@dataclass
class AssuranceRun:
    data: Path
    conn: psycopg.Connection
    runs: list[HuntResult]
    report: dict[str, Any]
    open_cases: dict[str, str]


def case_fingerprint(conn: psycopg.Connection) -> str:
    return str(conn.execute(
        """SELECT md5(string_agg(case_id || status || coalesce(resolution, '') || evidence_sha256 || version
                                 || coalesce(confirmed_as_of::text, ''), ',' ORDER BY case_id))
           FROM assurance.cases""").fetchone()[0])  # type: ignore[index]


def open_cases(conn: psycopg.Connection) -> dict[str, str]:
    return dict(conn.execute("SELECT case_id, status FROM assurance.cases WHERE status <> 'RESOLVED'").fetchall())


@pytest.fixture(scope="module")
def dataset(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out = tmp_path_factory.mktemp("assurance") / "seed11"
    generate(CONFIG, out)
    return out


@pytest.fixture(scope="module")
def run(dataset: Path) -> Iterator[AssuranceRun]:
    with temp_database(), psycopg.connect(autocommit=True) as conn:
        prepare(conn, dataset)
        runs: list[HuntResult] = []
        ingest_and_process(conn, dataset, chunk_size=CHUNK, after_file=lambda c: runs.append(hunt(c)))
        report = evaluate(load_ledger(dataset), load_cases(conn))
        print(json.dumps(report, indent=1, default=str))  # shown with -s; the Phase 4 report quotes it
        yield AssuranceRun(dataset, conn, runs, report, open_cases(conn))


def test_no_false_confirmation_and_every_confirmable_leak_is_confirmed(run: AssuranceRun) -> None:
    assert all(r.get("confirmed_fp", 0) == 0 for r in run.report["results"].values()), run.report["results"]
    ledger = load_ledger(run.data)
    cases = {(c["rule_id"], match_key(c)): c for c in load_cases(run.conn)}
    clocks = dict(run.conn.execute("SELECT stream, max_event_time FROM ops.stream_state").fetchall())
    checked = 0
    for rule_id, scenario in SCENARIOS.items():
        if scenario is None:
            continue
        kind, key = scenario
        for a in (a for a in ledger if a["anomaly_type"] == kind):
            case = cases.get((rule_id, key(a["refs"])))
            if case is None:
                # the only legitimate miss: an unpaid invoice that is not due yet at the end of the data
                due_end = datetime.combine(date.fromisoformat(a["refs"]["due_date"]) + timedelta(days=1),
                                           datetime.min.time(), DHAKA)
                assert rule_id == "MFG-05" and due_end > clocks["mfg"], a
                continue
            horizon_passed = case["anchor_time"] + case["confirm_after"] <= case["last_evaluated_as_of"]
            if horizon_passed:
                assert case["ever_confirmed"] and case["status"] == "CONFIRMED_BY_RULE", case
            else:
                assert case["status"] in ("PENDING", "SUSPICIOUS"), case
            checked += 1
    assert checked >= 30
    confirmed_rules = {r for r, v in run.report["results"].items() if v.get("confirmed_tp", 0) > 0}
    assert len(confirmed_rules) >= 8, confirmed_rules


def test_delayed_but_normal_transactions_are_never_confirmed(run: AssuranceRun) -> None:
    benign = run.report["benign_checks"]
    assert benign["confirmed then cleared by data"] == 0
    assert benign["TEL-05 confirmed"] == 0
    delayed = benign["DELAYED_BILLING"]
    # every delayed billing was visible as an open gap, reached SUSPICIOUS, and cleared when it arrived
    assert delayed["scenarios"] >= 3
    assert delayed["cases_opened"] == delayed["cases_suspicious_at_some_point"] == delayed["scenarios"]
    assert delayed["confirmed"] == 0 and delayed["still_open"] == 0
    late_pay = benign["DELAYED_DEALER_PAYMENT"]
    assert late_pay["scenarios"] >= 5 and late_pay["confirmed"] == 0
    # TEL-05 is a pure delay detector: it saw gaps (recharges in transit) and every one of them closed
    tel05 = run.report["results"]["TEL-05"]
    assert tel05["cases"] > 0 and tel05["auto_cleared"] == tel05["cases"]
    assert sum(r.auto_cleared for r in run.runs) > 0
    assert run.report["passed"]


def test_hunter_is_idempotent(run: AssuranceRun) -> None:
    before = case_fingerprint(run.conn)
    again = hunt(run.conn)
    assert (again.opened, again.status_changes, again.auto_cleared, again.reopened) == (0, 0, 0, 0)
    assert case_fingerprint(run.conn) == before


def test_final_open_cases_do_not_depend_on_hunting_cadence(run: AssuranceRun) -> None:
    """Hunting once at the end gives the same open cases and statuses as hunting after every file: status is a
    function of the data and the event-time clock, not of when the hunter happened to run."""
    with temp_database(), psycopg.connect(autocommit=True) as conn:
        prepare(conn, run.data)
        ingest_and_process(conn, run.data, chunk_size=CHUNK)
        hunt(conn)
        assert open_cases(conn) == run.open_cases


def test_failed_hunt_rolls_back(run: AssuranceRun, monkeypatch: pytest.MonkeyPatch) -> None:
    before = case_fingerprint(run.conn)
    runs_before = run.conn.execute("SELECT count(*) FROM assurance.hunter_runs").fetchone()
    real_observe = hunter_mod.observe

    def failing(conn: psycopg.Connection, rule: Any, as_of: datetime) -> list[dict[str, Any]]:
        if rule.rule_id == "MFG-03":
            raise RuntimeError("injected failure in the eighth rule")
        return real_observe(conn, rule, as_of)

    monkeypatch.setattr(hunter_mod, "observe", failing)
    run.conn.execute("UPDATE ops.stream_state SET max_event_time = max_event_time + interval '30 days'")
    try:
        with pytest.raises(RuntimeError, match="injected failure"):
            hunt(run.conn)
    finally:
        run.conn.execute("UPDATE ops.stream_state SET max_event_time = max_event_time - interval '30 days'")
    assert case_fingerprint(run.conn) == before
    assert run.conn.execute("SELECT count(*) FROM assurance.hunter_runs").fetchone() == runs_before


def test_analyst_actions_need_a_role_are_versioned_and_audited(run: AssuranceRun) -> None:
    conn = run.conn
    add_operator(conn, "viewer.one", "Viewer One", "viewer", "viewer-pass-123")
    add_operator(conn, "analyst.one", "Analyst One", "analyst", "analyst-pass-123")
    assert authenticate(conn, "analyst.one", "wrong-password-1") is None
    viewer = authenticate(conn, "viewer.one", "viewer-pass-123")
    analyst = authenticate(conn, "analyst.one", "analyst-pass-123")
    assert viewer and analyst
    case_id, version = conn.execute(
        """SELECT case_id, version FROM assurance.cases WHERE status = 'CONFIRMED_BY_RULE' AND rule_id = 'TEL-02'
           ORDER BY case_id LIMIT 1""").fetchone()  # type: ignore[misc]

    with pytest.raises(PermissionDenied):
        case_actions.resolve(conn, viewer, case_id, "RECOVERED", "refund issued", version)
    with pytest.raises(PermissionDenied):
        case_actions.add_note(conn, None, case_id, "anonymous note")
    with pytest.raises(case_actions.CaseConflict):
        case_actions.resolve(conn, analyst, case_id, "RECOVERED", "refund issued", version - 1)

    case_actions.add_note(conn, analyst, case_id, "Customer called; gateway shows two captures.")
    case_actions.resolve(conn, analyst, case_id, "RECOVERED", "Second charge refunded, ref RF-1001.", version)
    status, resolution, resolved_by = conn.execute(
        "SELECT status, resolution, resolved_by FROM assurance.cases WHERE case_id = %s", (case_id,)).fetchone()  # type: ignore[misc]
    assert (status, resolution, resolved_by) == ("RESOLVED", "RECOVERED", "analyst.one")

    hunt(conn)  # the discrepancy is still in the data, but an analyst's resolution stands
    assert conn.execute("SELECT status FROM assurance.cases WHERE case_id = %s", (case_id,)).fetchone() == (
        "RESOLVED",)

    version = conn.execute("SELECT version FROM assurance.cases WHERE case_id = %s", (case_id,)).fetchone()[0]  # type: ignore[index]
    case_actions.reopen(conn, analyst, case_id, "Refund bounced.", version)
    assert conn.execute("SELECT status, resolution FROM assurance.cases WHERE case_id = %s",
                        (case_id,)).fetchone() == ("CONFIRMED_BY_RULE", None)

    history = [r[0] for r in conn.execute(
        "SELECT action FROM assurance.case_events WHERE case_id = %s ORDER BY case_event_id", (case_id,))]
    assert history[-3:] == ["NOTE", "RESOLVED", "REOPENED"]
    audit = conn.execute(
        """SELECT actor, action, outcome FROM ops.audit_log WHERE target_id = %s OR action = 'auth.login'
           ORDER BY audit_id""", (case_id,)).fetchall()
    assert ("viewer.one", "case.resolve", "DENIED") in audit
    assert ("anonymous", "case.note", "DENIED") in audit
    assert ("analyst.one", "auth.login", "DENIED") in audit
    assert ("analyst.one", "case.resolved", "OK") in audit
    assert ("analyst.one", "case.reopened", "OK") in audit
    assert BY_ID["TEL-02"].case_id(conn.execute("SELECT subject_key FROM assurance.cases WHERE case_id = %s",
                                                (case_id,)).fetchone()[0]) == case_id  # type: ignore[index]
