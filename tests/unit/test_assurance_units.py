"""Leak Hunter rules, security helpers and the offline evaluation logic, without a database."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

import pytest

from signalflow import REPO_ROOT
from signalflow.assurance.rules import HORIZON, OUTPUT_COLUMNS, RULES, RULES_DIR
from signalflow.generator.manufacturing import LATE_PAYMENT_MAX
from signalflow.generator.transport import LATE_BEYOND, LATE_WITHIN
from signalflow.security import ROLES, Operator, hash_password, verify_password
from signalflow_eval.leak_hunter import evaluate


def test_ten_rules_five_per_domain_each_with_a_reviewed_sql_file() -> None:
    assert [r.domain for r in RULES].count("telecom") == 5 and [r.domain for r in RULES].count("mfg") == 5
    assert len({r.rule_id for r in RULES}) == 10
    assert {p.name for p in RULES_DIR.glob("*.sql")} == {r.sql_file for r in RULES}
    for r in RULES:
        text = r.sql.lower()
        for col in OUTPUT_COLUMNS:
            assert re.search(rf"\bas {col}\b", text), (r.rule_id, col)
        # psycopg placeholders: the clock is the only parameter; any other % would be misparsed
        assert re.sub(r"%\(as_of\)s", "", r.sql).count("%") == 0, r.rule_id


@pytest.mark.parametrize("rule", RULES, ids=lambda r: r.rule_id)
def test_status_boundaries(rule: object) -> None:
    from signalflow.assurance.rules import Rule

    assert isinstance(rule, Rule)
    tick = timedelta(seconds=1)
    if rule.expected_within > timedelta(0):
        assert rule.status_for(rule.expected_within - tick) == "PENDING"
    assert rule.status_for(rule.expected_within) == "SUSPICIOUS"
    assert rule.status_for(rule.confirm_after - tick) == "SUSPICIOUS"
    assert rule.status_for(rule.confirm_after) == "CONFIRMED_BY_RULE"


def test_horizons_exceed_every_delay_the_platform_accepts() -> None:
    """The guarantee 'a delayed but complete transaction is never confirmed' rests on these inequalities."""
    for r in RULES:
        assert r.confirm_after >= HORIZON[r.domain]
    assert HORIZON["telecom"] > max(LATE_BEYOND["telecom"][1], LATE_WITHIN[1], timedelta(hours=20))
    assert HORIZON["mfg"] > max(LATE_BEYOND["mfg"][1], LATE_WITHIN[1])
    overdue = next(r for r in RULES if r.rule_id == "MFG-05")
    assert overdue.expected_within >= LATE_PAYMENT_MAX
    assert overdue.confirm_after >= LATE_PAYMENT_MAX + HORIZON["mfg"]


def test_case_ids_are_deterministic_and_rule_scoped() -> None:
    a, b = RULES[0], RULES[1]
    assert a.case_id("RCH-1") == a.case_id("RCH-1") != b.case_id("RCH-1")
    assert re.fullmatch(r"TEL-01-[0-9A-F]{10}", a.case_id("RCH-1"))


def test_password_hashing() -> None:
    h1, h2 = hash_password("correct horse 1"), hash_password("correct horse 1")
    assert h1 != h2 and h1.startswith("scrypt$")
    assert verify_password("correct horse 1", h1) and not verify_password("correct horse 2", h1)
    assert not verify_password("anything", "not-a-hash")
    with pytest.raises(ValueError):
        hash_password("short")


def test_role_order() -> None:
    allowed = {role: Operator("u", "U", role).can("case.resolve") for role in ROLES}
    assert allowed == {"viewer": False, "analyst": True, "operator": True, "admin": True}
    assert not Operator("u", "U", "operator").can("operators.manage")


def test_detection_code_never_imports_the_evaluation_package() -> None:
    offenders = [str(p) for p in (REPO_ROOT / "src" / "signalflow").rglob("*.py") if "signalflow_eval" in p.read_text()]
    assert offenders == []


def _case(rule: str, key: str, confirmed: bool, status: str, resolution: str | None = None) -> dict[str, object]:
    t = datetime(2026, 10, 1, tzinfo=UTC)
    return {"case_id": f"{rule}-{key}", "rule_id": rule, "subject_key": key, "status": status,
            "resolution": resolution, "evidence": {"transactions": [{"source_ref": key}]},
            "ever_confirmed": confirmed, "anchor_time": t, "confirm_after": timedelta(hours=36),
            "last_evaluated_as_of": t, "was_suspicious": True}


def test_evaluation_counts_and_fails_on_a_delay_confirmed_as_a_leak() -> None:
    ledger = [{"anomaly_type": "MISSING_BILLING", "refs": {"recharge_id": "R1"}},
              {"anomaly_type": "MISSING_BILLING", "refs": {"recharge_id": "R2"}},
              {"anomaly_type": "DELAYED_BILLING", "refs": {"recharge_id": "R9"}}]
    cases = [_case("TEL-01", "R1", True, "CONFIRMED_BY_RULE"), _case("TEL-01", "R2", False, "SUSPICIOUS"),
             _case("TEL-01", "R9", False, "RESOLVED", "AUTO_CLEARED")]
    report = evaluate(ledger, cases)
    tel01 = report["results"]["TEL-01"]
    assert (tel01["positives"], tel01["confirmed_tp"], tel01["confirmed_fp"], tel01["open_positives"]) == (2, 1, 0, 1)
    assert tel01["precision_confirmed"] == 1.0 and tel01["recall_confirmed"] == 0.5
    assert report["passed"]

    cases[2] = _case("TEL-01", "R9", True, "RESOLVED", "AUTO_CLEARED")  # delay confirmed, then cleared by data
    report = evaluate(ledger, cases)
    assert report["benign_checks"]["DELAYED_BILLING"]["confirmed"] == 1
    assert report["benign_checks"]["confirmed then cleared by data"] == 1
    assert report["results"]["TEL-01"]["confirmed_fp"] == 1
    assert not report["passed"]
