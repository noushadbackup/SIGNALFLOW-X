"""Revenue Leak Hunter: run every rule against committed Silver and move cases through their lifecycle.

One run is one transaction under an advisory lock, so concurrent runs serialise and a crash leaves the previous
state. Ages are measured on each stream's event-time clock (``ops.stream_state.max_event_time``), never on the wall
clock, so replaying the same arrivals produces the same case history.

Lifecycle per (rule, subject):
    observed for the first time          -> OPENED with the status the rule computes
    status computed differently          -> STATUS_CHANGED (PENDING -> SUSPICIOUS -> CONFIRMED_BY_RULE)
    no longer observed (counterpart came) -> AUTO_CLEARED (status RESOLVED, resolution AUTO_CLEARED)
    observed again after auto-clearing   -> REOPENED
Cases an analyst resolved keep their resolution; the hunter only refreshes their evidence.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from ..db import require_autocommit
from ..log import get_logger
from .rules import RULES, Rule

ACTOR = "system:leak-hunter"
log = get_logger(__name__)


@dataclass
class HuntResult:
    run_id: int = 0
    as_of: dict[str, str | None] = field(default_factory=dict)
    observed: int = 0
    opened: int = 0
    status_changes: int = 0
    auto_cleared: int = 0
    reopened: int = 0
    per_rule: dict[str, dict[str, Any]] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {"run_id": self.run_id, "as_of": self.as_of, "observed": self.observed, "opened": self.opened,
                "status_changes": self.status_changes, "auto_cleared": self.auto_cleared,
                "reopened": self.reopened, "per_rule": self.per_rule}


def evidence_digest(evidence: Any) -> str:
    return hashlib.sha256(json.dumps(evidence, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def sync_rules(conn: psycopg.Connection, rules: tuple[Rule, ...] = RULES) -> None:
    for r in rules:
        conn.execute(
            """INSERT INTO assurance.rules (rule_id, domain, title, description, exposure_kind, expected_within,
                                            confirm_after, sql_sha256)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (rule_id) DO UPDATE SET domain = EXCLUDED.domain, title = EXCLUDED.title,
                   description = EXCLUDED.description, exposure_kind = EXCLUDED.exposure_kind,
                   expected_within = EXCLUDED.expected_within, confirm_after = EXCLUDED.confirm_after,
                   sql_sha256 = EXCLUDED.sql_sha256, updated_at = now()
               WHERE (assurance.rules.title, assurance.rules.description, assurance.rules.expected_within,
                      assurance.rules.confirm_after, assurance.rules.sql_sha256, assurance.rules.exposure_kind)
                     IS DISTINCT FROM (EXCLUDED.title, EXCLUDED.description, EXCLUDED.expected_within,
                                       EXCLUDED.confirm_after, EXCLUDED.sql_sha256, EXCLUDED.exposure_kind)""",
            (r.rule_id, r.domain, r.title, r.description, r.exposure_kind, r.expected_within, r.confirm_after,
             r.sql_sha256))


def stream_clocks(conn: psycopg.Connection) -> dict[str, datetime | None]:
    return {s: m for s, m in conn.execute("SELECT stream, max_event_time FROM ops.stream_state")}


def observe(conn: psycopg.Connection, rule: Rule, as_of: datetime) -> list[dict[str, Any]]:
    """Rows the rule observes at ``as_of``; only rules that use the clock take the parameter."""
    params = {"as_of": as_of} if "%(as_of)s" in rule.sql else None
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(rule.sql, params)  # SQL comes from a reviewed file in sql/assurance, never from input
        rows = cur.fetchall()
    keys = [r["subject_key"] for r in rows]
    if len(keys) != len(set(keys)):
        raise ValueError(f"rule {rule.rule_id} returned the same subject twice")
    return rows


def _event(conn: psycopg.Connection, case_id: str, action: str, from_status: str | None, to_status: str | None,
           as_of: datetime, details: dict[str, Any] | None = None) -> None:
    conn.execute(
        """INSERT INTO assurance.case_events (case_id, action, from_status, to_status, actor, as_of, details)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (case_id, action, from_status, to_status, ACTOR, as_of, Jsonb(details or {})))


def hunt(conn: psycopg.Connection, rules: tuple[Rule, ...] = RULES) -> HuntResult:
    require_autocommit(conn)
    started = datetime.now(UTC)
    result = HuntResult()
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(hashtext('signalflow.leak-hunter'))")
        sync_rules(conn, rules)
        clocks = stream_clocks(conn)
        result.as_of = {s: (c.isoformat() if c else None) for s, c in sorted(clocks.items())}
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""SELECT case_id, rule_id, status, rule_status, observed, resolution, evidence_sha256,
                                  confirmed_as_of FROM assurance.cases""")
            existing = {r["case_id"]: r for r in cur.fetchall()}
        for rule in rules:
            clock = clocks.get(rule.domain)
            if clock is None:
                result.per_rule[rule.rule_id] = {"skipped": "no events on this stream yet"}
                continue
            seen = _apply_rule(conn, rule, clock, existing, result)
            for case in existing.values():
                if case["rule_id"] != rule.rule_id or not case["observed"] or case["case_id"] in seen:
                    continue
                _clear(conn, case, clock, result)
            counts = Counter(c["status"] for c in existing.values() if c["rule_id"] == rule.rule_id)
            result.per_rule[rule.rule_id] = {"observed": len(seen), "as_of": clock.isoformat(), **dict(counts)}
        result.run_id = int(conn.execute(  # type: ignore[index]
            """INSERT INTO assurance.hunter_runs (as_of, observed, opened, status_changes, auto_cleared, reopened,
                                                  per_rule, started_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING run_id""",
            (Jsonb(result.as_of), result.observed, result.opened, result.status_changes, result.auto_cleared,
             result.reopened, Jsonb(result.per_rule), started)).fetchone()[0])
    log.info("leak hunter run", extra={k: v for k, v in result.as_dict().items() if k != "per_rule"})
    return result


def _apply_rule(conn: psycopg.Connection, rule: Rule, clock: datetime, existing: dict[str, dict[str, Any]],
                result: HuntResult) -> set[str]:
    seen: set[str] = set()
    for row in observe(conn, rule, clock):
        case_id = rule.case_id(row["subject_key"])
        seen.add(case_id)
        result.observed += 1
        status = rule.status_for(clock - row["anchor_time"])
        digest = evidence_digest(row["evidence"])
        exposure = Decimal(row["exposure_bdt"] or 0).quantize(Decimal("0.01"))
        case = existing.get(case_id)
        if case is None:
            conn.execute(
                """INSERT INTO assurance.cases (case_id, rule_id, domain, subject_key, status, rule_status, observed,
                       business_unit, region_code, business_date, anchor_time, summary, exposure_bdt, evidence,
                       evidence_sha256, first_detected_as_of, confirmed_as_of, last_evaluated_as_of)
                   VALUES (%s, %s, %s, %s, %s, %s, true, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (case_id, rule.rule_id, rule.domain, row["subject_key"], status, status, row["business_unit"],
                 row["region_code"], row["business_date"], row["anchor_time"], row["summary"], exposure,
                 Jsonb(row["evidence"]), digest, clock, clock if status == "CONFIRMED_BY_RULE" else None, clock))
            _event(conn, case_id, "OPENED", None, status, clock, {"exposure_bdt": str(exposure)})
            existing[case_id] = {"case_id": case_id, "rule_id": rule.rule_id, "status": status, "rule_status": status,
                                 "observed": True, "resolution": None, "evidence_sha256": digest,
                                 "confirmed_as_of": clock if status == "CONFIRMED_BY_RULE" else None}
            result.opened += 1
            continue

        new_status, action = case["status"], None
        if case["status"] == "RESOLVED" and case["resolution"] == "AUTO_CLEARED":
            new_status, action = status, "REOPENED"
            result.reopened += 1
        elif case["status"] != "RESOLVED" and case["status"] != status:
            new_status, action = status, "STATUS_CHANGED"
            result.status_changes += 1
        unchanged = (action is None and case["observed"] and case["rule_status"] == status
                     and case["evidence_sha256"] == digest)
        if unchanged:
            continue
        confirmed_as_of = case["confirmed_as_of"] or (clock if new_status == "CONFIRMED_BY_RULE" else None)
        reopening = action == "REOPENED"
        conn.execute(
            """UPDATE assurance.cases SET status = %s, rule_status = %s, observed = true, summary = %s,
                   exposure_bdt = %s, evidence = %s, evidence_sha256 = %s, confirmed_as_of = %s,
                   last_evaluated_as_of = %s, updated_at = now(),
                   resolution = CASE WHEN %s THEN NULL ELSE resolution END,
                   resolved_at = CASE WHEN %s THEN NULL ELSE resolved_at END,
                   resolved_as_of = CASE WHEN %s THEN NULL ELSE resolved_as_of END,
                   resolved_by = CASE WHEN %s THEN NULL ELSE resolved_by END,
                   version = version + CASE WHEN %s THEN 1 ELSE 0 END
               WHERE case_id = %s""",
            (new_status, status, row["summary"], exposure, Jsonb(row["evidence"]), digest, confirmed_as_of, clock,
             reopening, reopening, reopening, reopening, action is not None, case_id))
        if action:
            _event(conn, case_id, action, case["status"], new_status, clock)
        case.update(status=new_status, rule_status=status, observed=True, evidence_sha256=digest,
                    confirmed_as_of=confirmed_as_of, resolution=None if reopening else case["resolution"])
    return seen


def _clear(conn: psycopg.Connection, case: dict[str, Any], clock: datetime, result: HuntResult) -> None:
    """The discrepancy is no longer observed: the counterpart arrived or the amounts now agree."""
    if case["status"] == "RESOLVED":  # an analyst already resolved it; just record that it is gone
        conn.execute("""UPDATE assurance.cases SET observed = false, rule_status = NULL, last_evaluated_as_of = %s,
                            updated_at = now() WHERE case_id = %s""", (clock, case["case_id"]))
        case.update(observed=False, rule_status=None)
        return
    conn.execute(
        """UPDATE assurance.cases SET status = 'RESOLVED', resolution = 'AUTO_CLEARED', observed = false,
               rule_status = NULL, resolved_at = now(), resolved_as_of = %s, resolved_by = %s,
               last_evaluated_as_of = %s, updated_at = now(), version = version + 1
           WHERE case_id = %s""", (clock, ACTOR, clock, case["case_id"]))
    _event(conn, case["case_id"], "AUTO_CLEARED", case["status"], "RESOLVED", clock,
           {"was_confirmed": case["status"] == "CONFIRMED_BY_RULE"})
    case.update(status="RESOLVED", resolution="AUTO_CLEARED", observed=False, rule_status=None)
    result.auto_cleared += 1
