"""Analyst actions on Leak Hunter cases: notes, assignment, resolution and reopening.

Each action checks the operator's role, then changes the case, appends to its history and writes the audit log in
one transaction. Status-changing actions carry the case version the analyst looked at; if the hunter or another
analyst changed the case since, the action is refused rather than silently applied to a different state.
"""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from ..security import Operator, audit, authorize

RESOLUTIONS = ("RECOVERED", "CORRECTED_AT_SOURCE", "WRITTEN_OFF", "FALSE_POSITIVE", "ACCEPTED_RISK")


class CaseConflict(Exception):
    pass


def _locked(conn: psycopg.Connection, case_id: str) -> tuple[str, str | None, bool, str | None, int]:
    row = conn.execute("""SELECT status, resolution, observed, rule_status, version FROM assurance.cases
                          WHERE case_id = %s FOR UPDATE""", (case_id,)).fetchone()
    if row is None:
        raise KeyError(f"unknown case {case_id}")
    return row


def _check_version(case_id: str, version: int, expected: int | None) -> None:
    if expected is not None and version != expected:
        raise CaseConflict(f"case {case_id} changed since you opened it (version {expected} -> {version}); reload")


def _history(conn: psycopg.Connection, op: Operator, case_id: str, action: str, from_status: str | None,
             to_status: str | None, note: str | None, details: dict[str, Any] | None = None) -> None:
    conn.execute(
        """INSERT INTO assurance.case_events (case_id, action, from_status, to_status, actor, note, details)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (case_id, action, from_status, to_status, op.username, note, Jsonb(details or {})))
    audit(conn, op.username, op.role, f"case.{action.lower()}", "OK", "case", case_id,
          {"from_status": from_status, "to_status": to_status, **(details or {})})


def add_note(conn: psycopg.Connection, op: Operator | None, case_id: str, note: str) -> None:
    op = authorize(conn, op, "case.note", "case", case_id)
    if not note.strip():
        raise ValueError("note is empty")
    with conn.transaction():
        status, *_ = _locked(conn, case_id)
        _history(conn, op, case_id, "NOTE", status, status, note.strip())


def assign(conn: psycopg.Connection, op: Operator | None, case_id: str, assignee: str | None,
           expected_version: int | None = None) -> None:
    op = authorize(conn, op, "case.assign", "case", case_id)
    with conn.transaction():
        status, _, _, _, version = _locked(conn, case_id)
        _check_version(case_id, version, expected_version)
        conn.execute("UPDATE assurance.cases SET assignee = %s, version = version + 1, updated_at = now() "
                     "WHERE case_id = %s", (assignee, case_id))
        _history(conn, op, case_id, "ASSIGNED", status, status, None, {"assignee": assignee})


def resolve(conn: psycopg.Connection, op: Operator | None, case_id: str, resolution: str, note: str,
            expected_version: int | None = None) -> None:
    op = authorize(conn, op, "case.resolve", "case", case_id)
    if resolution not in RESOLUTIONS:
        raise ValueError(f"resolution must be one of {', '.join(RESOLUTIONS)}")
    if not note.strip():
        raise ValueError("a resolution needs a note that explains the evidence")
    with conn.transaction():
        status, _, _, _, version = _locked(conn, case_id)
        _check_version(case_id, version, expected_version)
        if status == "RESOLVED":
            raise CaseConflict(f"case {case_id} is already resolved")
        conn.execute(
            """UPDATE assurance.cases SET status = 'RESOLVED', resolution = %s, resolved_at = now(),
                   resolved_by = %s, resolved_as_of = last_evaluated_as_of, version = version + 1, updated_at = now()
               WHERE case_id = %s""", (resolution, op.username, case_id))
        _history(conn, op, case_id, "RESOLVED", status, "RESOLVED", note.strip(), {"resolution": resolution})


def reopen(conn: psycopg.Connection, op: Operator | None, case_id: str, note: str,
           expected_version: int | None = None) -> None:
    op = authorize(conn, op, "case.reopen", "case", case_id)
    with conn.transaction():
        status, resolution, observed, rule_status, version = _locked(conn, case_id)
        _check_version(case_id, version, expected_version)
        if status != "RESOLVED" or resolution == "AUTO_CLEARED":
            raise CaseConflict("only cases an analyst resolved can be reopened")
        if not observed or rule_status is None:
            raise CaseConflict("the discrepancy is no longer observed; there is nothing to reopen")
        conn.execute(
            """UPDATE assurance.cases SET status = %s, resolution = NULL, resolved_at = NULL, resolved_by = NULL,
                   resolved_as_of = NULL, version = version + 1, updated_at = now() WHERE case_id = %s""",
            (rule_status, case_id))
        _history(conn, op, case_id, "REOPENED", "RESOLVED", rule_status, note.strip() or None,
                 {"previous_resolution": resolution})
