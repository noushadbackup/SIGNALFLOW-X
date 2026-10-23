"""Run one Chaos Lab incident end to end and produce its structured incident report.

Steps: authorize → build lab (with the incident's held-back records) → baseline → inject → observe → evidence →
recover → verify against the clean-run reference → report. The run row in ``ops.chaos_runs`` (control database)
shows the current phase while it runs; the report is stored there and returned.
"""

from __future__ import annotations

import json
import time
import traceback
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from ..assurance.hunter import hunt
from ..log import get_logger
from ..observability import alerts
from ..security import Operator, audit, authorize
from . import lab as lab_mod
from .incidents import INCIDENTS, Context, check

log = get_logger(__name__)
LAB_STALL = timedelta(seconds=2)  # the lab's stall threshold for PROCESSING_STALLED (production default: 2 min)


def _phase(control: psycopg.Connection, run_id: str, phase: str) -> None:
    control.execute("UPDATE ops.chaos_runs SET phase = %s WHERE run_id = %s", (phase, run_id))


def run(control: psycopg.Connection, code: str, operator: Operator | None,
        dataset: lab_mod.LabDataset | None = None, lab_name: str | None = None) -> dict[str, Any]:
    incident = INCIDENTS[code]
    dataset = dataset or lab_mod.LabDataset()
    lab_db = lab_mod.lab_database_name(lab_name)
    run_id = f"CHAOS-{code}-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:4]}"
    operator = authorize(control, operator, "chaos.run", "chaos_incident", run_id)
    control.execute(
        """INSERT INTO ops.chaos_runs (run_id, incident, title, lab_database, dataset, operator, operator_role,
                                       status, phase) VALUES (%s, %s, %s, %s, %s, %s, %s, 'RUNNING', 'starting')""",
        (run_id, code, incident.title, lab_db, Jsonb(dataset.__dict__), operator.username, operator.role))
    with control.transaction():
        audit(control, operator.username, operator.role, "chaos.run", "OK", "chaos_incident", run_id,
              {"incident": code, "lab_database": lab_db})
    timeline: list[dict[str, Any]] = []
    report: dict[str, Any] = {"run_id": run_id, "incident": code, "title": incident.title,
                              "expected_behaviour": list(incident.expected_behaviour),
                              "lab_database": lab_db, "dataset": dataset.__dict__ | {"key": dataset.key},
                              "operator": {"username": operator.username, "role": operator.role},
                              "started_at": datetime.now(UTC).isoformat()}
    ctx = Context(run_id, dataset, operator=operator)

    def step(name: str) -> float:
        _phase(control, run_id, name)
        timeline.append({"phase": name, "at": datetime.now(UTC).isoformat()})
        return time.perf_counter()

    def done(t0: float) -> None:
        timeline[-1]["seconds"] = round(time.perf_counter() - t0, 2)

    status = "ERROR"
    try:
        t = step("reference")
        reference = lab_mod.ensure_reference(control, dataset, lab_db)
        done(t)

        t = step("build lab")
        files = len(list((dataset.ensure() / "arrivals").glob("*.jsonl")))
        fraction = incident.process_fraction()
        work = dataset.directory.parent / "work" / run_id
        ctx.build = lab_mod.build(lab_db, dataset, work, withhold=incident.withhold(dataset),
                                  process_files=int(files * fraction) if fraction else None)
        done(t)
        with lab_mod.lab_connect(lab_db) as conn:
            baseline = lab_mod.metrics(conn)
            report["lab"] = {"files_processed": ctx.build.files_processed,
                             "held_back_files": len(ctx.build.held_back_files),
                             "held_back_records": len(ctx.build.withheld_lines), "seconds": ctx.build.seconds_build}
            report["baseline"] = {"silver": baseline["silver"],
                                  "gross_recharge_bdt": baseline["gold"]["gross_recharge_bdt"],
                                  "open_cases": len(baseline["open_cases"])}

            t = step("inject")
            inject_started = datetime.now(UTC)
            report["injected"] = incident.inject(ctx, conn)
            ctx.injected["gap_cases"] = sorted(set(baseline["open_cases"]) - set(reference["open_cases"]))
            hunt(conn)
            lab_mod.dbt_lab(lab_db)  # Gold as an analyst would see it during the incident
            time.sleep(LAB_STALL.total_seconds() + 1)
            after_inject_alerts = alerts.evaluate(conn, stall_after=LAB_STALL, since=inject_started)["firing"]
            done(t)

            t = step("observe")
            observed = incident.observe(ctx, conn)
            report["observed"] = observed
            report["alerts"] = [{k: a[k] for k in ("rule", "subject", "severity", "summary", "details")}
                                for a in after_inject_alerts]
            done(t)

            t = step("evidence")
            report["evidence"] = incident.evidence(ctx, conn)
            done(t)

            t = step("recover")
            authorize(control, operator, "chaos.recover", "chaos_incident", run_id)
            recovery = incident.recover(ctx, conn)
            hunt(conn)
            lab_mod.dbt_lab(lab_db)
            alerts.evaluate(conn, stall_after=LAB_STALL, since=inject_started)
            report["recovery"] = recovery
            done(t)

            t = step("verify")
            after = lab_mod.metrics(conn) | {"alerts_after_inject": after_inject_alerts, "observed": observed,
                                             "recovery": recovery}
            checks = incident.verify(ctx, conn, reference, baseline, after)
            checks.append(check("Open Leak Hunter cases equal the clean run", reference["open_cases"],
                                after["open_cases"]))
            done(t)
        report["verification"] = checks
        status = "PASSED" if all(c["passed"] for c in checks) else "FAILED"
    except Exception as exc:  # the report must say what broke, not just that it broke
        report["error"] = {"type": type(exc).__name__, "message": str(exc)[:2000],
                           "traceback": traceback.format_exc()[-4000:]}
        log.exception("chaos incident failed", extra={"run_id": run_id})
    report["timeline"] = timeline
    report["status"] = status
    report["finished_at"] = datetime.now(UTC).isoformat()
    report["notes"] = [
        "Runs in a disposable lab database rebuilt for this incident; no other database is modified.",
        "The clean-run reference is the same dataset processed without interruption, held-back records or "
        "injected faults; it is what 'correct' means in the verification.",
        f"Recovery: {incident.recovery_kind}.",
    ]
    report = json.loads(json.dumps(report, default=str))  # timestamps and decimals as text, as stored
    control.execute("""UPDATE ops.chaos_runs SET status = %s, phase = 'finished', finished_at = clock_timestamp(),
                       report = %s WHERE run_id = %s""", (status, Jsonb(report), run_id))
    with control.transaction():
        audit(control, operator.username, operator.role, "chaos.completed", "OK" if status == "PASSED" else "FAILED",
              "chaos_incident", run_id, {"status": status})
    log.info("chaos incident finished", extra={"run_id": run_id, "status": status})
    return report
