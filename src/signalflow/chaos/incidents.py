"""Chaos Lab incidents A–E: what each one injects into the lab, what it observes, how it recovers, how it verifies.

Every incident follows the same steps (see ``runner.run``): build the lab, take a baseline, inject, observe, collect
evidence, recover, verify against the clean-run reference. Verification only uses what the incident itself injected
and the reference metrics; detection code (Silver rules, Leak Hunter) never sees either.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from .. import CONTRACTS_DIR
from ..assurance import cases as case_actions
from ..contracts import ContractRegistry
from ..ingest.bronze_loader import ingest_file
from ..processing.silver import PROCESSOR, process
from ..security import Operator
from .lab import LabBuild, LabDataset, silver_fingerprint

Check = dict[str, Any]


def check(name: str, expected: Any, actual: Any, passed: bool | None = None) -> Check:
    return {"name": name, "expected": expected, "actual": actual,
            "passed": bool(expected == actual) if passed is None else bool(passed)}


def iso(dt: datetime) -> str:
    dt = dt.astimezone(UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{dt.microsecond // 1000:03d}Z"


def parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def bucket(key: str, modulo: int) -> int:
    return int(hashlib.md5(key.encode()).hexdigest()[:8], 16) % modulo  # noqa: S324 - sampling, not security


@dataclass
class Context:
    run_id: str
    dataset: LabDataset
    build: LabBuild | None = None
    operator: Operator | None = None
    injected: dict[str, Any] = field(default_factory=dict)
    sent_files: list[str] = field(default_factory=list)
    seq: int = 0

    def next_event_id(self) -> str:
        import uuid

        self.seq += 1
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"signalflow-chaos:{self.run_id}:{self.seq}"))

    @property
    def chaos_dir(self) -> Path:
        assert self.build is not None
        path = self.build.work_dir / "chaos"
        path.mkdir(exist_ok=True)
        return path


def clocks(conn: psycopg.Connection) -> dict[str, datetime]:
    return {s: m for s, m in conn.execute("SELECT stream, max_event_time FROM ops.stream_state") if m is not None}


def line(event: dict[str, Any], arrival: datetime) -> str:
    return json.dumps({"arrival_time": iso(arrival), "event": event}, separators=(",", ":"))


def send(conn: psycopg.Connection, ctx: Context, step: str, lines: list[str], process_now: bool = True
         ) -> dict[str, Any]:
    """Write ``lines`` as one arrival file, load it into Bronze and (by default) process it into Silver."""
    path = ctx.chaos_dir / f"chaos-{ctx.run_id}-{step}.jsonl"
    path.write_text("".join(f"{x}\n" for x in lines))
    loaded = ingest_file(conn, path)
    ctx.sent_files.append(path.name)
    result: dict[str, Any] = {"file": path.name, "lines": loaded.lines, "bronze_inserted": loaded.inserted}
    if process_now:
        batches = process(conn, ContractRegistry.load(CONTRACTS_DIR), chunk_size=20_000)
        result.update(accepted=sum(b.accepted for b in batches), duplicates=sum(b.duplicates for b in batches),
                      quarantined=sum(sum(b.quarantined.values()) for b in batches))
    return result


def bronze_ids_for_file(conn: psycopg.Connection, file_name: str) -> list[int]:
    return [r[0] for r in conn.execute(
        "SELECT bronze_id FROM bronze.raw_events WHERE source_position LIKE %s ORDER BY bronze_id",
        (f"{file_name}:%",))]


def revenue_matches(gold: dict[str, Any], reference: dict[str, Any]) -> Check:
    differing = sorted(k for k in set(gold["revenue"]) | set(reference["revenue"])
                       if gold["revenue"].get(k) != reference["revenue"].get(k))
    return check("Gold recharge revenue per day and channel equals the clean run", [], differing[:10])


def cases_by_rule(conn: psycopg.Connection, rules: tuple[str, ...]) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""SELECT case_id, rule_id, status, resolution, summary, exposure_bdt, evidence, version
                       FROM assurance.cases WHERE rule_id = ANY(%s) ORDER BY case_id""", (list(rules),))
        return cur.fetchall()


class Incident:
    code = ""
    title = ""
    expected_behaviour: tuple[str, ...] = ()
    recovery_kind = ""

    def withhold(self, dataset: LabDataset) -> Callable[[dict[str, Any]], bool] | None:
        return None

    def process_fraction(self) -> float | None:
        return None

    def inject(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        raise NotImplementedError

    def observe(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        raise NotImplementedError

    def evidence(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        raise NotImplementedError

    def recover(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        raise NotImplementedError

    def verify(self, ctx: Context, conn: psycopg.Connection, reference: dict[str, Any],
               baseline: dict[str, Any], after: dict[str, Any]) -> list[Check]:
        raise NotImplementedError


def data_end(dataset: LabDataset) -> datetime:
    cfg = dataset.config()
    start = datetime(cfg.start_date.year, cfg.start_date.month, cfg.start_date.day, tzinfo=UTC) - timedelta(hours=6)
    return start + timedelta(days=cfg.days)


def recharge_selector(dataset: LabDataset, residue: int, modulo: int, hours_before_end: tuple[float, float]
                      ) -> Callable[[dict[str, Any]], bool]:
    """Successful recharges (and their billing charges) whose event time falls in a window before the end of the
    data, sampled by a hash of the recharge id."""
    end = data_end(dataset)
    lo, hi = end - timedelta(hours=hours_before_end[0]), end - timedelta(hours=hours_before_end[1])

    def pick(event: dict[str, Any]) -> bool:
        etype = event.get("event_type")
        payload = event.get("payload") or {}
        rid = payload.get("recharge_id")
        if etype not in ("telecom.recharge", "telecom.billing_charge") or not isinstance(rid, str):
            return False
        if bucket(rid, modulo) != residue:
            return False
        try:
            t = parse(str(event["event_time"]))
        except (KeyError, ValueError):
            return False
        if t.tzinfo is None:  # an envelope violation in the dataset; not a usable transaction
            return False
        if etype == "telecom.recharge":
            return payload.get("status") == "SUCCESS" and lo <= t <= hi
        return lo <= t <= hi + timedelta(minutes=2)
    return pick


def withheld_events(ctx: Context) -> list[dict[str, Any]]:
    assert ctx.build is not None
    return [json.loads(x)["event"] for x in ctx.build.withheld_lines]


# --- Incident A ------------------------------------------------------------------------------------------------------

class DuplicateRechargeStorm(Incident):
    code = "A"
    title = "Duplicate recharge storm"
    expected_behaviour = (
        "Every redelivered copy is logged in ops.duplicate_events against the first accepted copy",
        "Trusted (Silver) transaction counts do not change",
        "Gold revenue does not double-count: it equals the clean run",
        "No duplicate-charge case is opened (a redelivery is not a second payment)",
        "A DUPLICATE_SURGE alert names the stream and sample event ids")
    recovery_kind = "none required: first arrival wins (ADR-0006); the step re-checks the books"
    COPIES = 3
    EVENTS = 200

    def inject(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        rows = conn.execute(
            """SELECT s.event_id::text, b.raw_record FROM silver.events s JOIN bronze.raw_events b USING (bronze_id)
               WHERE s.event_type = 'telecom.recharge' AND s.payload ->> 'status' = 'SUCCESS'
               ORDER BY md5(s.event_id::text) LIMIT %s""", (self.EVENTS,)).fetchall()
        now = max(clocks(conn).values())
        lines = []
        for i, (_, raw) in enumerate(rows):
            event = json.loads(raw)["event"]
            for c in range(self.COPIES):
                lines.append(line(event, now + timedelta(milliseconds=10 * (i * self.COPIES + c))))
        ctx.injected = {"event_ids": [r[0] for r in rows], "copies_per_event": self.COPIES}
        cases_before = len(cases_by_rule(conn, ("TEL-02",)))
        ctx.injected["tel02_cases_before"] = cases_before
        return {"records_sent": len(lines), "distinct_events": len(rows), **send(conn, ctx, "storm", lines)}

    def observe(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        ids = ctx.injected["event_ids"]
        dup, distinct = conn.execute("""SELECT count(*), count(DISTINCT event_id) FROM ops.duplicate_events
                                        WHERE event_id = ANY(%s::uuid[])""", (ids,)).fetchone()  # type: ignore[misc]
        return {"duplicates_logged_for_storm_events": dup, "distinct_events_with_duplicates": distinct,
                "silver_events": silver_fingerprint(conn)["events"]}

    def evidence(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        rows = conn.execute(
            """SELECT d.event_id::text, d.first_bronze_id, array_agg(d.bronze_id ORDER BY d.bronze_id)
               FROM ops.duplicate_events d WHERE d.event_id = ANY(%s::uuid[])
               GROUP BY 1, 2 ORDER BY 1 LIMIT 5""", (ctx.injected["event_ids"],)).fetchall()
        return {"sample": [{"event_id": e, "accepted_from_bronze_id": f, "duplicate_bronze_ids": b}
                           for e, f, b in rows],
                "how_to_query": "SELECT * FROM ops.duplicate_events WHERE event_id = '<event_id>'"}

    def recover(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        return {"action": "none", "reason": self.recovery_kind}

    def verify(self, ctx: Context, conn: psycopg.Connection, reference: dict[str, Any],
               baseline: dict[str, Any], after: dict[str, Any]) -> list[Check]:
        expected_dups = len(ctx.injected["event_ids"]) * self.COPIES
        logged = conn.execute("SELECT count(*) FROM ops.duplicate_events WHERE event_id = ANY(%s::uuid[]) "
                              "AND bronze_id = ANY(%s)", (ctx.injected["event_ids"],
                                                          bronze_ids_for_file(conn, ctx.sent_files[0]))).fetchone()
        return [
            check("Every redelivered copy logged as a duplicate", expected_dups, logged[0] if logged else 0),
            check("Silver equals the clean run (count and fingerprint)", reference["silver"], after["silver"]),
            check("Silver count unchanged by the storm", baseline["silver"]["events"], after["silver"]["events"]),
            revenue_matches(after["gold"], reference["gold"]),
            check("No duplicate-charge case opened by the storm", ctx.injected["tel02_cases_before"],
                  len(cases_by_rule(conn, ("TEL-02",)))),
            check("DUPLICATE_SURGE alert raised", True, any(a["rule"] == "DUPLICATE_SURGE"
                                                             for a in after["alerts_after_inject"])),
        ]


# --- Incident B ------------------------------------------------------------------------------------------------------

class LateEventSurge(Incident):
    code = "B"
    title = "Late event surge"
    expected_behaviour = (
        "Every delayed transaction is accepted and classified by lateness (ADR-0004); none is dropped",
        "Events beyond the 6 h watermark are listed in ops.late_events; already-final days are marked RESTATED",
        "Leak Hunter cases caused by the missing transactions are auto-cleared, and none was confirmed",
        "After the Gold rebuild, revenue equals the clean run")
    recovery_kind = "rebuild Gold for the affected days (dbt), so restated KPIs include the late events"

    def withhold(self, dataset: LabDataset) -> Callable[[dict[str, Any]], bool]:
        # recharges from the last 30 h of data; delayed by up to 30 h, less than the 36 h Leak Hunter horizon
        return recharge_selector(dataset, residue=1, modulo=6, hours_before_end=(30, 0.25))

    def inject(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        events = withheld_events(ctx)
        now = max(clocks(conn).values())
        lines = [line(e, now + timedelta(milliseconds=5 * i)) for i, e in enumerate(events)]
        ctx.injected = {"event_ids": sorted({e["event_id"] for e in events}),
                        "recharges": sum(1 for e in events if e["event_type"] == "telecom.recharge"),
                        "billing_charges": sum(1 for e in events if e["event_type"] == "telecom.billing_charge")}
        return {"records_sent": len(lines), **ctx.injected | {"event_ids": len(ctx.injected["event_ids"])},
                **send(conn, ctx, "late-surge", lines)}

    def observe(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        ids = ctx.injected["event_ids"]
        classes: dict[str, int] = dict(conn.execute("""SELECT arrival_class, count(*) FROM silver.events
                                       WHERE event_id = ANY(%s::uuid[]) GROUP BY 1""", (ids,)).fetchall())
        restated = [str(d) for (d,) in conn.execute(
            """SELECT DISTINCT k.business_date FROM ops.kpi_restatements k
               JOIN ops.late_events l ON l.stream = k.stream AND l.business_date = k.business_date
               WHERE l.event_id = ANY(%s::uuid[]) ORDER BY 1""", (ids,))]
        return {"accepted_by_arrival_class": classes, "restated_business_dates": restated,
                "late_events_logged": conn.execute(
                    "SELECT count(*) FROM ops.late_events WHERE event_id = ANY(%s::uuid[])", (ids,)
                ).fetchone()[0]}  # type: ignore[index]

    def evidence(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        rows = conn.execute(
            """SELECT event_id::text, stream, event_time, watermark_at_arrival, round(lateness_seconds / 3600.0, 2),
                      business_date FROM ops.late_events WHERE event_id = ANY(%s::uuid[])
               ORDER BY lateness_seconds DESC LIMIT 5""", (ctx.injected["event_ids"],)).fetchall()
        gap = ctx.injected.get("gap_cases", [])
        cleared = conn.execute(
            """SELECT case_id, rule_id, status, resolution, summary FROM assurance.cases
               WHERE case_id = ANY(%s) ORDER BY case_id""", (gap,)).fetchall()
        return {"latest_late_events": [dict(zip(("event_id", "stream", "event_time", "watermark_at_arrival",
                                                  "lateness_hours", "business_date"), r, strict=True)) for r in rows],
                "cases_caused_by_the_missing_transactions": [
                    dict(zip(("case_id", "rule_id", "status", "resolution", "summary"), r, strict=True))
                    for r in cleared]}

    def recover(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        return {"action": "dbt rebuild of the Gold models (runner)", "reason": self.recovery_kind}

    def verify(self, ctx: Context, conn: psycopg.Connection, reference: dict[str, Any],
               baseline: dict[str, Any], after: dict[str, Any]) -> list[Check]:
        ids = ctx.injected["event_ids"]
        bronze = bronze_ids_for_file(conn, ctx.sent_files[0])
        accepted, duplicates, quarantined, invalid_ids = conn.execute(
            """SELECT (SELECT count(*) FROM silver.events WHERE bronze_id = ANY(%(b)s)),
                      (SELECT count(*) FROM ops.duplicate_events WHERE bronze_id = ANY(%(b)s)),
                      (SELECT count(*) FROM silver.quarantine WHERE bronze_id = ANY(%(b)s)),
                      (SELECT count(DISTINCT event_id) FROM silver.quarantine WHERE bronze_id = ANY(%(b)s)
                         AND NOT EXISTS (SELECT 1 FROM silver.events s WHERE s.event_id::text = quarantine.event_id))
           """, {"b": bronze}).fetchone()  # type: ignore[misc]
        beyond = conn.execute("""SELECT count(*) FROM silver.events WHERE event_id = ANY(%s::uuid[])
                                 AND arrival_class = 'LATE_BEYOND_WATERMARK'""", (ids,)).fetchone()
        logged = conn.execute("SELECT count(*) FROM ops.late_events WHERE event_id = ANY(%s::uuid[])", (ids,)
                              ).fetchone()
        restated_days = {d for (d,) in conn.execute(
            """SELECT DISTINCT l.business_date FROM ops.late_events l JOIN ops.kpi_restatements k
                   ON k.stream = l.stream AND k.business_date = l.business_date
               WHERE l.event_id = ANY(%s::uuid[])""", (ids,))}
        flagged = {d for (d,) in conn.execute(
            """SELECT DISTINCT business_date FROM gold.mart_telecom_revenue_daily
               WHERE kpi_status = 'RESTATED' AND business_date = ANY(%s)""", (list(restated_days),))}
        gap = ctx.injected.get("gap_cases", [])
        still_open_or_confirmed = conn.execute(
            """SELECT count(*) FROM assurance.cases WHERE case_id = ANY(%s)
               AND (status <> 'RESOLVED' OR confirmed_as_of IS NOT NULL)""", (gap,)).fetchone()
        return [
            check("Every delayed record accounted for (accepted, duplicate or quarantined)", len(bronze),
                  accepted + duplicates + quarantined),
            check("Every valid delayed transaction accepted into Silver (records invalid in the dataset itself "
                  "stay quarantined)", len(ids) - invalid_ids, accepted),
            check("Every beyond-watermark event listed in ops.late_events", beyond[0] if beyond else 0,
                  logged[0] if logged else 0),
            check("Restated days are flagged RESTATED in the Gold revenue mart", sorted(map(str, restated_days)),
                  sorted(map(str, flagged))),
            check("Cases caused by the missing transactions were auto-cleared and never confirmed", 0,
                  still_open_or_confirmed[0] if still_open_or_confirmed else 0),
            check("Silver equals the clean run (count and fingerprint)", reference["silver"], after["silver"]),
            revenue_matches(after["gold"], reference["gold"]),
        ]


# --- Incident C ------------------------------------------------------------------------------------------------------

def _mutations(event: dict[str, Any], i: int) -> tuple[str, str, str]:
    """(raw line content for the event field or a raw line, expected reason, description)."""
    e = copy.deepcopy(event)
    p = e["payload"]
    match i % 6:
        case 0:
            p.pop("amount_bdt", None)
            return json.dumps(e), "CONTRACT_VIOLATION", "required field amount_bdt removed"
        case 1:
            p["amount_bdt"] = f"{p.get('amount_bdt')} BDT"
            return json.dumps(e), "CONTRACT_VIOLATION", "amount_bdt sent as a string"
        case 2:
            e["schema_version"] = "2.0"
            p["amount_poisha"] = int(round(float(p.pop("amount_bdt", 0)) * 100))
            return json.dumps(e), "UNSUPPORTED_SCHEMA_VERSION", "producer switched to unregistered v2.0"
        case 3:
            p["status"] = "PARTIAL"
            return json.dumps(e), "CONTRACT_VIOLATION", "status outside its enum"
        case 4:
            e["event_time"] = str(e["event_time"]).rstrip("Z")
            return json.dumps(e), "ENVELOPE_VIOLATION", "event_time without UTC designator"
        case _:
            return "", "UNPARSEABLE", "record truncated mid-write"


class SchemaBreakingChange(Incident):
    code = "C"
    title = "Schema breaking change"
    expected_behaviour = (
        "Every incompatible record is quarantined with a reason code; Bronze keeps it verbatim",
        "The contract report names the contract, the rule broken and sample records",
        "A CONTRACT_VIOLATIONS alert is raised for operators",
        "Silver and Gold contain none of the invalid records",
        "After the producer fix, the corrected records are accepted and the books equal the clean run")
    recovery_kind = "producer fix: the same transactions re-sent in their registered v1.0 shape"

    def withhold(self, dataset: LabDataset) -> Callable[[dict[str, Any]], bool]:
        return recharge_selector(dataset, residue=2, modulo=8, hours_before_end=(20, 0.5))

    def inject(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        events = withheld_events(ctx)
        recharges = [e for e in events if e["event_type"] == "telecom.recharge"]
        now = max(clocks(conn).values())
        lines, expected = [], []
        for i, e in enumerate(recharges):
            body, reason, note = _mutations(e, i)
            arrival = iso(now + timedelta(milliseconds=5 * i))
            lines.append(f'{{"arrival_time":"{arrival}","event":{body}}}' if body
                         else f'{{"arrival_time":"{arrival}","event":{{"event_id":"{e["event_id"][:8]}')
            expected.append({"event_id": e["event_id"], "expected_reason": reason, "mutation": note})
        ctx.injected = {"broken": expected, "withheld_event_ids": sorted({e["event_id"] for e in events})}
        return {"records_sent": len(lines), **send(conn, ctx, "schema-break", lines)}

    def observe(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        ids = bronze_ids_for_file(conn, ctx.sent_files[0])
        by_reason: dict[str, int] = dict(conn.execute("""SELECT reason_code, count(*) FROM silver.quarantine
                                         WHERE bronze_id = ANY(%s) GROUP BY 1 ORDER BY 1""", (ids,)).fetchall())
        return {"quarantined_by_reason": by_reason,
                "reached_silver": conn.execute("SELECT count(*) FROM silver.events WHERE bronze_id = ANY(%s)",
                                               (ids,)).fetchone()[0]}  # type: ignore[index]

    def evidence(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        ids = bronze_ids_for_file(conn, ctx.sent_files[0])
        report = conn.execute(
            """SELECT coalesce(contract_id, event_type || '/' || schema_version, 'unparseable'), reason_code,
                      count(*), (array_agg(bronze_id ORDER BY bronze_id))[1:3], (array_agg(details))[1]
               FROM silver.quarantine WHERE bronze_id = ANY(%s) GROUP BY 1, 2 ORDER BY 1, 2""", (ids,)).fetchall()
        return {"contract_report": [{"contract": c, "reason": r, "records": n, "sample_bronze_ids": b,
                                     "violation": d} for c, r, n, b, d in report],
                "how_to_query": "SELECT * FROM silver.quarantine q JOIN bronze.raw_events b USING (bronze_id) "
                                "WHERE q.bronze_id = <id>"}

    def recover(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        events = withheld_events(ctx)
        now = max(clocks(conn).values())
        lines = [line(e, now + timedelta(milliseconds=5 * i)) for i, e in enumerate(events)]
        return {"action": "re-send corrected records", **send(conn, ctx, "producer-fix", lines)}

    def verify(self, ctx: Context, conn: psycopg.Connection, reference: dict[str, Any],
               baseline: dict[str, Any], after: dict[str, Any]) -> list[Check]:
        ids = bronze_ids_for_file(conn, ctx.sent_files[0])
        got = [r[0] for r in conn.execute("SELECT reason_code FROM silver.quarantine WHERE bronze_id = ANY(%s) "
                                          "ORDER BY bronze_id", (ids,))]
        want = [b["expected_reason"] for b in ctx.injected["broken"]]
        leaked = conn.execute("SELECT count(*) FROM silver.events WHERE bronze_id = ANY(%s)", (ids,)).fetchone()
        return [
            check("Each broken record quarantined with the expected reason", want, got),
            check("No broken record reached Silver", 0, leaked[0] if leaked else 0),
            check("CONTRACT_VIOLATIONS alert raised", True, any(a["rule"] == "CONTRACT_VIOLATIONS"
                                                                 for a in after["alerts_after_inject"])),
            check("Bronze keeps every broken record verbatim", len(want), len(ids)),
            check("After the fix, Silver equals the clean run", reference["silver"], after["silver"]),
            revenue_matches(after["gold"], reference["gold"]),
        ]


# --- Incident D ------------------------------------------------------------------------------------------------------

class StreamingInterruption(Incident):
    code = "D"
    title = "Streaming component interruption"
    expected_behaviour = (
        "The Silver processor (a separate OS process) is killed with SIGKILL while it drains a backlog",
        "The interruption is observable: the checkpoint stops, Bronze backlog stays, PROCESSING_STALLED fires",
        "No partial micro-batch is visible: every Silver row is at or below the committed checkpoint",
        "A restart resumes from the checkpoint; the books equal the clean run",
        "Zero loss is not claimed in general: it is verified here by comparing with the clean run")
    recovery_kind = "restart the processor; it resumes from the last committed checkpoint (ADR-0006)"
    CHUNK = 250

    def process_fraction(self) -> float:
        return 0.7

    def inject(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        assert ctx.build is not None
        loaded = sum(ingest_file(conn, p).inserted for p in ctx.build.held_back_files)  # backlog lands in Bronze
        checkpoint_before = self._checkpoint(conn)
        runs_before = self._runs(conn)
        env = {**os.environ, "PGDATABASE": ctx.build.database}
        proc = subprocess.Popen([sys.executable, "-m", "signalflow.chaos.worker", "--chunk-size", str(self.CHUNK)],
                                env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, start_new_session=True)
        deadline = time.monotonic() + 120
        while self._runs(conn) < runs_before + 2 and proc.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        in_txn = conn.execute(
            """SELECT count(*) FROM pg_stat_activity WHERE application_name = 'signalflow-chaos-worker'
               AND datname = current_database() AND xact_start IS NOT NULL""").fetchone()
        exited_early = proc.poll() is not None
        if not exited_early:
            os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=30)
        time.sleep(0.5)  # let Postgres notice the dead backend and roll back its transaction
        ctx.injected = {"backlog_rows": loaded, "checkpoint_before": checkpoint_before,
                        "checkpoint_at_kill": self._checkpoint(conn),
                        "batches_committed_before_kill": self._runs(conn) - runs_before,
                        "transaction_open_at_kill": bool(in_txn and in_txn[0]), "killed": not exited_early,
                        "exit_code": proc.returncode}
        return dict(ctx.injected)

    @staticmethod
    def _checkpoint(conn: psycopg.Connection) -> int:
        row = conn.execute("SELECT last_bronze_id FROM ops.checkpoints WHERE processor = %s", (PROCESSOR,)).fetchone()
        return int(row[0]) if row else 0

    @staticmethod
    def _runs(conn: psycopg.Connection) -> int:
        row = conn.execute("SELECT count(*) FROM ops.processing_runs").fetchone()
        return int(row[0]) if row else 0

    def observe(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        checkpoint = self._checkpoint(conn)
        backlog, beyond = conn.execute(
            """SELECT count(*) FILTER (WHERE bronze_id > %s),
                      (SELECT count(*) FROM silver.events WHERE bronze_id > %s)
               FROM bronze.raw_events""", (checkpoint, checkpoint)).fetchone()  # type: ignore[misc]
        ctx.injected["silver_rows_beyond_checkpoint_after_kill"] = beyond
        return {"checkpoint": checkpoint, "bronze_backlog": backlog, "silver_rows_beyond_checkpoint": beyond,
                "processor_running": bool(conn.execute(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'signalflow-chaos-worker'"
                ).fetchone()[0])}  # type: ignore[index]

    def evidence(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        rows = conn.execute(
            """SELECT run_id, from_bronze_id, to_bronze_id, rows_read, rows_accepted, finished_at
               FROM ops.processing_runs ORDER BY run_id DESC LIMIT 3""").fetchall()
        return {"last_committed_batches": [dict(zip(("run_id", "from_bronze_id", "to_bronze_id", "rows_read",
                                                     "rows_accepted", "finished_at"), r, strict=True)) for r in rows],
                "kill": {k: ctx.injected[k] for k in ("killed", "exit_code", "transaction_open_at_kill",
                                                      "checkpoint_at_kill")}}

    def recover(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        t0 = time.perf_counter()
        batches = process(conn, ContractRegistry.load(CONTRACTS_DIR), chunk_size=self.CHUNK)
        return {"action": "processor restarted", "batches": len(batches),
                "accepted": sum(b.accepted for b in batches), "seconds_to_drain": round(time.perf_counter() - t0, 2),
                "resumed_from_bronze_id": batches[0].from_bronze_id if batches else None}

    def verify(self, ctx: Context, conn: psycopg.Connection, reference: dict[str, Any],
               baseline: dict[str, Any], after: dict[str, Any]) -> list[Check]:
        accounted = conn.execute(
            """SELECT (SELECT count(*) FROM bronze.raw_events),
                      (SELECT count(*) FROM silver.events) + (SELECT count(*) FROM silver.quarantine)
                      + (SELECT count(*) FROM ops.duplicate_events)""").fetchone()
        stalled = any(a["rule"] == "PROCESSING_STALLED" for a in after["alerts_after_inject"])
        return [
            check("The processor was killed while it was working", True, ctx.injected["killed"]),
            check("No partial batch visible after the kill (Silver rows beyond the checkpoint)", 0,
                  ctx.injected["silver_rows_beyond_checkpoint_after_kill"]),
            check("PROCESSING_STALLED alert raised while the backlog waited", True, stalled),
            check("Every Bronze record accounted for exactly once (Silver + quarantine + duplicates)",
                  accounted[0] if accounted else None, accounted[1] if accounted else None),
            check("After restart, Silver equals the clean run", reference["silver"], after["silver"]),
            revenue_matches(after["gold"], reference["gold"]),
        ]


# --- Incident E ------------------------------------------------------------------------------------------------------

class StockDiscrepancy(Incident):
    code = "E"
    title = "Manufacturing stock discrepancy"
    expected_behaviour = (
        "A replayed adjustment (an approval applied twice) and an adjustment that contradicts its approval are "
        "both flagged by Leak Hunter rule MFG-04, with the root source records as evidence",
        "The inventory mart marks the affected positions UNCERTAIN instead of presenting the book as fact",
        "Recovery is an approved recount per position plus an analyst resolution of each case, all audited",
        "After recovery the affected positions are CONSISTENT and stock equals the clean run")
    recovery_kind = "approved recount per affected position, then analyst resolution of each case"

    def inject(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""SELECT m.payload AS movement, a.payload AS approval, m.event_time
                           FROM silver.events m JOIN silver.events a
                             ON a.event_type = 'mfg.stock_count_approval'
                            AND a.payload ->> 'approval_id' = m.payload ->> 'source_ref'
                           WHERE m.event_type = 'mfg.inventory_movement'
                             AND m.payload ->> 'movement_type' = 'ADJUSTMENT'
                             AND (a.payload ->> 'counted_quantity')::int - (a.payload ->> 'system_quantity')::int
                                 = (m.payload ->> 'quantity_delta')::int
                           ORDER BY m.event_time, m.payload ->> 'movement_id'""")
            adjustments = cur.fetchall()
        first, second = adjustments[0], next(a for a in adjustments[1:]
                                             if (a["movement"]["warehouse_id"], a["movement"]["sku"])
                                             != (adjustments[0]["movement"]["warehouse_id"],
                                                 adjustments[0]["movement"]["sku"]))
        now = clocks(conn)["mfg"]
        replayed = self._movement(ctx, first["movement"], first["movement"]["quantity_delta"],
                                  now - timedelta(minutes=10))
        delta = second["movement"]["quantity_delta"]
        contradicting = self._movement(ctx, second["movement"], delta + (7 if delta != -7 else 8),
                                       now - timedelta(minutes=8))
        events = [replayed, contradicting]
        ctx.injected = {"event_ids": [e["event_id"] for e in events],
                        "movements": [e["payload"]["movement_id"] for e in events],
                        "positions": sorted({f"{e['payload']['warehouse_id']}|{e['payload']['sku']}" for e in events}),
                        "deltas": {e["payload"]["movement_id"]: e["payload"]["quantity_delta"] for e in events},
                        "approvals_cited": [first["approval"]["approval_id"], second["approval"]["approval_id"]]}
        lines = [line(e, now + timedelta(seconds=1 + i)) for i, e in enumerate(events)]
        return {"records_sent": len(lines), **send(conn, ctx, "stock-conflict", lines)}

    @staticmethod
    def _envelope(ctx: Context, event_type: str, at: datetime, payload: dict[str, Any]) -> dict[str, Any]:
        return {"event_id": ctx.next_event_id(), "event_type": event_type, "schema_version": "1.0",
                "source_system": "mfg-wms", "event_time": iso(at), "produced_at": iso(at + timedelta(milliseconds=400)),
                "payload": payload}

    def _movement(self, ctx: Context, template: dict[str, Any], delta: int, at: datetime) -> dict[str, Any]:
        event = self._envelope(ctx, "mfg.inventory_movement", at, dict(template, quantity_delta=delta))
        event["payload"]["movement_id"] = "MOV-" + hashlib.sha256(event["event_id"].encode()).hexdigest()[:12].upper()
        return event

    def observe(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        flagged = [c for c in cases_by_rule(conn, ("MFG-04",))
                   if c["evidence"]["transactions"][0]["id"] in ctx.injected["movements"]]
        positions = {f"{w}|{s}": st for w, s, st in conn.execute(
            "SELECT warehouse_id, sku, position_status FROM gold.mart_inventory_position")}
        return {"cases": [{k: c[k] for k in ("case_id", "status", "summary")} for c in flagged],
                "position_status": {p: positions.get(p) for p in ctx.injected["positions"]}}

    def evidence(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        rows = conn.execute(
            """SELECT b.bronze_id, b.source_position, b.event_type, b.raw_record FROM bronze.raw_events b
               WHERE b.event_id = ANY(%s) ORDER BY b.bronze_id""", (ctx.injected["event_ids"],)).fetchall()
        flagged = [c for c in cases_by_rule(conn, ("MFG-04",))
                   if c["evidence"]["transactions"][0]["id"] in ctx.injected["movements"]]
        return {"root_source_records": [{"bronze_id": b, "position": p, "event_type": t, "raw": json.loads(r)}
                                        for b, p, t, r in rows],
                "case_evidence": {c["case_id"]: c["evidence"] for c in flagged}}

    def recover(self, ctx: Context, conn: psycopg.Connection) -> dict[str, Any]:
        assert ctx.operator is not None
        now = clocks(conn)["mfg"] + timedelta(minutes=5)
        events: list[dict[str, Any]] = []
        recounts = []
        for i, pos in enumerate(ctx.injected["positions"]):
            wh, sku = pos.split("|")
            book = conn.execute(
                """SELECT coalesce(sum((payload ->> 'quantity_delta')::int), 0) FROM silver.events
                   WHERE event_type = 'mfg.inventory_movement' AND payload ->> 'warehouse_id' = %s
                     AND payload ->> 'sku' = %s""", (wh, sku)).fetchone()[0]  # type: ignore[index]
            bad = sum(d for m, d in ctx.injected["deltas"].items()
                      if self._position_of(conn, m) == pos)
            counted = int(book) - bad  # the physical count: the injected movements never happened on the shelf
            approval_id = "SCA-" + hashlib.sha256(f"{ctx.run_id}-recount-{pos}".encode()).hexdigest()[:12].upper()
            at = now + timedelta(minutes=i)
            events.append(self._envelope(ctx, "mfg.stock_count_approval", at,
                                         {"approval_id": approval_id, "warehouse_id": wh, "sku": sku,
                                          "counted_quantity": counted, "system_quantity": int(book),
                                          "approved_by_role": "INVENTORY_CONTROLLER"}))
            events.append(self._envelope(ctx, "mfg.inventory_movement", at + timedelta(minutes=2),
                                         {"movement_id": f"MOV-{approval_id[4:]}", "warehouse_id": wh, "sku": sku,
                                          "quantity_delta": counted - int(book), "movement_type": "ADJUSTMENT",
                                          "source_ref": approval_id}))
            recounts.append({"position": pos, "book": int(book), "counted": counted, "approval_id": approval_id})
        sent = send(conn, ctx, "recount", [line(e, now + timedelta(minutes=3, seconds=i))
                                           for i, e in enumerate(events)])
        ctx.injected["recovery_event_ids"] = [e["event_id"] for e in events]
        from ..assurance.hunter import hunt

        hunt(conn)
        resolved = []
        for c in cases_by_rule(conn, ("MFG-04",)):
            if c["evidence"]["transactions"][0]["id"] not in ctx.injected["movements"] or c["status"] == "RESOLVED":
                continue
            pos = self._position_of(conn, c["evidence"]["transactions"][0]["id"])
            recount = next(r for r in recounts if r["position"] == pos)
            case_actions.resolve(conn, ctx.operator, c["case_id"], "CORRECTED_AT_SOURCE",
                                 f"Chaos Lab {ctx.run_id}: movement did not happen physically; book corrected by "
                                 f"approved recount {recount['approval_id']} ({recount['book']} -> "
                                 f"{recount['counted']}).", c["version"])
            resolved.append(c["case_id"])
        return {"action": "approved recount and case resolution", "recounts": recounts, "sent": sent,
                "cases_resolved": resolved, "resolved_by": ctx.operator.username}

    @staticmethod
    def _position_of(conn: psycopg.Connection, movement_id: str) -> str:
        row = conn.execute("""SELECT payload ->> 'warehouse_id' || '|' || (payload ->> 'sku') FROM silver.events
                              WHERE event_type = 'mfg.inventory_movement' AND payload ->> 'movement_id' = %s""",
                           (movement_id,)).fetchone()
        return str(row[0]) if row else ""

    def verify(self, ctx: Context, conn: psycopg.Connection, reference: dict[str, Any],
               baseline: dict[str, Any], after: dict[str, Any]) -> list[Check]:
        cases = {c["evidence"]["transactions"][0]["id"]: c for c in cases_by_rule(conn, ("MFG-04",))}
        flagged = [m for m in ctx.injected["movements"] if m in cases]
        resolved = [m for m in flagged if cases[m]["status"] == "RESOLVED"
                    and cases[m]["resolution"] == "CORRECTED_AT_SOURCE"]
        audit = conn.execute("""SELECT count(*) FROM ops.audit_log WHERE action = 'case.resolved' AND outcome = 'OK'
                                AND target_id = ANY(%s)""", ([cases[m]["case_id"] for m in flagged],)).fetchone()
        positions = ctx.injected["positions"]
        uncertain_seen = after["observed"]["position_status"]
        status_now = {f"{w}|{s}": st for w, s, st in conn.execute(
            "SELECT warehouse_id, sku, position_status FROM gold.mart_inventory_position")}
        new_flags = [c for c in cases_by_rule(conn, ("MFG-04",))
                     if c["evidence"]["transactions"][0]["id"] in
                     {f"MOV-{e[4:]}" for e in [r["approval_id"] for r in after["recovery"]["recounts"]]}]
        excluded = ctx.injected["event_ids"] + ctx.injected["recovery_event_ids"]
        return [
            check("Both conflicting movements flagged by MFG-04", sorted(ctx.injected["movements"]), sorted(flagged)),
            check("Affected positions were UNCERTAIN before recovery", {p: "UNCERTAIN" for p in positions},
                  uncertain_seen),
            check("Each case resolved CORRECTED_AT_SOURCE by the operator", sorted(flagged), sorted(resolved)),
            check("Each resolution is in the audit log", len(flagged), audit[0] if audit else 0),
            check("Recount adjustments are not flagged", 0, len(new_flags)),
            check("Affected positions CONSISTENT after the recount", {p: "CONSISTENT" for p in positions},
                  {p: status_now.get(p) for p in positions}),
            check("Stock equals the clean run at every position",
                  {k: v for k, v in reference["gold"]["stock"].items()},
                  {k: v for k, v in after["gold"]["stock"].items()}),
            check("Silver without the incident's own events equals the clean run", reference["silver"],
                  silver_fingerprint(conn, excluded)),
        ]


INCIDENTS: dict[str, Incident] = {i.code: i for i in (DuplicateRechargeStorm(), LateEventSurge(),
                                                      SchemaBreakingChange(), StreamingInterruption(),
                                                      StockDiscrepancy())}


def money(v: Any) -> str:
    return str(Decimal(str(v)).quantize(Decimal("0.01")))
