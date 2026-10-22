"""Precision and recall of every Leak Hunter rule against the injected scenarios, plus the benign-delay checks.

Run after a lite pipeline run (``signalflow run``, which hunts after every arrival file):

    python -m signalflow_eval.leak_hunter --data data/seed7 [--database signalflow]

Definitions, per rule (a case "confirmed" means it reached CONFIRMED_BY_RULE at any time):
    positives            injected scenarios of the rule's type
    confirmed_tp / _fp   confirmed cases that are / are not injected scenarios
    precision_confirmed  confirmed_tp / confirmed (None when nothing was confirmed)
    recall_confirmed     confirmed_tp / positives
    open_positives       positives with a case still PENDING or SUSPICIOUS: the horizon has not passed yet
    missed               positives with no case at all (MFG-05: includes invoices not yet due)
Benign checks: delayed billing and late dealer payments must never be confirmed, TEL-05 (a delay detector) must
never confirm, and no case may be confirmed and later cleared by data (that would be a delay mistaken for a loss).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

Refs = dict[str, Any]

# rule -> (scenario type, key of the scenario that equals the case's match key)
SCENARIOS: dict[str, tuple[str, Callable[[Refs], str]] | None] = {
    "TEL-01": ("MISSING_BILLING", lambda r: r["recharge_id"]),
    "TEL-02": ("DUPLICATE_PAYMENT_CHARGE", lambda r: r["payment_txn_ref"]),
    "TEL-03": ("ACTIVATION_WITHOUT_CAPTURE", lambda r: r["activation_id"]),
    "TEL-04": ("SETTLEMENT_MISMATCH", lambda r: r["settlement_id"]),
    "TEL-05": None,  # no injected scenario: every case is a delay and must clear
    "MFG-01": ("SHIPMENT_WITHOUT_INVOICE", lambda r: r["shipment_id"]),
    "MFG-02": ("DEALER_SETTLEMENT_MISMATCH", lambda r: r["invoice_id"]),
    "MFG-03": ("DOUBLE_REFUND", lambda r: r["return_id"]),
    "MFG-04": ("UNAUTHORIZED_ADJUSTMENT", lambda r: r["source_ref"]),
    "MFG-05": ("DEALER_NON_PAYMENT", lambda r: r["invoice_id"]),
}
BENIGN: dict[str, tuple[str, Callable[[Refs], str]]] = {
    "DELAYED_BILLING": ("TEL-01", lambda r: r["recharge_id"]),
    "DELAYED_DEALER_PAYMENT": ("MFG-05", lambda r: r["invoice_id"]),
}


def match_key(case: dict[str, Any]) -> str:
    """The identifier the scenario ledger uses for this case's subject."""
    if case["rule_id"] == "MFG-04":
        return str(case["evidence"]["transactions"][0]["source_ref"])
    return str(case["subject_key"])


def load_ledger(data: Path) -> list[dict[str, Any]]:
    path = data / "truth" / "anomalies.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def load_cases(conn: psycopg.Connection) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""SELECT c.case_id, c.rule_id, c.subject_key, c.status, c.resolution, c.evidence,
                              c.confirmed_as_of IS NOT NULL AS ever_confirmed, c.anchor_time,
                              r.confirm_after, c.last_evaluated_as_of,
                              EXISTS (SELECT 1 FROM assurance.case_events e WHERE e.case_id = c.case_id
                                      AND e.to_status = 'SUSPICIOUS') AS was_suspicious
                       FROM assurance.cases c JOIN assurance.rules r USING (rule_id)""")
        return cur.fetchall()


def evaluate(ledger: list[dict[str, Any]], cases: list[dict[str, Any]]) -> dict[str, Any]:
    by_rule: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for c in cases:
        by_rule[c["rule_id"]][match_key(c)] = c
    results: dict[str, Any] = {}
    for rule_id, scenario in SCENARIOS.items():
        rc = by_rule.get(rule_id, {})
        confirmed = {k for k, c in rc.items() if c["ever_confirmed"]}
        if scenario is None:
            results[rule_id] = {"scenario": None, "cases": len(rc), "confirmed": len(confirmed),
                                "auto_cleared": sum(c["resolution"] == "AUTO_CLEARED" for c in rc.values()),
                                "open": sum(c["status"] != "RESOLVED" for c in rc.values())}
            continue
        kind, key = scenario
        positives = {key(a["refs"]) for a in ledger if a["anomaly_type"] == kind}
        tp, fp = confirmed & positives, confirmed - positives
        open_pos = {k for k in positives if k in rc and rc[k]["status"] in ("PENDING", "SUSPICIOUS")}
        flagged_noise = {k for k, c in rc.items() if k not in positives and c["status"] != "RESOLVED"}
        results[rule_id] = {
            "scenario": kind, "positives": len(positives), "cases": len(rc), "confirmed": len(confirmed),
            "confirmed_tp": len(tp), "confirmed_fp": len(fp),
            "precision_confirmed": round(len(tp) / len(confirmed), 4) if confirmed else None,
            "recall_confirmed": round(len(tp) / len(positives), 4) if positives else None,
            "open_positives": len(open_pos), "missed": len(positives - rc.keys()),
            "open_non_positives": len(flagged_noise),
            "auto_cleared": sum(c["resolution"] == "AUTO_CLEARED" for c in rc.values()),
            "false_confirmations": sorted(rc[k]["case_id"] for k in fp),
        }
    benign: dict[str, Any] = {}
    for kind, (rule_id, key) in BENIGN.items():
        keys = {key(a["refs"]) for a in ledger if a["anomaly_type"] == kind}
        seen = [by_rule[rule_id][k] for k in keys if k in by_rule.get(rule_id, {})]
        benign[kind] = {"scenarios": len(keys), "cases_opened": len(seen),
                        "cases_suspicious_at_some_point": sum(c["was_suspicious"] for c in seen),
                        "confirmed": sum(c["ever_confirmed"] for c in seen),
                        "still_open": sum(c["status"] != "RESOLVED" for c in seen)}
    benign["TEL-05 confirmed"] = results["TEL-05"]["confirmed"]
    benign["confirmed then cleared by data"] = sum(c["ever_confirmed"] and c["resolution"] == "AUTO_CLEARED"
                                                   for c in cases)
    passed = (all(v["confirmed"] == 0 for k, v in benign.items() if isinstance(v, dict))
              and benign["TEL-05 confirmed"] == 0 and benign["confirmed then cleared by data"] == 0
              and all(r.get("confirmed_fp", 0) == 0 for r in results.values()))
    return {"results": results, "benign_checks": benign, "passed": passed}


def run(data: Path, conn: psycopg.Connection, record: bool = True) -> dict[str, Any]:
    report = evaluate(load_ledger(data), load_cases(conn))
    manifest = json.loads((data / "truth" / "manifest.json").read_text())
    report["dataset"] = manifest["generator_config"]
    if record:
        with conn.transaction():
            conn.execute("""INSERT INTO assurance.evaluation_runs (dataset, results, benign_checks, passed)
                            VALUES (%s, %s, %s, %s)""",
                         (Jsonb(report["dataset"]), Jsonb(report["results"]), Jsonb(report["benign_checks"]),
                          report["passed"]))
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m signalflow_eval.leak_hunter")
    parser.add_argument("--data", required=True, help="dataset directory the database was built from")
    parser.add_argument("--database", help="Postgres database (default PGDATABASE)")
    parser.add_argument("--no-record", action="store_true")
    args = parser.parse_args(argv)
    kwargs = {"dbname": args.database} if args.database else {}
    with psycopg.connect(autocommit=True, **kwargs) as conn:
        report = run(Path(args.data), conn, record=not args.no_record)
    print(json.dumps(report, indent=2, default=str))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
