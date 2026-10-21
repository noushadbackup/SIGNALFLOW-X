"""Phase 1 acceptance: generator truth → Bronze → Silver → dbt Gold, on a real PostgreSQL.

Every assertion compares the pipeline with the generator's ground truth or with an independent re-computation.
"""

from __future__ import annotations

import json
import os
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import pytest

from signalflow import CONTRACTS_DIR
from signalflow.contracts import ContractRegistry, parse_utc
from signalflow.generator import GeneratorConfig, generate
from signalflow.ingest.bronze_loader import arrival_files, ingest_file
from signalflow.pipeline import ingest_and_process, prepare, run_dbt
from signalflow.processing.lateness import classify
from signalflow.processing.silver import PROCESSOR, InjectedFault, ProcessorBusy, process

from .conftest import temp_database

pytestmark = pytest.mark.integration
CONFIG = GeneratorConfig(seed=7)  # the documented demo dataset
CHUNK = 100_000  # larger than any arrival file, so each file is exactly one micro-batch

GOLD_MARTS = {
    "gold.mart_telecom_revenue_daily": "business_date, region, channel",
    "gold.mart_telecom_subscribers_daily": "business_date, home_region, segment",
    "gold.mart_telecom_usage_daily": "business_date, serving_region, usage_type",
    "gold.mart_inventory_position": "warehouse_id, sku",
    "gold.mart_mfg_sales_daily": "business_date, dealer_region, category",
    "gold.fct_channel_settlement": "settlement_id",
    "gold.fct_invoice": "invoice_id",
}


@dataclass
class CleanRun:
    data: Path
    truth: dict[str, Any]
    conn: psycopg.Connection
    dbt_output: str
    second_pass: Any
    fingerprints_before_rerun: dict[str, str]


def fingerprints(conn: psycopg.Connection) -> dict[str, str]:
    """Order-independent content hashes; wall-clock columns (ingested_at, processed_at, ...) are excluded."""
    queries = {
        "silver": "SELECT md5(string_agg(event_id::text || arrival_class || lateness_seconds || event_sha256 "
                  "|| bronze_id, ',' ORDER BY event_id)) FROM silver.events",
        "quarantine": "SELECT md5(string_agg(bronze_id || reason_code, ',' ORDER BY bronze_id)) FROM silver.quarantine",
        "duplicates": "SELECT md5(string_agg(bronze_id || ':' || first_bronze_id, ',' ORDER BY bronze_id)) "
                      "FROM ops.duplicate_events",
        "stream_state": "SELECT md5(string_agg(stream || max_event_time || watermark, ',' ORDER BY stream)) "
                        "FROM ops.stream_state",
        "checkpoint": "SELECT last_bronze_id::text FROM ops.checkpoints",
    }
    for table, order in GOLD_MARTS.items():
        queries[table] = f"SELECT md5(string_agg(t::text, ',' ORDER BY {order})) FROM {table} t"
    return {k: str(conn.execute(q).fetchone()[0]) for k, q in queries.items()}  # type: ignore[index]


@pytest.fixture(scope="module")
def dataset(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out = tmp_path_factory.mktemp("e2e") / "seed7"
    generate(CONFIG, out)
    return out


@pytest.fixture(scope="module")
def clean_run(dataset: Path) -> Iterator[CleanRun]:
    with temp_database(), psycopg.connect(autocommit=True) as conn:
        prepare(conn, dataset)
        ingest_and_process(conn, dataset, chunk_size=CHUNK)
        dbt = run_dbt("build")
        assert dbt.returncode == 0, dbt.stdout[-4000:]
        before = fingerprints(conn)
        second = ingest_and_process(conn, dataset, chunk_size=CHUNK)  # replay the whole transport
        dbt2 = run_dbt("build")
        assert dbt2.returncode == 0, dbt2.stdout[-4000:]
        truth = json.loads((dataset / "truth" / "manifest.json").read_text())
        yield CleanRun(dataset, truth, conn, dbt.stdout, second, before)


def scalar(conn: psycopg.Connection, sql: str) -> Any:
    return conn.execute(sql).fetchone()[0]  # type: ignore[index]


# ---------------------------------------------------------------------------------------------------------------
def test_dbt_build_passes_all_models_and_tests(clean_run: CleanRun) -> None:
    summary = [line for line in clean_run.dbt_output.splitlines() if "Done. PASS=" in line]
    assert summary and "ERROR=0" in summary[0] and "WARN=0" in summary[0], summary


def test_bronze_keeps_every_line_verbatim(clean_run: CleanRun) -> None:
    c = clean_run.conn
    assert scalar(c, "SELECT count(*) FROM bronze.raw_events") == clean_run.truth["lines_total"]
    assert scalar(c, "SELECT count(*) FROM bronze.raw_events WHERE parse_error IS NOT NULL") == \
        clean_run.truth["expected_quarantine_by_reason"]["UNPARSEABLE"]
    first_file = arrival_files(clean_run.data / "arrivals")[0]
    first_line = first_file.read_text().splitlines()[0]
    stored = scalar(c, f"SELECT raw_record FROM bronze.raw_events WHERE source_position = '{first_file.name}:1'")
    assert stored == first_line


def test_silver_contains_exactly_the_valid_unique_events(clean_run: CleanRun) -> None:
    expected = set((clean_run.data / "truth" / "valid_event_ids.txt").read_text().split())
    actual = {r[0] for r in clean_run.conn.execute("SELECT event_id::text FROM silver.events")}
    assert actual == expected


def test_quarantine_matches_injected_breaks(clean_run: CleanRun) -> None:
    rows = clean_run.conn.execute("SELECT reason_code, count(*) FROM silver.quarantine GROUP BY 1").fetchall()
    assert dict(rows) == clean_run.truth["expected_quarantine_by_reason"]


def test_technical_duplicates_logged_not_counted(clean_run: CleanRun) -> None:
    assert scalar(clean_run.conn, "SELECT count(*) FROM ops.duplicate_events") == \
        clean_run.truth["technical_duplicates"]


def test_arrival_classes_follow_documented_batch_watermark_semantics(clean_run: CleanRun) -> None:
    """Independent re-computation from the arrival files: one micro-batch per file, clock = max accepted event
    time before the batch, first occurrence of a valid event_id wins."""
    registry = ContractRegistry.load(CONTRACTS_DIR)
    clock: dict[str, Any] = {"telecom": None, "mfg": None}
    seen: set[str] = set()
    expected: dict[str, str] = {}
    for path in arrival_files(clean_run.data / "arrivals"):
        batch_max: dict[str, Any] = {}
        for line in path.read_text().splitlines():
            try:
                event = json.loads(line)["event"]
            except (ValueError, KeyError):
                continue
            if not registry.validate(event).ok or event["event_id"] in seen:
                continue
            seen.add(event["event_id"])
            stream = "telecom" if event["event_type"].startswith("telecom.") else "mfg"
            et = parse_utc(event["event_time"])
            expected[event["event_id"]] = classify(et, clock[stream])[0]
            batch_max[stream] = max(et, batch_max.get(stream, et))
        for s, m in batch_max.items():
            clock[s] = m if clock[s] is None else max(clock[s], m)
    actual = dict(clean_run.conn.execute("SELECT event_id::text, arrival_class FROM silver.events").fetchall())
    assert actual == expected
    assert Counter(actual.values())["LATE_BEYOND_WATERMARK"] > 0


def test_no_unplanned_lateness(clean_run: CleanRun) -> None:
    """Only events the generator delayed may be classified late (no false positives from normal jitter)."""
    injected = set()
    for raw in (clean_run.data / "truth" / "anomalies.jsonl").read_text().splitlines():
        a = json.loads(raw)
        if a["anomaly_type"].startswith("LATE_"):
            injected.add(a["refs"]["event_id"])
    late = {r[0] for r in clean_run.conn.execute(
        "SELECT event_id::text FROM silver.events WHERE arrival_class <> 'ON_TIME'")}
    assert late <= injected


def test_gold_revenue_equals_truth_to_the_poisha(clean_run: CleanRun) -> None:
    rows = clean_run.conn.execute(
        """SELECT business_date::text, channel, region, successful_recharges, gross_recharge_bdt
           FROM gold.mart_telecom_revenue_daily WHERE successful_recharges > 0""").fetchall()
    actual = {(d, ch, rg): (n, int(Decimal(a) * 100)) for d, ch, rg, n, a in rows}
    expected = {(r["business_date"], r["channel"], r["region"]): (r["success_count"], r["amount_poisha"])
                for r in clean_run.truth["recharge_revenue"]}
    assert actual == expected


def test_gold_inventory_equals_truth(clean_run: CleanRun) -> None:
    rows = clean_run.conn.execute("SELECT warehouse_id, sku, on_hand_qty FROM gold.mart_inventory_position").fetchall()
    expected = {(r["warehouse_id"], r["sku"]): r["quantity"] for r in clean_run.truth["inventory_on_hand"]}
    assert {(w, s): int(q) for w, s, q in rows} == expected


def test_settlement_reconciliation_exposes_injected_mismatch(clean_run: CleanRun) -> None:
    flagged = {r[0] for r in clean_run.conn.execute(
        "SELECT settlement_id FROM gold.fct_channel_settlement WHERE difference_bdt <> 0")}
    injected = {json.loads(raw)["refs"]["settlement_id"]
                for raw in (clean_run.data / "truth" / "anomalies.jsonl").read_text().splitlines()
                if json.loads(raw)["anomaly_type"] == "SETTLEMENT_MISMATCH"}
    assert flagged == injected and injected


def test_replaying_all_files_changes_nothing(clean_run: CleanRun) -> None:
    assert clean_run.second_pass.bronze_inserted == 0
    assert clean_run.second_pass.accepted == 0
    assert fingerprints(clean_run.conn) == clean_run.fingerprints_before_rerun


def test_crash_mid_batch_then_resume_equals_clean_run(clean_run: CleanRun, dataset: Path,
                                                      monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SIGNALFLOW_ALLOW_FAULT_INJECTION", "1")
    files = arrival_files(dataset / "arrivals")
    crash_at = len(files) // 2
    registry = ContractRegistry.load(CONTRACTS_DIR)
    with temp_database(), psycopg.connect(autocommit=True) as conn:
        prepare(conn, dataset)
        ingest_and_process(conn, dataset, chunk_size=CHUNK, files=files[:crash_at])
        silver_before = scalar(conn, "SELECT count(*) FROM silver.events")
        checkpoint_before = scalar(conn, f"SELECT last_bronze_id FROM ops.checkpoints WHERE processor = '{PROCESSOR}'")

        ingest_file(conn, files[crash_at])
        with pytest.raises(InjectedFault):
            process(conn, registry, chunk_size=CHUNK, fail_after_rows=0)  # dies after writing, before commit
        # Nothing of the failed batch is visible; checkpoint did not move.
        assert scalar(conn, "SELECT count(*) FROM silver.events") == silver_before
        assert scalar(conn, f"SELECT last_bronze_id FROM ops.checkpoints WHERE processor = '{PROCESSOR}'") \
            == checkpoint_before

        process(conn, registry, chunk_size=CHUNK)  # restart
        ingest_and_process(conn, dataset, chunk_size=CHUNK, files=files[crash_at + 1:])
        dbt = run_dbt("build")
        assert dbt.returncode == 0, dbt.stdout[-4000:]
        assert fingerprints(conn) == clean_run.fingerprints_before_rerun


def test_second_processor_is_refused_while_first_holds_the_checkpoint(dataset: Path) -> None:
    registry = ContractRegistry.load(CONTRACTS_DIR)
    with temp_database(), psycopg.connect(autocommit=True) as first, psycopg.connect(autocommit=True) as second:
        prepare(first, dataset)
        ingest_file(first, arrival_files(dataset / "arrivals")[0])
        with first.transaction():
            first.execute("INSERT INTO ops.checkpoints (processor) VALUES (%s) ON CONFLICT DO NOTHING", (PROCESSOR,))
        with first.transaction():
            first.execute("SELECT 1 FROM ops.checkpoints WHERE processor = %s FOR UPDATE", (PROCESSOR,))
            with pytest.raises(ProcessorBusy):
                process(second, registry)


def test_fault_injection_is_disabled_by_default(dataset: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SIGNALFLOW_ALLOW_FAULT_INJECTION", raising=False)
    assert os.environ.get("SIGNALFLOW_ALLOW_FAULT_INJECTION") is None
    with temp_database(), psycopg.connect(autocommit=True) as conn, pytest.raises(PermissionError):
        process(conn, ContractRegistry.load(CONTRACTS_DIR), fail_after_rows=1)
