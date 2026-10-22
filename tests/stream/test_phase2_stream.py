"""Phase 2 acceptance: Kafka + Debezium outbox + Spark Structured Streaming produce the same Silver as the lite
reference, survive hard kills without loss or duplication, and absorb a duplicate storm.

Needs `docker compose --profile stream up -d` (Kafka, Kafka Connect + Debezium, Postgres), Java 17+ for local-mode
Spark, and the jars from scripts/fetch_artifacts.sh. Each test gets its own topics, databases, replication slots
and Spark checkpoints, and removes them afterwards.
"""

from __future__ import annotations

import json
import threading
from decimal import Decimal
from pathlib import Path

import pytest

from signalflow import CONTRACTS_DIR
from signalflow.contracts import ContractRegistry
from signalflow.erp import ErpApp, connect_db
from signalflow.ingest.bronze_loader import arrival_files
from signalflow.pipeline import run_dbt
from signalflow.streaming.replay import duplicate_storm, read_line, replay
from signalflow.streaming.topics import Router

from .conftest import StreamEnv, batch_independent_fingerprints, sigkill

pytestmark = pytest.mark.stream


def _truth(data: Path) -> dict[str, object]:
    return json.loads((data / "truth" / "manifest.json").read_text())  # type: ignore[no-any-return]


def _injected_late(data: Path) -> set[str]:
    out = set()
    for raw in (data / "truth" / "anomalies.jsonl").read_text().splitlines():
        a = json.loads(raw)
        if a["anomaly_type"].startswith("LATE_"):
            out.add(a["refs"]["event_id"])
    return out


def _assert_matches_reference(env: StreamEnv, data: Path, reference: dict[str, str]) -> None:
    truth = _truth(data)
    with env.conn() as c:
        assert batch_independent_fingerprints(c) == reference
        # Every line reached Bronze exactly once, whatever was replayed.
        assert c.execute("SELECT count(*) FROM bronze.raw_events").fetchone()[0] == truth["lines_total"]  # type: ignore[index]
        assert c.execute("SELECT count(*), count(DISTINCT (source_stream, source_position)) "
                         "FROM bronze.raw_events").fetchone() == (truth["lines_total"],) * 2
        # Committed batch ids are contiguous from 0: no batch lost, none committed twice (primary key).
        ids = [r[0] for r in c.execute("SELECT batch_id FROM ops.stream_commits WHERE query_name = 'silver_events' "
                                       "ORDER BY batch_id")]
        assert ids == list(range(len(ids)))
        # Lateness: only events the generator delayed may be late, whatever the batch boundaries were.
        late = {r[0] for r in c.execute("SELECT event_id::text FROM silver.events WHERE arrival_class <> 'ON_TIME'")}
        assert late <= _injected_late(data)


def _replay_in_background(env: StreamEnv, data: Path, pace_ms: int) -> threading.Thread:
    registry = ContractRegistry.load(CONTRACTS_DIR)
    errors: list[BaseException] = []

    def run() -> None:
        try:
            replay(data, env.settings, registry, pace_ms=pace_ms)
        except BaseException as exc:  # surfaced by the caller
            errors.append(exc)

    t = threading.Thread(target=run, daemon=True)
    t.errors = errors  # type: ignore[attr-defined]
    t.start()
    return t


def test_spark_sigkill_mid_stream_then_restart_equals_lite_reference(
        stream_env: StreamEnv, seed7_data: Path, lite_reference: dict[str, str]) -> None:
    env = stream_env
    spark = env.spark("--trigger-seconds", "1")
    producer = _replay_in_background(env, seed7_data, pace_ms=700)  # ~60 s of arrivals
    try:
        env.wait_for(lambda: int(env.scalar("SELECT count(*) FROM ops.stream_commits") or 0) >= 2,
                     timeout=300, what="two committed micro-batches")
    except TimeoutError:
        sigkill(spark)
        pytest.fail("Spark never committed 2 batches:\n" + env.spark_log_tail())
    committed_at_kill = int(env.scalar("SELECT count(*) FROM bronze.raw_events") or 0)
    still_producing = producer.is_alive()
    sigkill(spark)
    producer.join(timeout=300)
    assert not producer.errors  # type: ignore[attr-defined]
    assert still_producing, "the kill must happen while records are still arriving"
    assert 0 < committed_at_kill < int(_truth(seed7_data)["lines_total"])  # type: ignore[arg-type]

    assert env.run_spark_until_drained() == 0, env.spark_log_tail()
    _assert_matches_reference(env, seed7_data, lite_reference)

    # Gold built by the same dbt project on the streaming database reconciles with the generator's truth.
    proc = run_dbt("build", database=env.settings.stream_db)
    assert proc.returncode == 0, proc.stdout[-4000:]
    with env.conn() as c:
        revenue = {(d, ch, rg): (n, int(Decimal(a) * 100)) for d, ch, rg, n, a in c.execute(
            """SELECT business_date::text, channel, region, successful_recharges, gross_recharge_bdt
               FROM gold.mart_telecom_revenue_daily WHERE successful_recharges > 0""")}
        inventory = {(w, s): int(q) for w, s, q in c.execute(
            "SELECT warehouse_id, sku, on_hand_qty FROM gold.mart_inventory_position")}
    truth = _truth(seed7_data)
    assert revenue == {(r["business_date"], r["channel"], r["region"]): (r["success_count"], r["amount_poisha"])
                       for r in truth["recharge_revenue"]}  # type: ignore[attr-defined]
    assert inventory == {(r["warehouse_id"], r["sku"]): r["quantity"]
                         for r in truth["inventory_on_hand"]}  # type: ignore[attr-defined]


def test_exit_between_our_commit_and_sparks_commit_is_replayed_as_a_noop(
        stream_env: StreamEnv, seed7_data: Path, lite_reference: dict[str, str]) -> None:
    """The narrowest crash window: Postgres has committed batch 1, Spark has not recorded it. On restart Spark
    re-executes batch 1 with the same offsets; the sink must recognise it and write nothing."""
    env = stream_env
    files = arrival_files(seed7_data / "arrivals")
    registry = ContractRegistry.load(CONTRACTS_DIR)
    replay(seed7_data, env.settings, registry, files=files[:20])
    first = env.spark("--trigger-seconds", "1", extra_env={
        "SIGNALFLOW_ALLOW_FAULT_INJECTION": "1", "SIGNALFLOW_FAULT_EXIT_AFTER_BATCH": "1"})
    env.wait_for(lambda: int(env.scalar("SELECT count(*) FROM ops.stream_commits") or 0) >= 1, timeout=300,
                 what="batch 0")
    replay(seed7_data, env.settings, registry, files=files[20:])  # gives Spark a batch 1 to commit
    assert first.wait(timeout=300) == 70, env.spark_log_tail()
    assert env.scalar("SELECT max(batch_id) FROM ops.stream_commits") == 1

    assert env.run_spark_until_drained() == 0, env.spark_log_tail()
    with env.conn() as c:
        assert c.execute("SELECT batch_id FROM ops.stream_replays").fetchall() == [(1,)]
    _assert_matches_reference(env, seed7_data, lite_reference)


def test_failure_inside_a_batch_rolls_back_and_the_retry_completes(
        stream_env: StreamEnv, seed7_data: Path, lite_reference: dict[str, str]) -> None:
    env = stream_env
    replay(seed7_data, env.settings, ContractRegistry.load(CONTRACTS_DIR))
    code = env.run_spark_until_drained(extra_env={"SIGNALFLOW_ALLOW_FAULT_INJECTION": "1",
                                                  "SIGNALFLOW_FAULT_FAIL_IN_BATCH": "0"})
    assert code == 1
    assert env.scalar("SELECT count(*) FROM bronze.raw_events") == 0  # nothing half-written
    assert env.scalar("SELECT count(*) FROM ops.stream_commits") == 0
    assert env.run_spark_until_drained() == 0, env.spark_log_tail()
    _assert_matches_reference(env, seed7_data, lite_reference)


def test_duplicate_storm_leaves_silver_and_gold_unchanged(
        stream_env: StreamEnv, seed7_data: Path, lite_reference: dict[str, str]) -> None:
    env = stream_env
    registry = ContractRegistry.load(CONTRACTS_DIR)
    replay(seed7_data, env.settings, registry)
    assert env.run_spark_until_drained() == 0, env.spark_log_tail()
    with env.conn() as c:
        before = batch_independent_fingerprints(c)
        revenue_before = c.execute("SELECT md5(string_agg(event_id::text, ',' ORDER BY event_id)) FROM silver.events "
                                   "WHERE event_type = 'telecom.recharge'").fetchone()

    copies = 3
    sent = duplicate_storm(seed7_data, env.settings, registry, copies=copies)
    recharge_lines = [read_line(line) for p in arrival_files(seed7_data / "arrivals")
                      for line in p.read_text().splitlines()]
    recharge_lines = [r for r in recharge_lines
                      if isinstance(r.event, dict) and r.event.get("event_type") == "telecom.recharge"]
    valid = sum(1 for r in recharge_lines if registry.validate(r.event).ok)
    assert sent == copies * len(recharge_lines)

    assert env.run_spark_until_drained() == 0, env.spark_log_tail()
    with env.conn() as c:
        after = batch_independent_fingerprints(c)
        assert after["silver"] == before["silver"] == lite_reference["silver"]
        # Every valid re-sent copy is logged as a duplicate; every invalid one is quarantined again (it is a new
        # record in Bronze), so nothing disappears silently.
        assert int(after["duplicates"]) - int(before["duplicates"]) == copies * valid
        assert c.execute("SELECT count(*) FROM silver.quarantine").fetchone()[0] == \
            sum(_truth(seed7_data)["expected_quarantine_by_reason"].values()) + \
            copies * (len(recharge_lines) - valid)  # type: ignore[attr-defined,index]
        assert c.execute("SELECT md5(string_agg(event_id::text, ',' ORDER BY event_id)) FROM silver.events "
                         "WHERE event_type = 'telecom.recharge'").fetchone() == revenue_before


def test_master_data_change_is_captured_by_cdc(stream_env: StreamEnv, seed7_data: Path) -> None:
    env = stream_env
    registry = ContractRegistry.load(CONTRACTS_DIR)
    with connect_db(env.settings.erp_db) as erp_conn:
        old = erp_conn.execute("SELECT credit_days FROM erp.dealers WHERE dealer_id = 'DLR-0001'").fetchone()[0]  # type: ignore[index]
        ErpApp(erp_conn, registry, Router(env.settings.topic_prefix, registry)).update_dealer_credit_days(
            "DLR-0001", old + 15)
    env.wait_for(lambda: env.run_spark_until_drained() == 0 and int(env.scalar(
        "SELECT count(*) FROM bronze.master_data_changes WHERE op = 'u'") or 0) == 1, timeout=300,
        what="dealer update in bronze.master_data_changes")
    with env.conn() as c:
        snapshot = c.execute("SELECT count(*) FROM bronze.master_data_changes WHERE op = 'r'").fetchone()[0]  # type: ignore[index]
        raw = c.execute("SELECT raw_record FROM bronze.master_data_changes WHERE op = 'u'").fetchone()[0]  # type: ignore[index]
    change = json.loads(raw)
    assert snapshot == 150 + 12 + 4  # dealers + products + warehouses from the initial snapshot
    assert change["before"]["credit_days"] == old and change["after"]["credit_days"] == old + 15
    assert change["after"]["dealer_id"] == "DLR-0001"
