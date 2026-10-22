"""Phase 3 acceptance: Postgres -> Iceberg publication, dbt on Trino, and the lite-vs-lake differential test.

Needs `docker compose --profile lake up` (Postgres, SeaweedFS, Trino), Java 17+ and the lake jars. Every test
uses its own database and its own namespace prefix inside the Iceberg catalog, and drops both afterwards.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import psycopg
import pytest

from signalflow.ingest.bronze_loader import arrival_files
from signalflow.lakehouse.admin import run_dbt_lake
from signalflow.lakehouse.compare import compare_gold
from signalflow.lakehouse.settings import CATALOG
from signalflow.lakehouse.tables import TABLES
from signalflow.lakehouse.trino_sql import query, snapshots
from signalflow.pipeline import ingest_and_process, prepare, run_dbt

from .conftest import LakeEnv

pytestmark = pytest.mark.lake

# Natural key of each published table: row count and distinct-key count must both equal Postgres.
KEYS = {
    "bronze.raw_events": "bronze_id", "bronze.master_data_changes": "change_id", "silver.events": "event_id",
    "silver.quarantine": "bronze_id", "ops.duplicate_events": "bronze_id", "ops.late_events": "event_id",
    "ops.processing_runs": "run_id", "ops.stream_state": "stream",
    "ops.kpi_restatements": "stream || cast(business_date as varchar)", "ref.regions": "region_code",
    "ref.packages": "package_code", "ref.products": "sku", "ref.factories": "factory_id",
    "ref.warehouses": "warehouse_id", "ref.dealers": "dealer_id",
}
SILVER_FINGERPRINT_PG = ("SELECT md5(string_agg(event_id::text || ':' || event_sha256, ',' ORDER BY event_id::text)) "
                         "FROM silver.events")


def silver_fingerprint_lake(env: LakeEnv) -> str:
    _, rows = query(env.settings, f"""
        SELECT lower(to_hex(md5(to_utf8(
                   array_join(array_agg(event_id || ':' || event_sha256 ORDER BY event_id), ',')))))
        FROM {CATALOG}.{env.settings.namespace('silver')}.events""")
    return str(rows[0][0])


def assert_lake_matches_postgres(env: LakeEnv) -> dict[str, int]:
    counts = {}
    with env.conn() as pg:
        for spec in TABLES:
            key = KEYS[spec.key]
            pg_key = key.replace("cast(business_date as varchar)", "business_date::text")
            pg_n, pg_d = pg.execute(f"SELECT count(*), count(DISTINCT {pg_key}) FROM ({spec.select}) s").fetchone()  # type: ignore[misc]
            _, rows = query(env.settings, f"SELECT count(*), count(DISTINCT {key}) "
                                          f"FROM {CATALOG}.{env.settings.namespace(spec.layer)}.{spec.name}")
            assert rows[0] == (pg_n, pg_d), f"{spec.key}: lake {rows[0]} vs postgres {(pg_n, pg_d)}"
            assert pg_n == pg_d, f"{spec.key}: key is not unique in Postgres"
            counts[spec.key] = pg_n
        assert silver_fingerprint_lake(env) == pg.execute(SILVER_FINGERPRINT_PG).fetchone()[0]  # type: ignore[index]
    return counts


def snapshot_counts(env: LakeEnv) -> dict[str, int]:
    return {spec.key: len(snapshots(env.settings, spec.layer, spec.name)) for spec in TABLES}


def test_incremental_publication_then_dbt_on_trino_equals_lite_gold(lake_env: LakeEnv, seed7_data: Path) -> None:
    env = lake_env
    files = arrival_files(seed7_data / "arrivals")
    half = len(files) // 2
    with env.conn() as conn:
        prepare(conn, seed7_data)
        ingest_and_process(conn, seed7_data, chunk_size=100_000, files=files[:half])
    assert env.publish() == 0
    first = assert_lake_matches_postgres(env)

    with env.conn() as conn:
        ingest_and_process(conn, seed7_data, chunk_size=100_000, files=files[half:])
    assert env.publish() == 0
    second = assert_lake_matches_postgres(env)
    assert second["silver.events"] > first["silver.events"] > 0
    assert second["bronze.raw_events"] == sum(1 for f in files for _ in f.open())  # every arrival line

    # Snapshot history is visible and increments carry increasing high-water marks.
    appends = [s for s in snapshots(env.settings, "silver", "events") if s["hwm"] is not None]
    assert len(appends) == 2 and int(appends[0]["hwm"]) < int(appends[1]["hwm"])
    assert int(appends[1]["added_records"]) == second["silver.events"] - first["silver.events"]

    # Nothing new committed: a third run publishes nothing and creates no snapshot anywhere.
    before = snapshot_counts(env)
    assert env.publish() == 0
    assert snapshot_counts(env) == before

    # Gold on both engines from the same dbt project, compared row by row.
    lite = run_dbt("build", database=env.database)
    assert lite.returncode == 0, lite.stdout[-3000:]
    lake = run_dbt_lake("build", settings=env.settings)
    assert lake.returncode == 0, lake.stdout[-3000:]
    assert "PASS=100" in lake.stdout
    diffs = compare_gold(env.settings, env.database)
    assert len(diffs) == 23
    for d in diffs:
        assert d.identical, (d.table, d.rows_postgres, d.rows_lake, d.only_in_postgres, d.only_in_lake)
    revenue = next(d for d in diffs if d.table == "mart_telecom_revenue_daily")
    assert revenue.totals_postgres["gross_recharge_bdt"] == revenue.totals_lake["gross_recharge_bdt"]

    # Compaction rewrites files, not data.
    fingerprint = silver_fingerprint_lake(env)
    proc = subprocess.run([sys.executable, "-m", "signalflow.cli", "lake", "maintain"], env=env.env,
                          capture_output=True, text=True, timeout=600)
    assert proc.returncode == 0, proc.stdout[-3000:] + proc.stderr[-3000:]
    assert silver_fingerprint_lake(env) == fingerprint
    assert assert_lake_matches_postgres(env) == second
    assert snapshots(env.settings, "silver", "events")[-1]["operation"] == "replace"


def _wait_for_log(path: Path, needle: str, proc: subprocess.Popen[bytes], timeout: float = 300) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if needle in path.read_text(errors="replace"):
            return
        if proc.poll() is not None:
            raise AssertionError(f"publisher exited ({proc.returncode}) before {needle!r}:\n"
                                 + path.read_text()[-3000:])
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {needle!r}")


def test_publisher_killed_mid_write_and_between_tables_publishes_each_row_once(
        lake_env: LakeEnv, seed7_data: Path) -> None:
    env = lake_env
    with env.conn() as conn:
        prepare(conn, seed7_data)
        ingest_and_process(conn, seed7_data, chunk_size=100_000)
        pg_silver = conn.execute("SELECT count(*) FROM silver.events").fetchone()[0]  # type: ignore[index]

    # 1. SIGKILL the driver and its JVM while silver.events is being written (after Bronze committed).
    proc, log_path = env.publish_proc()
    silver_ident = f"{CATALOG}.{env.settings.namespace('silver')}.events"
    _wait_for_log(log_path, f'"table": "{silver_ident}"', proc)   # created empty, write starting
    time.sleep(1.5)
    assert proc.poll() is None, "publisher finished before it could be killed; the test did not exercise a crash"
    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    proc.wait(timeout=60)
    _, rows = query(env.settings, f"SELECT count(*) FROM {silver_ident}")
    assert rows[0][0] == 0, "an interrupted write must not be visible"
    _, rows = query(env.settings, f"SELECT count(*) FROM {CATALOG}.{env.settings.namespace('bronze')}.raw_events")
    assert rows[0][0] > 0, "Bronze committed before the kill"

    # 2. Clean exit right after silver.events committed: later tables are not published yet.
    code = env.publish({"SIGNALFLOW_ALLOW_FAULT_INJECTION": "1", "SIGNALFLOW_FAULT_EXIT_AFTER_TABLE": "silver.events"})
    assert code == 70
    _, rows = query(env.settings, f"SELECT count(*) FROM {silver_ident}")
    assert rows[0][0] == pg_silver

    # 3. A normal run finishes the job; every table holds each source row exactly once.
    assert env.publish() == 0
    assert_lake_matches_postgres(env)
    appends = [s for s in snapshots(env.settings, "silver", "events") if s["hwm"] is not None]
    assert len(appends) == 1, "silver.events must be appended once, not again by the recovery run"


def test_recreated_source_database_is_refused_instead_of_mixed(lake_env: LakeEnv, seed7_data: Path) -> None:
    """A source database dropped and recreated under the same name restarts its identity columns. Appending by
    high-water mark would then skip the new rows below the old mark and mix two datasets in one table, so the
    publisher must refuse until the namespace is dropped or another prefix is used."""
    env = lake_env
    files = arrival_files(seed7_data / "arrivals")
    with env.conn() as conn:
        prepare(conn, seed7_data)
        ingest_and_process(conn, seed7_data, chunk_size=100_000, files=files[: len(files) // 2])
    cmd = [sys.executable, "-m", "signalflow.lakehouse.publish", "--database", env.database, "--table", "silver.events"]
    first = subprocess.run(cmd, env=env.env, capture_output=True, text=True, timeout=600, check=False)
    assert first.returncode == 0, first.stdout[-3000:] + first.stderr[-3000:]
    ident = f"{CATALOG}.{env.settings.namespace('silver')}.events"
    _, before = query(env.settings, f"SELECT count(*) FROM {ident}")

    with psycopg.connect(autocommit=True) as admin_conn:
        admin_conn.execute(f'DROP DATABASE "{env.database}" WITH (FORCE)')
        admin_conn.execute(f'CREATE DATABASE "{env.database}"')
    with env.conn() as conn:
        prepare(conn, seed7_data)
        ingest_and_process(conn, seed7_data, chunk_size=100_000)  # more rows than before: ids pass the old mark
    again = subprocess.run(cmd, env=env.env, capture_output=True, text=True, timeout=600, check=False)
    assert again.returncode != 0
    assert "SourceMismatch" in again.stdout + again.stderr and "earlier incarnation" in again.stdout + again.stderr
    _, after = query(env.settings, f"SELECT count(*) FROM {ident}")
    assert after == before
