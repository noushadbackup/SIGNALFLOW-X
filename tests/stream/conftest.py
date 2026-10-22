from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace
from pathlib import Path

import psycopg
import pytest

from signalflow.streaming import admin
from signalflow.streaming.settings import StreamSettings


def require_stream_infra() -> StreamSettings:
    settings = StreamSettings.from_env()
    problems = []
    if not os.environ.get("PGHOST"):
        problems.append("PGHOST not set")
    host, _, port = settings.bootstrap.partition(":")
    try:
        socket.create_connection((host, int(port)), timeout=3).close()
    except OSError:
        problems.append(f"Kafka not reachable at {settings.bootstrap}")
    try:
        urllib.request.urlopen(settings.connect_url, timeout=3).close()  # noqa: S310
    except OSError:
        problems.append(f"Kafka Connect not reachable at {settings.connect_url}")
    if not list(settings.jars_dir.glob("*.jar")):
        problems.append(f"Spark jars missing in {settings.jars_dir} (scripts/fetch_artifacts.sh)")
    if problems:
        if os.environ.get("SIGNALFLOW_REQUIRE_STREAM") == "1":
            pytest.fail("stream tests required but infrastructure is missing: " + "; ".join(problems))
        pytest.skip("stream tests need `docker compose --profile stream up`: " + "; ".join(problems))
    return settings


@dataclass
class StreamEnv:
    settings: StreamSettings
    env: dict[str, str]

    def conn(self) -> psycopg.Connection:
        return psycopg.connect(dbname=self.settings.stream_db, autocommit=True)

    def spark(self, *args: str, extra_env: dict[str, str] | None = None) -> subprocess.Popen[bytes]:
        log = open(self.settings.state_dir / f"spark-{time.time_ns()}.log", "wb")  # noqa: SIM115
        return subprocess.Popen([sys.executable, "-m", "signalflow.streaming.spark_silver", *args],
                                env={**self.env, **(extra_env or {})}, stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True)  # own process group: the driver and its JVM die together

    def run_spark_until_drained(self, extra_env: dict[str, str] | None = None, timeout: float = 600) -> int:
        proc = self.spark("--available-now", extra_env=extra_env)
        try:
            return proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            raise

    def wait_for(self, predicate: Callable[[], bool], timeout: float = 120, what: str = "condition") -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.5)
        raise TimeoutError(f"timed out waiting for {what}")

    def scalar(self, sql: str) -> object:
        with self.conn() as c:
            row = c.execute(sql).fetchone()
            return row[0] if row else None

    def spark_log_tail(self) -> str:
        logs = sorted(self.settings.state_dir.glob("spark-*.log"))
        return logs[-1].read_text(errors="replace")[-4000:] if logs else ""


@pytest.fixture
def stream_env(tmp_path: Path, seed7_data: Path) -> Iterator[StreamEnv]:
    """Isolated streaming deployment per test: own topic prefix, databases, slots, connectors and checkpoints."""
    base = require_stream_infra()
    run = uuid.uuid4().hex[:8]
    settings = replace(base, topic_prefix=f"t{run}", stream_db=f"sfx_stream_{run}", erp_db=f"sfx_erp_{run}",
                       state_dir=tmp_path / "state")
    settings.state_dir.mkdir(parents=True)
    env = {**os.environ, "SIGNALFLOW_TOPIC_PREFIX": settings.topic_prefix, "SIGNALFLOW_STREAM_DB": settings.stream_db,
           "SIGNALFLOW_ERP_DB": settings.erp_db, "SIGNALFLOW_STATE_DIR": str(settings.state_dir),
           "SPARK_LOG_LEVEL": "WARN"}
    admin.init(settings, seed7_data)
    try:
        yield StreamEnv(settings, env)
    finally:
        admin.teardown(settings, drop_databases=True)


def sigkill(proc: subprocess.Popen[bytes]) -> None:
    """Hard kill of the whole Spark driver process tree (Python driver + its JVM), like an OOM kill."""
    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    proc.wait(timeout=60)


FINGERPRINT_SQL = {
    "silver": "SELECT md5(string_agg(event_id::text || ':' || event_sha256, ',' ORDER BY event_id)) FROM silver.events",
    "quarantine": "SELECT md5(string_agg(reason_code || ':' || coalesce(event_id, '-'), ',' "
                  "ORDER BY reason_code, event_id)) FROM silver.quarantine",
    "duplicates": "SELECT count(*)::text FROM ops.duplicate_events",
}


def batch_independent_fingerprints(conn: psycopg.Connection) -> dict[str, str]:
    """What must not depend on how records were cut into micro-batches. (Arrival classes and KPI finality do
    depend on batch boundaries by design, ADR-0004, so they are checked against the lateness oracle instead.)"""
    return {k: str(conn.execute(q).fetchone()[0]) for k, q in FINGERPRINT_SQL.items()}  # type: ignore[index]


@pytest.fixture(scope="session")
def seed7_data(tmp_path_factory: pytest.TempPathFactory) -> Path:
    from signalflow.generator import GeneratorConfig, generate

    out = tmp_path_factory.mktemp("stream") / "seed7"
    generate(GeneratorConfig(seed=7), out)
    return out


@pytest.fixture(scope="session")
def lite_reference(seed7_data: Path) -> dict[str, str]:
    """Phase 1 lite pipeline on the same dataset: the reference the streaming path must match."""
    require_stream_infra()
    from signalflow.pipeline import ingest_and_process, prepare

    name = f"sfx_lite_ref_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(autocommit=True) as admin_conn:
        admin_conn.execute(f'CREATE DATABASE "{name}"')
    try:
        with psycopg.connect(dbname=name, autocommit=True) as conn:
            prepare(conn, seed7_data)
            ingest_and_process(conn, seed7_data, chunk_size=100_000)
            return batch_independent_fingerprints(conn)
    finally:
        with psycopg.connect(autocommit=True) as admin_conn:
            admin_conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
