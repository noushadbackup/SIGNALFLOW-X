from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import urllib.request
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, replace
from pathlib import Path

import psycopg
import pytest

from signalflow.lakehouse import admin
from signalflow.lakehouse.settings import LakeSettings


def require_lake_infra() -> LakeSettings:
    settings = LakeSettings.from_env()
    problems = []
    for var in ("PGHOST", "SIGNALFLOW_S3_ACCESS_KEY", "SIGNALFLOW_S3_SECRET_KEY"):
        if not os.environ.get(var):
            problems.append(f"{var} not set")
    try:
        socket.create_connection((settings.trino_host, settings.trino_port), timeout=3).close()
    except OSError:
        problems.append(f"Trino not reachable at {settings.trino_host}:{settings.trino_port}")
    try:
        urllib.request.urlopen(settings.s3_endpoint, timeout=3).close()  # noqa: S310
    except urllib.error.HTTPError:
        pass  # anonymous request refused: the S3 endpoint is up
    except OSError:
        problems.append(f"S3 endpoint not reachable at {settings.s3_endpoint}")
    if not list(settings.jars_dir.glob("*.jar")):
        problems.append(f"lake jars missing in {settings.jars_dir} (scripts/fetch_artifacts.sh)")
    if problems:
        if os.environ.get("SIGNALFLOW_REQUIRE_LAKE") == "1":
            pytest.fail("lake tests required but infrastructure is missing: " + "; ".join(problems))
        pytest.skip("lake tests need `docker compose --profile lake up`: " + "; ".join(problems))
    return settings


@dataclass
class LakeEnv:
    settings: LakeSettings
    database: str
    log_dir: Path

    @property
    def env(self) -> dict[str, str]:
        return {**os.environ, "PGDATABASE": self.database, "SIGNALFLOW_LAKE_PREFIX": self.settings.namespace_prefix,
                "SPARK_LOG_LEVEL": "WARN"}

    def conn(self) -> psycopg.Connection:
        return psycopg.connect(dbname=self.database, autocommit=True)

    def publish_proc(self, extra_env: dict[str, str] | None = None) -> tuple[subprocess.Popen[bytes], Path]:
        log_path = self.log_dir / f"publish-{time.time_ns()}.log"
        log = open(log_path, "wb")  # noqa: SIM115
        proc = subprocess.Popen([sys.executable, "-m", "signalflow.lakehouse.publish", "--database", self.database],
                                env={**self.env, **(extra_env or {})}, stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True)  # own process group: the driver and its JVM die together
        return proc, log_path

    def publish(self, extra_env: dict[str, str] | None = None, timeout: float = 600) -> int:
        proc, log_path = self.publish_proc(extra_env)
        code = proc.wait(timeout=timeout)
        if code not in (0, 70):
            raise AssertionError(log_path.read_text()[-4000:])
        return code


@pytest.fixture(scope="session")
def seed7_data(tmp_path_factory: pytest.TempPathFactory) -> Path:
    from signalflow.generator import GeneratorConfig, generate

    out = tmp_path_factory.mktemp("lake") / "seed7"
    generate(GeneratorConfig(seed=7), out)
    return out


@pytest.fixture
def lake_env(tmp_path: Path) -> Iterator[LakeEnv]:
    base = require_lake_infra()
    tag = uuid.uuid4().hex[:8]
    settings = replace(base, namespace_prefix=f"t{tag}_")
    database = f"sfx_lake_{tag}"
    with psycopg.connect(autocommit=True) as a:
        a.execute(f'CREATE DATABASE "{database}"')
    env = LakeEnv(settings, database, tmp_path)
    try:
        yield env
    finally:
        admin.drop_namespaces(settings)
        with psycopg.connect(autocommit=True) as a:
            a.execute(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
