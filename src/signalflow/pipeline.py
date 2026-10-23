"""Lite-profile orchestration: the same steps Airflow will call as tasks in the full profile."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import psycopg

from . import CONTRACTS_DIR, REPO_ROOT
from .contracts import ContractRegistry
from .db import load_reference, migrate
from .ingest.bronze_loader import arrival_files, ingest_file
from .log import get_logger
from .processing.silver import BatchStats, process

DBT_DIR = REPO_ROOT / "dbt"
log = get_logger(__name__)


@dataclass
class RunSummary:
    files: int = 0
    lines: int = 0
    bronze_inserted: int = 0
    batches: list[BatchStats] = field(default_factory=list)

    @property
    def accepted(self) -> int:
        return sum(b.accepted for b in self.batches)


def ingest_and_process(conn: psycopg.Connection, data_dir: Path, chunk_size: int = 5000,
                       fail_after_rows: int | None = None, files: list[Path] | None = None,
                       after_file: Callable[[psycopg.Connection], object] | None = None) -> RunSummary:
    """Replay arrival files in order, processing after each file, so every file is one micro-batch window.
    This mirrors a streaming job triggered once per arrival interval. ``after_file`` runs once the file's batch
    is committed (the lite profile runs the Leak Hunter there, as Airflow does on a schedule in the full one)."""
    registry = ContractRegistry.load(CONTRACTS_DIR)
    summary = RunSummary()
    for path in files if files is not None else arrival_files(data_dir / "arrivals"):
        result = ingest_file(conn, path)
        summary.files += 1
        summary.lines += result.lines
        summary.bronze_inserted += result.inserted
        summary.batches += process(conn, registry, chunk_size=chunk_size, fail_after_rows=fail_after_rows)
        if after_file is not None:
            after_file(conn)
    return summary


def prepare(conn: psycopg.Connection, data_dir: Path) -> None:
    migrate(conn)
    load_reference(conn, data_dir / "reference")


def dbt_executable() -> str:
    """The dbt installed with this interpreter (a venv's bin is not always on PATH, e.g. under Streamlit or cron)."""
    sibling = Path(sys.executable).with_name("dbt")
    return str(sibling) if sibling.exists() else shutil.which("dbt") or "dbt"


def run_dbt(command: str = "build", extra: list[str] | None = None,
            database: str | None = None) -> subprocess.CompletedProcess[str]:
    """Run dbt against PGDATABASE, or against ``database`` (e.g. the streaming profile's database)."""
    args = [dbt_executable(), "--no-use-colors", command, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    args += extra or []
    env = dict(os.environ, DBT_SEND_ANONYMOUS_USAGE_STATS="false")
    if database:
        env["PGDATABASE"] = database
    log.info("dbt start", extra={"dbt_args": args[1:]})
    proc = subprocess.run(args, capture_output=True, text=True, env=env, cwd=DBT_DIR)
    log.info("dbt finished", extra={"returncode": proc.returncode})
    return proc
