"""Benchmark workloads and the benchmark databases they run in."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import psycopg
from psycopg import sql

from signalflow import REPO_ROOT
from signalflow.chaos.lab import PROTECTED_ENV, PROTECTED_NAMES
from signalflow.generator import GeneratorConfig


@dataclass(frozen=True)
class Workload:
    """A generator configuration. Event volume grows with subscribers x days (usage records dominate)."""
    key: str
    days: int
    subscribers: int
    dealers: int
    seed: int = 11

    @property
    def dataset_key(self) -> str:
        return f"seed{self.seed}-{self.days}d-{self.subscribers}s-{self.dealers}d"

    def config(self) -> GeneratorConfig:
        return GeneratorConfig(seed=self.seed, days=self.days, subscribers=self.subscribers, dealers=self.dealers)

    def directory(self) -> Path:
        root = Path(os.environ.get("SIGNALFLOW_BENCH_DATA", REPO_ROOT / "data" / "bench"))
        return root / self.dataset_key


# Arrival lines per workload are recorded by every run (generation.lines); XS produces 13,037.
WORKLOADS = {
    "XS": Workload("XS", days=3, subscribers=1_000, dealers=100),     # the integration test and CI
    "S": Workload("S", days=7, subscribers=5_000, dealers=150),
    "M": Workload("M", days=14, subscribers=10_000, dealers=300),
    "L": Workload("L", days=14, subscribers=18_000, dealers=400),
}


def bench_database_name(name: str | None = None) -> str:
    """The benchmark database. Its name must contain 'bench' and must not be any database the platform uses."""
    name = name or os.environ.get("SIGNALFLOW_BENCH_DB", "signalflow_bench")
    protected = PROTECTED_NAMES | {os.environ[e] for e in (*PROTECTED_ENV, "SIGNALFLOW_LAB_DB") if os.environ.get(e)}
    if name in protected or "bench" not in name or not name.replace("_", "").isalnum():
        raise ValueError(f"refusing to use {name!r} as a benchmark database: it must contain 'bench' and must not "
                         "be a demo, streaming, ERP, catalog, lab, Airflow or Superset database")
    return name


def recreate(name: str, template: str | None = None) -> None:
    """Drop and create a benchmark database, optionally as a file copy of another benchmark database."""
    bench_database_name(name)
    with psycopg.connect(dbname="postgres", autocommit=True) as admin:
        admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
        if template is None:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        else:
            admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE {} STRATEGY FILE_COPY").format(
                sql.Identifier(name), sql.Identifier(bench_database_name(template))))


def drop(name: str) -> None:
    bench_database_name(name)
    with psycopg.connect(dbname="postgres", autocommit=True) as admin:
        admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))


def connect(name: str) -> psycopg.Connection:
    return psycopg.connect(dbname=bench_database_name(name), autocommit=True, application_name="signalflow-bench")
