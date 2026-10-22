"""SignalFlow X lakehouse refresh (Phase 3).

Thin wrappers around the `signalflow` CLI (ADR-0007): no business logic lives here, so every step can also be run
by hand with the same command. Settings come from the worker's environment (docker-compose.yml, profile `airflow`).

    publish ─┐
             ├─> dbt_build_lake ─┐
             │                   ├─> differential_check
    dbt_build_postgres ──────────┘
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

CLI = os.environ.get("SIGNALFLOW_CLI", "signalflow")
SOURCE_DB = os.environ.get("SIGNALFLOW_SOURCE_DB", "signalflow_stream")

default_args = {
    "owner": "signalflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="signalflow_lakehouse",
    description="Publish committed Silver to Iceberg, build Gold on Trino, check it against Postgres Gold",
    schedule=timedelta(minutes=30),
    start_date=datetime(2026, 10, 1),
    catchup=False,           # every run publishes everything committed so far; there is nothing to backfill
    max_active_runs=1,       # one publisher at a time (append high-water marks assume a single writer)
    default_args=default_args,
    tags=["signalflow", "lakehouse"],
) as dag:
    publish = BashOperator(
        task_id="publish",
        bash_command=f"{CLI} lake publish --database {SOURCE_DB}",
        doc_md="Spark job: new committed Bronze/Silver/ops rows and changed reference tables to Iceberg.",
    )
    dbt_build_postgres = BashOperator(
        task_id="dbt_build_postgres",
        bash_command=f"PGDATABASE={SOURCE_DB} {CLI} lake transform-postgres",
        doc_md="dbt build on the operational Postgres database (the reference side of the differential check).",
    )
    dbt_build_lake = BashOperator(
        task_id="dbt_build_lake",
        bash_command=f"{CLI} lake transform",
        doc_md="dbt build on Trino: Gold as Iceberg tables, every model test run against the lake.",
    )
    differential_check = BashOperator(
        task_id="differential_check",
        bash_command=f"{CLI} lake compare --database {SOURCE_DB}",
        doc_md="Fails the run if any Gold model differs between Postgres and the lake.",
    )
    publish >> dbt_build_lake
    [dbt_build_lake, dbt_build_postgres] >> differential_check

with DAG(
    dag_id="signalflow_lake_maintenance",
    description="Compact small files and expire old snapshots (keeps the last 50 per table)",
    schedule="0 3 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["signalflow", "lakehouse", "maintenance"],
) as maintenance:
    BashOperator(task_id="maintain", bash_command=f"{CLI} lake maintain --retain-last 50")
