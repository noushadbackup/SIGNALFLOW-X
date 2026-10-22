"""SignalFlow X Revenue Leak Hunter schedule (Phase 4).

One task, a thin wrapper around the `signalflow` CLI (ADR-0007, ADR-0010). The hunter runs inside one database
transaction under an advisory lock, so an overlapping manual run waits instead of racing, and a failed run leaves
the previous case state untouched. Case ages are measured on the stream's event-time clock, so the schedule only
decides how fresh the cases are, never which status they get.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

CLI = os.environ.get("SIGNALFLOW_CLI", "signalflow")
SOURCE_DB = os.environ.get("SIGNALFLOW_SOURCE_DB", "signalflow_stream")

with DAG(
    dag_id="signalflow_leak_hunter",
    description="Run the ten reconciliation rules against committed Silver and move cases through their lifecycle",
    schedule=timedelta(minutes=10),
    start_date=datetime(2026, 10, 1),
    catchup=False,           # each run evaluates everything committed so far; missed runs need no replay
    max_active_runs=1,
    default_args={
        "owner": "signalflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["signalflow", "assurance"],
) as dag:
    BashOperator(
        task_id="hunt",
        bash_command=f"{CLI} assurance hunt --database {SOURCE_DB}",
        doc_md="Opens, escalates and auto-clears cases; writes one row to assurance.hunter_runs per run.",
    )
