# ADR-0007: Trino and Airflow only where they earn their cost

**Status:** Accepted (2026-10-21)

## Context
The brief lists Trino ("if justified") and Airflow. Both are memory-heavy (Trino JVM ~1.5–2 GB, Airflow 3
scheduler + API server + DB ~1.5–2 GB).

## Decision
* **Trino**: included in `full`. Justification: Superset and dbt need a SQL engine over Iceberg tables in object
  storage; Spark Thrift Server would work but is a weaker interactive engine. Not used in `lite`, where Postgres
  serves Gold.
* **Airflow**: included in `full` to orchestrate backfills, dbt runs, reconciliation and Time Machine replays as
  DAGs. In `lite` the same steps are CLI commands, which Airflow tasks will call (no logic lives in DAG files).

## Consequences
* Lite profile stays under ~2 GB.
* Two invocation paths for the same steps; mitigated by keeping DAGs thin wrappers around the CLI.
