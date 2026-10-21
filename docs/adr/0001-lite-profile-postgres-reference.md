# ADR-0001: Postgres medallion as lite profile and reference implementation

**Status:** Accepted (2026-10-21)

## Context
The target stack (Kafka, Spark, Iceberg, Trino, Airflow, Superset) needs ~10–12 GB RAM. A reviewer cloning the repo
should be able to see the data logic working in minutes, and the distributed path needs an independent oracle to
prove it is correct.

## Decision
Implement Bronze/Silver/Gold first on a single PostgreSQL 16 database (`bronze`, `silver`, `gold`, `ops` schemas),
with Python for ingestion/validation and dbt for modelling. This becomes the `lite` profile. The `full` profile
reuses the same contracts and dbt marts on Iceberg/Trino.

## Consequences
* Reviewers can run the whole foundation with one container.
* Silver logic exists twice (Python and, later, Spark). Mitigated by shared JSON Schema contracts and a differential
  test that runs both on the same seed and compares event-id sets and financial totals.
* Postgres will not show distributed-processing skills; that is Phase 2–3's job, measured against this baseline.

## Alternatives considered
* **Spark-only from day one**: slower feedback, no independent oracle, high setup cost before any business logic.
* **DuckDB for lite**: excellent for analytics, but Postgres is also the operational database and Debezium source,
  so it avoids a third engine.
