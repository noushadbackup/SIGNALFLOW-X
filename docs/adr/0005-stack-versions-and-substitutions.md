# ADR-0005: Stack versions, compatibility evidence and substitutions

**Status:** Accepted 2026-10-21; re-verified at the start of each phase (Phase 2 spike and Phase 3: 2026-10-22). "Verified" below means checked in the
build environment on that date (package installed, image manifest resolved or container started), not merely
believed compatible.

## Decision and evidence

| Component | Chosen version | Evidence (2026-10-21) | Used from |
|-----------|----------------|-----------------------|-----------|
| Python | 3.12 | venv created; `pyspark` 3.5 does not support 3.13, and dbt is most mature on 3.12 | Phase 1 |
| PostgreSQL | 16 (`postgres:16-alpine`) | container started, `PostgreSQL 16.15` | Phase 1 |
| dbt-core / dbt-postgres | 1.11.16 / 1.11.0 | installed, `dbt --version` OK | Phase 1 |
| jsonschema / psycopg | 4.26.0 / 3.2.13 | installed, tests pass | Phase 1 |
| Apache Kafka | 4.1.1 (KRaft, no ZooKeeper) | **2026-10-22: broker running in compose, all stream tests pass against it** | Phase 2 |
| Debezium | **3.6.3.Final PostgreSQL connector on Kafka Connect 4.1.1** | 2026-10-22: quay.io still unreachable, so the image is built from the official `apache/kafka:4.1.1` image plus the connector plugin tarball from Maven Central (SHA-1 verified by `scripts/fetch_artifacts.sh`). Connect loads `PostgresConnector 3.6.3.Final`; outbox router and table CDC tested. The 2.7.3 fallback was not needed. | Phase 2 |
| Apache Spark / PySpark | **4.0.3** (Scala 2.13), Kafka connector `spark-sql-kafka-0-10_2.13:4.0.3` + `kafka-clients 3.9.1` | 2026-10-22: PySpark 4.0.3 runs on Java 17 (CI) and 21 (dev box); reads Kafka 4.1.1. 4.0.x chosen over 4.1.x because `iceberg-spark-runtime-4.0_2.13` has releases 1.10.0–1.12.0 while the 4.1 runtime only has 1.11.0+ (Phase 3 picks the Iceberg version). | Phase 2 |
| Apache Iceberg | **1.10.1** (`iceberg-spark-runtime-4.0_2.13`, `iceberg-aws-bundle`), **JDBC catalog in Postgres** (`jdbc.schema-version=V1`), Postgres JDBC 42.7.8 | 2026-10-22: Spark writes, Trino 478 reads and writes the same tables; `tests/lake` pass | Phase 3 |
| Object storage | **SeaweedFS 3.97** S3 gateway (MinIO not pullable) | 2026-10-22: in use; AWS SDK v2 default `aws-chunked` checksums corrupt objects on SeaweedFS, so clients run with checksums `WHEN_REQUIRED` (ADR-0009) | Phase 3 |
| Trino | 478 (Iceberg connector, JDBC catalog, native S3) | 2026-10-22: running; dbt-trino 1.10.6 builds 100/100 | Phase 3 |
| Apache Airflow | 3.1.0 (LocalExecutor, metadata in Postgres) | 2026-10-22: `airflow dags test` green on the host; DAG runs green in compose | Phase 3 |
| Apache Superset | 5.0.0 + `trino[sqlalchemy]` 0.340.0 | 2026-10-22: running in compose, dashboard created by `signalflow bi bootstrap` | Phase 3 |
| Prometheus / Grafana | v3.5.0 / 12.2.0 | image manifests resolved | Phase 5 |
| Kafka Python client | confluent-kafka 2.12.0 | idempotent producer used by the telecom simulator | Phase 2 |
| Streamlit / Plotly | 1.50.0 / 6.3.0 | control tower preview running (screenshots in Phase 1 follow-up) | Phase 6 (preview earlier) |

## Modifications to the requested stack, and why

1. **Iceberg catalog = JDBC catalog on the existing Postgres** instead of a separate REST catalog service (Polaris,
   Nessie). Spark and Trino both support it; it removes one service. Upgrade path: REST catalog for multi-engine
   governance.
2. **Object storage is "S3-compatible", not "MinIO" specifically.** MinIO's community distribution changed in 2025
   and the image was not pullable here. The code only uses the S3 API, so the backend is a deployment choice.
3. **Debezium runs on a self-built Connect image** (official Kafka image + Debezium plugin from Maven Central),
   because Debezium's own 3.x images are only on quay.io. Same connector, Connect runtime matches the broker.
4. **Kafka in KRaft mode** — ZooKeeper was removed in Kafka 4.0.
5. **JREs are copied from the official Temurin image** into the Spark and Airflow images instead of `apt-get`
   installing them, so image builds depend only on the Python package index.

## Consequences
Any version change is a PR that updates this table with new evidence.
