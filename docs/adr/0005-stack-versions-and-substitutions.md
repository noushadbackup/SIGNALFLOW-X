# ADR-0005: Stack versions, compatibility evidence and substitutions

**Status:** Accepted 2026-10-21; re-verified at the start of each phase. "Verified" below means checked in the
build environment on that date (package installed, image manifest resolved or container started), not merely
believed compatible.

## Decision and evidence

| Component | Chosen version | Evidence (2026-10-21) | Used from |
|-----------|----------------|-----------------------|-----------|
| Python | 3.12 | venv created; `pyspark` 3.5 does not support 3.13, and dbt is most mature on 3.12 | Phase 1 |
| PostgreSQL | 16 (`postgres:16-alpine`) | container started, `PostgreSQL 16.15` | Phase 1 |
| dbt-core / dbt-postgres | 1.11.16 / 1.11.0 | installed, `dbt --version` OK | Phase 1 |
| jsonschema / psycopg | 4.26.0 / 3.2.13 | installed, tests pass | Phase 1 |
| Apache Kafka | 4.1.1 (KRaft, no ZooKeeper) | image manifest resolved via `mirror.gcr.io/apache/kafka` | Phase 2 |
| Debezium Connect | **3.x target; 2.7.3.Final fallback** | 3.x images are only on quay.io, which this environment cannot reach; `debezium/connect:2.7.3.Final` resolved on Docker Hub mirror. Connect 3.7 clients talk to Kafka 4 brokers (Kafka 4 removed only pre-2.1 protocol versions). Spike decides. | Phase 2 |
| Apache Spark / PySpark | **4.0.x primary, 3.5.x fallback** | PyPI lists 4.0.x–4.2.0 and 3.5.x; Maven Central lists `iceberg-spark-runtime-4.0_2.13` and `-3.5_2.12` (1.10.2–1.12.0). Exact pairing verified in the Phase 2 spike (Maven Central was rate-limiting during Phase 0). | Phase 2 |
| Apache Iceberg | 1.10.x+ with **JDBC catalog in Postgres** | runtime artifacts listed on Maven Central | Phase 3 |
| Object storage | **S3-compatible; MinIO if a pinned image is pullable, else SeaweedFS** | `minio/minio` tags did not resolve through the mirror; `chrislusf/seaweedfs` resolved | Phase 3 |
| Trino | 478 | image manifest resolved | Phase 3 |
| Apache Airflow | 3.1.x | image manifest resolved (`apache/airflow:3.1.0`) | Phase 3 |
| Apache Superset | 5.0.0 | image manifest resolved | Phase 3 |
| Prometheus / Grafana | v3.5.0 / 12.2.0 | image manifests resolved | Phase 5 |
| Streamlit | 1.5x+ (PyPI lists 1.66.1) | installable | Phase 6 |

## Modifications to the requested stack, and why

1. **Iceberg catalog = JDBC catalog on the existing Postgres** instead of a separate REST catalog service (Polaris,
   Nessie). Spark and Trino both support it; it removes one service. Upgrade path: REST catalog for multi-engine
   governance.
2. **Object storage is "S3-compatible", not "MinIO" specifically.** MinIO's community distribution changed in 2025
   and the image was not pullable here. The code only uses the S3 API, so the backend is a deployment choice.
3. **Debezium version is decided by the Phase 2 spike**, not assumed.
4. **Kafka in KRaft mode** — ZooKeeper was removed in Kafka 4.0.

## Consequences
Any version change is a PR that updates this table with new evidence.
