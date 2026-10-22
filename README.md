# SignalFlow X — Enterprise Data Nervous System

A production-inspired data platform for two **simulated** Bangladeshi enterprises — a mobile operator and an
electronics manufacturer — built to show data-engineering judgement with evidence: versioned contracts,
replay-safe processing, event-time correctness, reconciliation and tested failure recovery.

> All data is synthetic. No real company, customer or phone-number data is used.

## Status

| Phase | Scope | State |
|-------|-------|-------|
| 0 | Requirements, contracts, architecture, ADRs, plan, test strategy | ✅ done — [report](docs/reports/PHASE0_REPORT.md) |
| 1 | Generator, Postgres medallion, ingestion, dbt models, validation, E2E test | ✅ done — [report](docs/reports/PHASE1_REPORT.md) |
| 2 | Kafka (KRaft), ERP outbox + Debezium CDC, Spark Structured Streaming Bronze/Silver, kill/restart tests | ✅ done — [report](docs/reports/PHASE2_REPORT.md) |
| 3 | Iceberg on S3-compatible storage, Trino, dbt-trino, lite-vs-lake differential test, Airflow, Superset | ✅ done — [report](docs/reports/PHASE3_REPORT.md) |
| 4–6 | Revenue Leak Hunter, Chaos Lab / Time Machine, control tower | planned — see [plan](docs/IMPLEMENTATION_PLAN.md) |

## Documentation

* [Requirements](docs/REQUIREMENTS.md) · [Architecture & risks](docs/ARCHITECTURE.md) ·
  [Data contracts](docs/DATA_CONTRACTS.md) · [ADRs](docs/adr/README.md) ·
  [Implementation plan](docs/IMPLEMENTATION_PLAN.md) · [Test strategy](docs/TEST_STRATEGY.md)

## Repository layout

```
contracts/                 JSON Schema data contracts (envelope + one file per event type/version)
src/signalflow/
  contracts.py             contract registry, validation policy, compatibility checker
  generator/               deterministic synthetic enterprise + ground-truth ledger
  ingest/                  file → bronze loader
  processing/              Silver rules (shared by both paths) + lite micro-batch processor
  streaming/               topics, telecom producer, Debezium connectors, Spark job, Postgres sink, lifecycle
  lakehouse/               Spark publisher to Iceberg, Trino helpers, Gold differential check, maintenance
  bi/                      Superset assets as code (connection, datasets, charts, dashboard)
  erp.py                   simulated ERP: business row + outbox row per transaction
  ui/                      Streamlit control tower (preview)
  pipeline.py, cli.py      orchestration steps and the `signalflow` command
sql/migrations/, sql/erp/  Postgres DDL (bronze / silver / ops / ref; ERP schema)
docker/                    Connect + Debezium, Spark, Airflow and Superset images; Trino catalog; SeaweedFS
airflow/dags/              lakehouse refresh and maintenance DAGs (thin wrappers around the CLI)
scripts/fetch_artifacts.sh pinned, checksum-verified JVM artifacts
dbt/                       staging views, dimensions (incl. SCD2), facts, marts, data tests
tests/unit, tests/integration, tests/stream, tests/lake
docs/                      requirements, architecture, contracts, ADRs, plan, reports
```

## Quickstart (lite profile, ~2 minutes)

Needs Docker, Python 3.12 and GNU make (on Windows use WSL2).

```bash
cp .env.example .env      # pick a local password
make setup                # venv + dependencies
make up                   # PostgreSQL 16, bound to 127.0.0.1
make generate             # deterministic dataset (seed 7): ~25k events, 3 days, both domains
make run                  # bronze -> silver -> dbt gold (100 models and data tests)
make status               # counts, quarantine reasons, stream watermarks
make ui                   # control tower preview on http://127.0.0.1:8501 (Streamlit, read-only)
make test                 # ruff, mypy, 105 unit tests, 15 end-to-end tests on a real database
```

What to look at next: [Phase 1 report](docs/reports/PHASE1_REPORT.md) has the measured results and demo queries.

## Quickstart (stream profile: Kafka + Debezium + Spark)

Needs the lite setup above plus Java 17+ on the host (Spark runs in local mode) and ~3 GB free RAM.

```bash
make stream-up            # fetch pinned jars + Debezium plugin (SHA-1 checked), start Postgres, Kafka, Connect
make stream-init          # stream DB, simulated ERP, 23 topics, outbox + master-data connectors
make stream-silver        # terminal 1: Spark Structured Streaming job (Ctrl-C and restart: resumes from checkpoint)
make stream-replay        # terminal 2: telecom → Kafka, manufacturing → ERP outbox → Debezium → Kafka
make stream-status        # connectors, slot lag, bronze/silver counts, committed batches, skipped replays
make stream-transform     # the same dbt project on the streaming database
make test-stream          # 5 acceptance tests incl. SIGKILL of the Spark job mid-stream (~6 min)
```

## Quickstart (lake profile: Iceberg + Trino, then Airflow and Superset)

Needs the lite setup (with `make run` done) plus Java 17+ on the host and ~4 GB free RAM; Airflow and Superset add
about 3 GB more.

```bash
make lake-up              # fetch Iceberg jars (SHA-1 checked), start SeaweedFS (S3 API) + bucket + Trino 478
make lake-publish         # committed Postgres state -> 15 Iceberg tables (incremental; re-running is a no-op)
make lake-transform       # the same dbt project on Trino: Gold as Iceberg tables, 100 models and tests
make lake-compare         # every Gold model, Postgres vs lake, row by row (23/23 identical on seed 7)
make lake-status          # rows, snapshots, last commit per table
make test-lake            # incremental publish, SIGKILL mid-write, crash between tables, compaction (~6 min)
make airflow-up           # Airflow 3.1 on 127.0.0.1:58081 (user admin; `make airflow-password`)
make superset-up && make superset-bootstrap   # Superset 5.0 on 127.0.0.1:58088, dashboard created from code
```

## Guarantees and where they are tested

| Claim | Test |
|-------|------|
| Every received line is kept verbatim; re-ingest is a no-op | `test_bronze_keeps_every_line_verbatim`, `test_replaying_all_files_changes_nothing` |
| Silver = exactly the contract-valid unique events | `test_silver_contains_exactly_the_valid_unique_events`, `test_quarantine_matches_injected_breaks` |
| Crash mid-batch leaves no partial state; resume equals a clean run | `test_crash_mid_batch_then_resume_equals_clean_run` |
| Lateness follows the documented per-batch watermark policy | `test_arrival_classes_follow_documented_batch_watermark_semantics` |
| Gold money reconciles to the poisha | `test_gold_revenue_equals_truth_to_the_poisha`, dbt `assert_revenue_mart_reconciles_to_silver` |
| Kafka/Debezium/Spark path gives the same Silver as the lite reference, and Gold = truth, after a SIGKILL mid-stream | `test_spark_sigkill_mid_stream_then_restart_equals_lite_reference` |
| A batch re-executed after our commit is a no-op | `test_exit_between_our_commit_and_sparks_commit_is_replayed_as_a_noop` |
| A failure inside a streaming batch leaves nothing behind | `test_failure_inside_a_batch_rolls_back_and_the_retry_completes` |
| A duplicate storm changes no Silver or Gold figure; every copy is accounted for | `test_duplicate_storm_leaves_silver_and_gold_unchanged` |
| Master-data changes are captured by CDC with before/after images | `test_master_data_change_is_captured_by_cdc` |
| Incremental lake publication equals Postgres table by table; a run with nothing new adds no snapshot | `test_incremental_publication_then_dbt_on_trino_equals_lite_gold` |
| Gold built by dbt on Trino/Iceberg is identical, row by row, to Gold on Postgres (all 23 models) | same test, and `signalflow lake compare` in the Airflow DAG |
| A publisher killed mid-write or between tables leaves no partial table and no duplicate on recovery | `test_publisher_killed_mid_write_and_between_tables_publishes_each_row_once` |
| Compaction rewrites files, not data | `test_incremental_publication_then_dbt_on_trino_equals_lite_gold` (fingerprint before/after) |

Not claimed: end-to-end exactly-once across systems (Kafka → Spark is at-least-once; the Postgres result is
effectively-once, see ADR-0008), production readiness of a single-node, unreplicated Kafka deployment, sub-minute
lake freshness (the lake is refreshed by Airflow every 30 minutes, ADR-0009), or authentication on Trino (local stack,
loopback only).
