# SignalFlow X — Enterprise Data Nervous System

A production-inspired data platform for two **simulated** Bangladeshi enterprises — a mobile operator and an
electronics manufacturer — built to show data-engineering judgement with evidence: versioned contracts,
replay-safe processing, event-time correctness, reconciliation and tested failure recovery.

> All data is synthetic. No real company, customer or phone-number data is used.

![SignalFlow X architecture](docs/assets/architecture.svg)

## Status

| Phase | Scope | State |
|-------|-------|-------|
| 0 | Requirements, contracts, architecture, ADRs, plan, test strategy | ✅ done — [report](docs/reports/PHASE0_REPORT.md) |
| 1 | Generator, Postgres medallion, ingestion, dbt models, validation, E2E test | ✅ done — [report](docs/reports/PHASE1_REPORT.md) |
| 2 | Kafka (KRaft), ERP outbox + Debezium CDC, Spark Structured Streaming Bronze/Silver, kill/restart tests | ✅ done — [report](docs/reports/PHASE2_REPORT.md) |
| 3 | Iceberg on S3-compatible storage, Trino, dbt-trino, lite-vs-lake differential test, Airflow, Superset | ✅ done — [report](docs/reports/PHASE3_REPORT.md) |
| 4 | Revenue Leak Hunter (10 SQL rules, event-time case lifecycle, analyst actions with roles and audit), digital twin pages | ✅ done — [report](docs/reports/PHASE4_REPORT.md) |
| 5 | Chaos Lab (incidents A–E), Data Time Machine (replay from Bronze, promote, undo, Iceberg rollback), table-level lineage and impact, Prometheus + Grafana | ✅ done — [report](docs/reports/PHASE5_REPORT.md) |
| 6 | Benchmark suite (4 workload sizes, baseline vs optimized, latency under load, recovery), compiled contract validation, Benchmark Lab, demo scripts, interview guide | ✅ done — [report](docs/reports/PHASE6_REPORT.md) |

## Documentation

* [Requirements](docs/REQUIREMENTS.md) · [Architecture & risks](docs/ARCHITECTURE.md) ·
  [Data contracts](docs/DATA_CONTRACTS.md) · [ADRs](docs/adr/README.md) ·
  [Implementation plan](docs/IMPLEMENTATION_PLAN.md) · [Test strategy](docs/TEST_STRATEGY.md)
* [Demonstration guide](docs/DEMO.md) (three scripted demos) · [Interview guide](docs/INTERVIEW_GUIDE.md) (design
  choices, trade-offs, likely questions) · [Benchmark results](docs/reports/PHASE6_REPORT.md#headline-results)

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
  assurance/               Revenue Leak Hunter: rule catalogue, hunter (case lifecycle), analyst case actions
  security.py              operators, roles, scrypt password hashes, permission checks, audit log
  erp.py                   simulated ERP: business row + outbox row per transaction
  chaos/, timemachine/     Chaos Lab incidents and lab database; replay from Bronze, promote, undo, Iceberg sync
  lineage/, observability/ table-level lineage and impact; Prometheus exporter and diagnostic alerts
  ui/                      Streamlit control tower (overview, digital twins, Leak Hunter, observatory, Chaos Lab,
                           Time Machine, lake, lineage, Benchmark Lab)
src/signalflow_eval/       offline evaluation against the generator's scenario ledger (never imported by detection)
src/signalflow_bench/      benchmark suite: real pipeline, several workload sizes, every result checked
  pipeline.py, cli.py      orchestration steps and the `signalflow` command
sql/migrations/, sql/erp/  Postgres DDL (bronze / silver / ops / ref / assurance; ERP schema)
sql/assurance/            the ten Leak Hunter rules, one reviewed SQL file each
docker/                    Connect + Debezium, Spark, Airflow and Superset images; Trino catalog; SeaweedFS
airflow/dags/              lakehouse refresh, maintenance and leak-hunter DAGs (thin wrappers around the CLI)
scripts/fetch_artifacts.sh pinned, checksum-verified JVM artifacts
scripts/demo/              the three demonstration scripts (docs/DEMO.md)
benchmarks/results/        recorded benchmark runs (JSON, one file per run)
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
make run                  # bronze -> silver -> dbt gold (100 models and data tests); Leak Hunter after each file
make status               # counts, quarantine reasons, stream watermarks
make evaluate             # Leak Hunter precision/recall against the scenario ledger (offline evaluation package)
make operator OPERATOR=nadia ROLE=analyst  # create a sign-in for case actions (password prompted, scrypt-hashed)
make ui                   # control tower on http://127.0.0.1:8501 (Streamlit; sign in to act on cases)
make test                 # ruff, mypy, unit tests, end-to-end tests on a real database
```

For the Leak Hunter, a longer dataset shows the whole lifecycle (leaks are confirmed only after the late-data horizon):
`make generate DATA=data/seed7-12d DAYS=12 SUBSCRIBERS=1000 && make run DATA=data/seed7-12d`.

What to look at next: [Phase 1 report](docs/reports/PHASE1_REPORT.md) has the measured results and demo queries;
[Phase 4 report](docs/reports/PHASE4_REPORT.md) has the Leak Hunter's evaluation;
[Phase 5 report](docs/reports/PHASE5_REPORT.md) covers the Chaos Lab, Time Machine, lineage and monitoring.

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

## Benchmarks and demos

```bash
make bench WORKLOAD=S     # ~6 min: every experiment on ~142k arrival lines in signalflow_bench* databases
make bench-list           # recorded runs; the control tower's Benchmark Lab shows them in detail
scripts/demo/demo1_live_operations.sh                 # Demo 1: live feed into a fresh database
ANALYST=nadia scripts/demo/demo2_revenue_leak.sh       # Demo 2: telecom leak from detection to decision
OPERATOR=tanvir scripts/demo/demo3_failure_recovery.sh # Demo 3: SIGKILL, late surge, replay and repair
```

Workloads XS, S, M and L run the real pipeline and check every result against the generator's ledger. Measured
results, the machine they come from and the bottleneck analysis are in the
[Phase 6 report](docs/reports/PHASE6_REPORT.md).

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
| No delayed-but-complete transaction is ever confirmed as a leak; every leak whose horizon has passed is confirmed | `test_delayed_but_normal_transactions_are_never_confirmed`, `test_no_false_confirmation_and_every_confirmable_leak_is_confirmed` |
| Case status depends on the data and the event-time clock, not on when the hunter ran; a re-run changes nothing | `test_final_open_cases_do_not_depend_on_hunting_cadence`, `test_hunter_is_idempotent` |
| A failed hunt leaves the previous case state | `test_failed_hunt_rolls_back` |
| Case actions need the analyst role, are versioned, and every attempt (incl. denied) is audited | `test_analyst_actions_need_a_role_are_versioned_and_audited` |
| Detection never reads the scenario ledger | `test_detection_code_never_imports_the_evaluation_package`, Phase 1 isolation test |
| Each Chaos Lab incident (duplicate storm, late surge, schema break, processor SIGKILL, stock discrepancy) recovers to exactly the clean run of the same dataset | `tests/integration/test_phase5_chaos.py` (one test per incident) |
| Incidents only touch a lab database; protected database names are refused | `test_lab_refuses_protected_database_names` |
| A Time Machine replay repairs damaged Silver from Bronze, Gold is rebuilt and reconciles, and undo restores the previous state exactly | `test_inflated_amounts_are_repaired_reconciled_and_undone`, `test_deleted_events_are_reinserted_with_their_original_arrival_class` |
| A replay is never promoted over a stale preview or while a processor runs | `test_promotion_is_refused_when_silver_changed_after_the_preview_or_a_processor_runs` |
| The Iceberg copies follow a promotion and roll back with `rollback_to_snapshot` | `tests/lake/test_phase5_timemachine_lake.py` |
| Lineage impact is profile-aware and never claims column-level lineage | `tests/unit/test_lineage.py` |
| Compiled and reference (jsonschema) validation decide identically; jsonschema decides every rejection | `test_compiled_and_reference_validators_decide_identically`, `test_valid_events_pass_contracts_and_invalid_fail_as_expected[*]` |
| Every benchmark run checks its results (Silver ids, duplicates, quarantine, Gold to the poisha, stock, reconciliation, recovery) and drops its databases | `tests/integration/test_phase6_bench.py`; checks recorded per run in `ops.benchmark_runs` |

Not claimed: end-to-end exactly-once across systems (Kafka → Spark is at-least-once; the Postgres result is
effectively-once, see ADR-0008), production readiness of a single-node, unreplicated Kafka deployment, sub-minute
lake freshness (the lake is refreshed by Airflow every 30 minutes, ADR-0009), or authentication on Trino (local stack,
loopback only). Leak Hunter exposure figures are rule-based estimates of money at risk, not audited losses.
Lineage is table-level, not column-level. A replay cannot recover records Bronze never received. Benchmark figures
describe the machine that recorded them; the streaming and lake paths are tested for correctness, not benchmarked.
The optional AI investigator was not built.
