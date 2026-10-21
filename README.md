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
| 2–6 | Streaming, lakehouse, Revenue Leak Hunter, Chaos Lab / Time Machine, control tower | planned — see [plan](docs/IMPLEMENTATION_PLAN.md) |

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
  ingest/                  bronze loaders (file now, Kafka in Phase 2)
  processing/              silver processor: validate, dedupe, lateness, transactional checkpoint
  pipeline.py, cli.py      orchestration steps and the `signalflow` command
sql/migrations/            Postgres DDL for bronze / silver / ops / ref
dbt/                       staging views, dimensions (incl. SCD2), facts, marts, data tests
tests/unit, tests/integration
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
make test                 # ruff, mypy, 96 unit tests, 14 end-to-end tests on a real database
```

What to look at next: [Phase 1 report](docs/reports/PHASE1_REPORT.md) has the measured results and demo queries.

## Guarantees and where they are tested

| Claim | Test |
|-------|------|
| Every received line is kept verbatim; re-ingest is a no-op | `test_bronze_keeps_every_line_verbatim`, `test_replaying_all_files_changes_nothing` |
| Silver = exactly the contract-valid unique events | `test_silver_contains_exactly_the_valid_unique_events`, `test_quarantine_matches_injected_breaks` |
| Crash mid-batch leaves no partial state; resume equals a clean run | `test_crash_mid_batch_then_resume_equals_clean_run` |
| Lateness follows the documented per-batch watermark policy | `test_arrival_classes_follow_documented_batch_watermark_semantics` |
| Gold money reconciles to the poisha | `test_gold_revenue_equals_truth_to_the_poisha`, dbt `assert_revenue_mart_reconciles_to_silver` |

Not claimed: end-to-end exactly-once across systems, production readiness of a single-node deployment.
