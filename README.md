# SignalFlow X — Enterprise Data Nervous System

A production-inspired data platform for two **simulated** Bangladeshi enterprises — a mobile operator and an
electronics manufacturer — built to show data-engineering judgement with evidence: versioned contracts,
replay-safe processing, event-time correctness, reconciliation and tested failure recovery.

> All data is synthetic. No real company, customer or phone-number data is used.

## Status

| Phase | Scope | State |
|-------|-------|-------|
| 0 | Requirements, contracts, architecture, ADRs, plan, test strategy | ✅ done — [report](docs/reports/PHASE0_REPORT.md) |
| 1 | Generator, Postgres medallion, ingestion, dbt models, validation, E2E test | 🚧 in progress |
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
  generator/               deterministic synthetic enterprise + ground-truth ledger      (Phase 1)
  ingest/                  bronze loaders (file now, Kafka later)                         (Phase 1)
  processing/              silver processor: validate, dedupe, lateness, checkpoint       (Phase 1)
  cli.py                   `signalflow` command                                           (Phase 1)
sql/migrations/            Postgres DDL for bronze / silver / ops                          (Phase 1)
dbt/                       staging, dimensions, facts, marts, data tests                   (Phase 1)
tests/unit, tests/integration
docs/                      requirements, architecture, contracts, ADRs, plan, reports
```

## Quick check (Phase 0)

```bash
python3.12 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest tests/unit -q
```
