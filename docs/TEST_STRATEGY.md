# SignalFlow X — Test Strategy

Principle: every guarantee written in the docs points to a test that demonstrates it. Test results in reports are
pasted from real runs, with date and commit.

## Test layers

| Layer | Tool | Needs infra | What it proves | Location |
|-------|------|-------------|----------------|----------|
| Contract | pytest | no | contracts accept valid samples, reject each class of violation; minor versions compatible | `tests/unit/test_contracts.py` |
| Generator | pytest | no | determinism (same seed ⇒ byte-identical output), no phone-number-like strings, anomaly ledger consistent with emitted events | `tests/unit/test_generator.py` |
| Isolation | pytest | no | pipeline code never references ground truth | `tests/unit/test_isolation.py` |
| Processing logic | pytest | no | lateness classification boundaries, watermark advance rules | `tests/unit/test_lateness.py` |
| Data quality | dbt tests | Postgres | keys unique/not null, relationships, accepted values, reconciliation (mart = silver) | `dbt/models/**/*.yml`, `dbt/tests/` |
| End-to-end acceptance | pytest `-m integration` | Postgres | full pipeline result equals generator truth; idempotent re-ingest; crash + resume = clean run | `tests/integration/` |
| Streaming integration (P2) | pytest + compose | Kafka, Spark | same as E2E through Kafka/Spark, plus kill/restart | planned |
| Differential (P3) | pytest | full | lite and full profiles give identical results for one seed | planned |
| Chaos scenarios (P5) | pytest | full | each incident's expected behaviour, as JSON incident reports | planned |
| Benchmarks (P6) | script | full | measured throughput / latency / recovery; not pass/fail | planned |

## Oracle: the ground-truth ledger

The generator knows what it injected. It writes `truth/manifest.json` (expected unique valid event ids, expected
quarantine count, revenue per business date and channel in poisha, expected stock per warehouse/SKU) and
`truth/anomalies.jsonl`. Tests compare pipeline outputs with these. Pipeline code cannot read them (isolation test).

## Determinism

Seeds fix all randomness, IDs are UUIDv5 / hashes of seed-scoped counters, and timestamps derive from the
configured start date. Wall-clock time appears only in `ingested_at` / `processed_at` columns, which tests never
compare.

## Failure testing

The Silver processor accepts a `fail_after_rows` fault-injection parameter (test-only; refused unless
`SIGNALFLOW_ALLOW_FAULT_INJECTION=1`). The E2E test aborts mid-batch, asserts that the checkpoint did not move and
no partial batch is visible, resumes, and compares Gold checksums with a clean run.

## CI

GitHub Actions: lint (ruff), type check (mypy), unit tests, then E2E against a `postgres:16` service container.
Integration tests skip (with an explicit reason) only when no `PGHOST` is configured; CI always configures it.
