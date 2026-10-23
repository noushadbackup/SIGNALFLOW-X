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
| Streaming integration (P2) | pytest `-m stream` + compose `stream` profile | Kafka, Connect/Debezium, Postgres, Java 17+ | Silver/quarantine identical to the lite run; Gold = truth; SIGKILL mid-stream, exit between commits, failure inside a batch; duplicate storm; master-data CDC | `tests/stream/` |
| Streaming units (P2) | pytest | no | topic routing, wrapper parsing, connector configs carry no secrets, arrival-time precedence | `tests/unit/test_streaming_units.py` |
| Lake + differential (P3) | pytest `-m lake` + compose `lake` profile | Postgres, SeaweedFS, Trino, Java 17+ | incremental publication equals Postgres per table; every Gold model identical on Postgres and Trino; SIGKILL mid-write and exit between tables; compaction keeps data | `tests/lake/` |
| Lakehouse units (P3) | pytest | no | table specs select their high-water mark, no Postgres-only types; differential normalisation | `tests/unit/test_lakehouse_units.py` |
| Leak Hunter acceptance (P4) | pytest `-m integration` | Postgres | 11-day dataset hunted after every file: no false confirmation, every leak past its horizon confirmed, every delayed billing seen as suspicious then cleared, idempotent re-run, cadence-independent final state, rollback of a failed hunt, role checks and audit of analyst actions | `tests/integration/test_phase4_assurance.py` |
| Leak Hunter units (P4) | pytest | no | rule files return the shared columns; status boundaries; horizons exceed every generator delay; case ids; scrypt hashing; role order; detection never imports `signalflow_eval`; evaluation counting | `tests/unit/test_assurance_units.py` |
| Chaos Lab (P5) | pytest `-m integration` | Postgres | incidents A–E in a rebuilt lab database: each incident's expected behaviour holds and the recovered lab equals a clean run of the same dataset (Silver fingerprint, Gold revenue, stock); a viewer is refused and audited; protected database names are refused | `tests/integration/test_phase5_chaos.py` |
| Time Machine (P5) | pytest `-m integration` | Postgres | inflated amounts repaired from Bronze, Gold rebuilt and reconciled, undo restores the damaged state exactly; deleted events reinserted with their original arrival class; stale previews and a running processor block promotion; role and window guards; defects refused outside a lab | `tests/integration/test_phase5_timemachine.py` |
| Time Machine lake (P5) | pytest `-m lake` | Postgres, SeaweedFS, Trino | a promoted replay synced to Iceberg for exactly the touched keys, verified row by row; undo rolls the table back with `rollback_to_snapshot`; time travel shows the damaged state (the re-sync fallback is not yet tested) | `tests/lake/test_phase5_timemachine_lake.py` |
| Lineage and metrics units (P5) | pytest | no | event types, staging models and KPIs connected as derived; recharge reaches the revenue dashboard through Kafka, Spark, dbt and the lake in stream but not through Kafka in lite; job-failure impact depends on profile; KPI to dashboard links require model and column; alert subjects map to nodes or to nothing; column-level lineage is not claimed; exporter text format and failure reporting | `tests/unit/test_lineage.py`, `tests/unit/test_observability_units.py` |
| Benchmark suite (P6) | pytest `-m integration` | Postgres | the XS workload runs every experiment; every recorded check passes (Silver ids, duplicates, quarantine, Gold to the poisha, stock, reconciliation, Leak Hunter evaluation, recovery, replay); results file equals the database record; benchmark databases and data are removed. Speed is measured, never asserted | `tests/integration/test_phase6_bench.py` |
| Benchmark and validator units (P6) | pytest | no | nearest-rank weighted percentiles; per-row latency attribution across files and batches; benchmark database name guard; compiled and jsonschema validation decide identically on 1,930 variants and on generated data; contracts use only keywords both drafts agree on | `tests/unit/test_bench_units.py`, `tests/unit/test_contracts.py` |
| Benchmark runs (P6) | `make bench WORKLOAD=XS|S|M|L` | Postgres | measured throughput, latency percentiles, backlog, recovery, query times; a run with a failed check is recorded as FAILED | `benchmarks/results/` |

## Oracle: the ground-truth ledger

The generator knows what it injected. It writes `truth/manifest.json` (expected unique valid event ids, expected
quarantine count, revenue per business date and channel in poisha, expected stock per warehouse/SKU) and
`truth/anomalies.jsonl`. Tests compare pipeline outputs with these. Pipeline code cannot read them (isolation test).

The Leak Hunter is evaluated the same way, by a separate package (`src/signalflow_eval`) that matches cases to the
ledger and records precision and recall per rule in `assurance.evaluation_runs`. Detection code never imports it
(unit test). The ledger also lists *benign* scenarios (billing delayed by up to 20 h, dealers paying up to 3 days
late); the evaluation fails if any of them was ever confirmed.

## Determinism

Seeds fix all randomness, IDs are UUIDv5 / hashes of seed-scoped counters, and timestamps derive from the
configured start date. Wall-clock time appears only in `ingested_at` / `processed_at` columns, which tests never
compare.

## Failure testing

The Silver processor accepts a `fail_after_rows` fault-injection parameter (test-only; refused unless
`SIGNALFLOW_ALLOW_FAULT_INJECTION=1`). The E2E test aborts mid-batch, asserts that the checkpoint did not move and
no partial batch is visible, resumes, and compares Gold checksums with a clean run.

Streaming failure tests (Phase 2) use real process deaths, not only exceptions: the Spark driver and its JVM are
started in their own process group and killed with SIGKILL while records are still arriving. Two env-guarded hooks
(`SIGNALFLOW_FAULT_EXIT_AFTER_BATCH`, `SIGNALFLOW_FAULT_FAIL_IN_BATCH`, both refused without
`SIGNALFLOW_ALLOW_FAULT_INJECTION=1`) hit the two narrow windows deterministically: exit right after the Postgres
commit (Spark must replay the batch and the sink must skip it), and fail inside the transaction (nothing written).

The Chaos Lab (Phase 5) turns these into operator-run incidents: each one rebuilds a lab database, injects, observes,
recovers and is verified against a clean run of the same dataset (ADR-0011). Incident D kills the Silver processor,
running as its own OS process, with SIGKILL while it drains a backlog.

## CI

GitHub Actions: lint (ruff), type check (mypy), unit tests, then E2E against a `postgres:16` service container.
Integration tests skip (with an explicit reason) only when no `PGHOST` is configured; CI always configures it.
A second job (`stream`) starts the compose `stream` profile, sets up Java 17, runs `tests/stream` with
`SIGNALFLOW_REQUIRE_STREAM=1` (missing infrastructure fails instead of skipping) and builds the Spark job image.
A third job (`lake`) starts the compose `lake` profile, runs `tests/lake` with `SIGNALFLOW_REQUIRE_LAKE=1`, builds
the Airflow image and runs `airflow dags test signalflow_lakehouse` inside it against a lite-pipeline database,
then builds the Superset image.
