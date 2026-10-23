# Architecture Decision Records

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-lite-profile-postgres-reference.md) | Postgres medallion as lite profile and reference implementation | Accepted |
| [0002](0002-envelope-and-json-schema-contracts.md) | One event envelope, JSON Schema contracts shared by all transports | Accepted |
| [0003](0003-outbox-cdc-for-manufacturing.md) | Transactional outbox + Debezium for ERP; direct producers for telecom | Accepted, implemented in Phase 2 |
| [0004](0004-watermark-decides-finality-not-acceptance.md) | Watermark decides KPI finality, not acceptance | Accepted |
| [0005](0005-stack-versions-and-substitutions.md) | Stack versions, compatibility evidence and substitutions | Accepted, re-verified each phase |
| [0006](0006-transactional-micro-batch-checkpoint.md) | Transactional micro-batch checkpoint for lite Silver | Accepted |
| [0007](0007-trino-and-airflow-only-in-full-profile.md) | Trino and Airflow only where they earn their cost | Accepted |
| [0008](0008-streaming-sink-batch-id-commits.md) | Spark streaming sink: one Postgres transaction per micro-batch, keyed by batch id | Accepted (lake part superseded by 0009) |
| [0009](0009-lake-publication-from-committed-postgres-state.md) | The lake is published from committed Postgres state; Gold rebuilt by the same dbt project on Trino | Accepted |
| [0010](0010-leak-hunter-event-time-case-lifecycle.md) | Revenue Leak Hunter: SQL rules, event-time case lifecycle, evaluation kept outside detection | Accepted |
| [0011](0011-chaos-lab-disposable-lab-database.md) | Chaos Lab: predefined incidents in a disposable lab database, verified against a clean run | Accepted |
| [0012](0012-time-machine-replay-from-bronze.md) | Data Time Machine: replay a window from Bronze into an isolated preview, promote atomically, undo exactly | Accepted |
| [0013](0013-derived-table-lineage-and-scrape-time-metrics.md) | Lineage derived from code and artifacts at table level; metrics read from Postgres at scrape time | Accepted |
| [0014](0014-benchmarks-and-compiled-contract-validation.md) | Benchmarks run the real pipeline and check their own results; contracts are also compiled | Accepted |

Format: Context → Decision → Consequences → Alternatives considered.
