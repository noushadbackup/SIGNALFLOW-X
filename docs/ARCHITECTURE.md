# SignalFlow X — Architecture

Status: Phases 1–3 implemented and tested (`lite`, `stream` and `lake` profiles; see `docs/reports/`). Sections marked *(planned)* are designs, not
working code yet.

## 1. Context

```mermaid
flowchart LR
  subgraph Sources["Simulated enterprise sources (synthetic)"]
    GEN["Deterministic generator<br/>seed-controlled"]
    TEL["Telecom systems<br/>OCS · billing · CRM · mediation · payment GW"]
    ERP["Manufacturing ERP / WMS / MES<br/>(Postgres + outbox)"]
    GEN --> TEL & ERP
  end
  TEL -- "events (file today, Kafka in Phase 2)" --> BR
  ERP -- "outbox rows → Debezium CDC (Phase 2)" --> BR
  subgraph Platform["SignalFlow X data platform"]
    BR["Bronze<br/>verbatim records"] --> SV["Silver<br/>contract-valid, deduped, event-time classified"]
    BR --> Q["Quarantine"]
    SV --> GD["Gold<br/>dimensional model + marts"]
    GD --> RA["Revenue Leak Hunter (Phase 4)"]
  end
  GD --> UI["Control tower (Streamlit) · Superset"]
  RA --> UI
  GEN -. "ground-truth ledger<br/>(tests only)" .-> T[("truth/")]
```

## 2. Two deployment profiles, one design

The same contracts, layer semantics and tests apply to both profiles. The lite profile is not a toy: it is the
reference implementation used to check the distributed one (differential testing, Phase 3).

| Concern | `lite` (Phase 1, runs today) | `stream` (Phase 2, runs today) | `lake` / `full` (Phase 3 runs today; Phase 5 adds monitoring) |
|---------|------------------------------|--------------------------------|------------------------------|
| Transport | JSONL arrival files, ingested in arrival order | Kafka 4.1.1 (KRaft); telecom producers; ERP outbox → Debezium 3.6.3 | same as `stream` |
| Bronze | `bronze.raw_events` in Postgres | same table, written by Spark (`source_stream = kafka:<topic>`) | + Iceberg copy `lake.bronze.raw_events` on SeaweedFS (S3 API), published incrementally (ADR-0009) |
| Silver | Python micro-batch processor, transactional checkpoint | Spark 4.0.3 Structured Streaming, validation on executors, same Silver rules, batch-id commit marker (ADR-0008) | decided in Postgres as before; Spark job publishes committed rows to Iceberg (append by high-water mark) |
| Gold | dbt on Postgres | dbt on Postgres | the same dbt project on Trino 478 over Iceberg, compared row by row with Postgres Gold |
| Orchestration | `signalflow` CLI / Makefile | CLI / Makefile | Airflow 3.1 DAGs calling the CLI |
| BI | Streamlit control tower | Streamlit | Superset 5.0 (assets as code) + Streamlit Lakehouse page |
| Observability | `ops.*` tables + structured logs | + slot lag, replays | + `ops.lake_publish_runs`, `ops.lake_differential_checks`; Prometheus/Grafana in Phase 5 |

```mermaid
flowchart TB
  subgraph lite["lite profile (implemented)"]
    F["arrivals/*.jsonl"] -->|"bronze_loader<br/>unique(source_stream, source_position)"| B1[("bronze.raw_events")]
    B1 -->|"silver processor<br/>1 txn per micro-batch"| S1[("silver.events")]
    B1 --> Q1[("silver.quarantine")]
    S1 --> STG["dbt staging views<br/>(typed)"] --> G1[("gold.dim_* / fct_* / mart_*")]
    OPS[("ops.checkpoints · stream_state<br/>duplicate_events · late_events · kpi_restatements")]
    S1 -.-> OPS
  end
```

### Stream profile (Phase 2)

```mermaid
flowchart LR
  A["arrival files<br/>(source simulator)"] -->|"telecom events<br/>idempotent producer, key = event_id"| K[("Kafka 4.1.1 KRaft<br/>1 topic per type + major version<br/>+ unrouted per domain")]
  A -->|"mfg events"| ERP[("ERP database<br/>erp.documents + erp.outbox<br/>one transaction")]
  ERP -->|"WAL · pgoutput"| DZ["Debezium 3.6.3<br/>Outbox Event Router"]
  ERP -->|"table CDC<br/>dealers · products · warehouses"| DZM["Debezium<br/>master-data connector"]
  DZ --> K
  DZM -->|"<prefix>.erpcdc.*"| K
  K -->|"subscribePattern"| SP["Spark Structured Streaming<br/>mapInPandas validation"]
  SP -->|"foreachBatch: 1 Postgres txn per batch<br/>+ ops.stream_commits(batch_id)"| PG[("bronze / silver / ops<br/>stream database")]
  SP --> MD[("bronze.master_data_changes")]
  PG --> DBT["same dbt project"] --> G[("gold")]
```

Secrets: the Debezium password reaches the Connect worker as an environment variable and is referenced in the
connector config as `${env:CDC_PASSWORD}` (Kafka's `EnvVarConfigProvider`), so it never travels through the REST
API or into the `_connect_configs` topic. Debezium logs in as a dedicated `LOGIN REPLICATION` role with read-only
grants; publications are created by `signalflow stream init` as the table owner.

## 3. Layer contracts

### Bronze — raw and auditable
* One row per received record, including duplicates and unparseable lines. Original line text is stored verbatim
  (`raw_record`) with its SHA-256.
* Identity: `(source_stream, source_position)`; for files `arrivals:<file>:<line>`, for Kafka
  `<topic>:<partition>:<offset>`. Re-delivery of the same position is a no-op (idempotent load).
* Columns extracted best-effort for indexing (`event_id`, `event_type`, `event_time`), never trusted downstream.
* Replay: Silver can be rebuilt from Bronze by resetting the checkpoint (Phase 5 Time Machine builds on this).
* Sensitive data: the generator produces no personal data (no names, no phone numbers), so Bronze needs no masking.
  In a real deployment Bronze would be the most restricted layer.

### Silver — trusted events
* `silver.events`: one row per unique `event_id`, payload as `jsonb`, plus `stream`, `event_time`,
  `arrival_class`, `lateness_seconds`, `contract_id`, `bronze_id` (lineage back to the exact raw record).
* Typed per-entity views (`silver.stg_*`) are built by dbt with explicit casts.
* Validation, quarantine, dedupe and lateness rules: see [DATA_CONTRACTS.md](DATA_CONTRACTS.md).

### Gold — business-ready
See §5 for the model. Gold is fully rebuilt by dbt in the lite profile (cheap at this size, and it makes late-data
restatement correct by construction). Incremental models are a Phase 3 optimisation with a measured baseline.

## 4. Processing semantics

```mermaid
sequenceDiagram
  participant L as bronze_loader
  participant B as bronze.raw_events
  participant P as silver processor
  participant S as silver.events / quarantine / ops
  L->>B: INSERT ... ON CONFLICT (source_stream, source_position) DO NOTHING
  P->>S: BEGIN
  P->>B: SELECT rows WHERE bronze_id > checkpoint ORDER BY bronze_id LIMIT n
  P->>P: validate contract · classify lateness vs watermark at batch start
  P->>S: INSERT valid events ON CONFLICT (event_id) DO NOTHING
  P->>S: INSERT quarantine / duplicate / late rows
  P->>S: UPDATE stream_state (max_event_time, watermark), checkpoint = last bronze_id
  P->>S: COMMIT  (all-or-nothing)
```

Guarantee stated precisely: within one Postgres database, each Bronze row affects Silver exactly once, because
writes and checkpoint commit atomically. Crash → rollback → the next run re-reads the same rows. This is tested
with fault injection. It is not end-to-end exactly-once across systems, and no such claim is made.

## 5. Gold dimensional model (Phase 1 scope)

```mermaid
erDiagram
  dim_date ||--o{ fct_recharge : business_date
  dim_subscriber_scd2 ||--o{ fct_recharge : "as-of event_time"
  dim_region ||--o{ fct_recharge : region
  fct_recharge ||--o| fct_billing_charge : recharge_id
  dim_package ||--o{ fct_package_purchase : package_code
  dim_product ||--o{ fct_inventory_movement : sku
  dim_warehouse ||--o{ fct_inventory_movement : warehouse_id
  dim_dealer ||--o{ fct_dealer_order : dealer_id
  fct_dealer_order ||--o{ fct_shipment : order_id
  fct_shipment ||--o| fct_invoice : shipment_id
```

| Model | Grain | Primary key | Notes |
|-------|-------|-------------|-------|
| `dim_date` | calendar day (Asia/Dhaka) | `date_day` | Fri/Sat weekend |
| `dim_subscriber_scd2` | subscriber × validity interval | `subscriber_sk` | SCD2 on segment, home_region, status; justified because revenue must be attributed to the segment/region *at the time* |
| `dim_region`, `dim_package`, `dim_product`, `dim_warehouse`, `dim_dealer` | one row per member | natural key | from reference data |
| `fct_recharge` | one recharge attempt | `recharge_id` | business date, channel, status, amount, as-of subscriber key |
| `fct_billing_charge` | one balance credit | `charge_id` | |
| `fct_package_purchase` | one activation | `activation_id` | with capture status |
| `fct_return` | one return | `return_id` | refunds paid against it |
| `fct_usage` | one rated usage record | `usage_id` | |
| `fct_channel_settlement` | channel × business date | `settlement_id` | |
| `fct_inventory_movement` | one movement | `movement_id` | |
| `fct_dealer_order` | one order line | `order_id` | current status derived from status/shipment events |
| `fct_shipment` | one shipment | `shipment_id` | dispatch and delivery timestamps |
| `fct_invoice` | one invoice | `invoice_id` | settled amount, settlement status |
| `mart_telecom_revenue_daily` | business date × region × channel | composite | KPI finality column |
| `mart_telecom_subscribers_daily` | business date × region × segment | composite | active base from SCD2 |
| `mart_inventory_position` | warehouse × SKU | composite | on hand, 7-day outbound, days of cover (derived) |
| `mart_mfg_sales_daily` | business date × dealer region × category | composite | |
| `mart_mfg_production_daily` | business date × factory × category | composite | includes disruption days |
| `mart_telecom_usage_daily` | business date × serving region × usage type | composite | |

## 6. Full-profile design *(runs today except Prometheus/Grafana, Phase 5)*

```mermaid
flowchart LR
  OCS["telecom producers"] -->|"tel.* topics"| K[(Kafka KRaft)]
  ERPDB[("ERP Postgres<br/>business tables + outbox")] -->|"Debezium outbox router"| K
  K --> SS["Spark Structured Streaming<br/>bronze + silver (ADR-0008)"]
  SS --> PG[("Postgres<br/>bronze · silver · ops")]
  PG -->|"Spark publisher<br/>append by high-water mark (ADR-0009)"| OBJ[("Iceberg on SeaweedFS (S3 API)<br/>JDBC catalog in Postgres")]
  OBJ --> TR["Trino 478"] --> DBT["dbt-trino gold"] --> OBJ
  TR --> SUP["Superset"]
  TR --> ST["Streamlit control tower"]
  AF["Airflow"] -.schedules.-> DBT
  AF -.schedules.-> PG
  PR["Prometheus"] --> GF["Grafana"]
  K & SS & TR -.metrics.-> PR
```

Key decisions are recorded as ADRs in [`docs/adr/`](adr).

## 7. Risk assessment

| # | Risk | Likelihood | Impact | Mitigation |
|---|------|-----------|--------|-----------|
| R1 | Full profile exceeds WSL2 memory on a 16 GB laptop | High | High | profiles; per-service memory limits; Airflow/Superset only in `full`; measured RAM in reports |
| R2 | MinIO community images are no longer published the way they used to be; the image could not be pulled in the build environment | Medium | Medium | treat storage as "S3-compatible"; pin a known MinIO release where pullable, SeaweedFS as tested fallback. **Resolved in Phase 3:** SeaweedFS 3.97 in use; needed `AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED` (ADR-0009) |
| R3 | Debezium 3.x images are on quay.io, which this build environment cannot reach | Medium | Medium | **resolved in Phase 2:** Connect image built from the official Kafka 4.1.1 image + the Debezium 3.6.3 plugin from Maven Central (checksummed) |
| R4 | Spark ↔ Iceberg ↔ Kafka connector version drift | Medium | High | Phase 2 spike pinned Spark 4.0.3 + Kafka connector 4.0.3 / kafka-clients 3.9.1; **resolved in Phase 3:** Iceberg 1.10.1 runtime for Spark 4.0; Trino 478 reads the tables Spark writes and vice versa (tests) |
| R5 | Two Silver implementations (Python lite, Spark full) drift apart | Medium | High | **one** implementation of the Silver rules (`apply_validated`) used by both paths; stream tests require identical Silver/quarantine fingerprints vs the lite run on the same seed |
| R6 | Synthetic data too regular to be credible | Medium | Medium | Dhaka hourly demand curve, regional weights, segment behaviour, Fri/Sat effects, supply disruptions |
| R7 | Ground truth leaks into detection logic | Low | High | truth written outside pipeline paths; source-scan test; detection rules reviewed against ledger fields |
| R8 | Docker Hub rate limits during setup | Medium | Low | compose file uses `mirror.gcr.io` defaults overridable via env |
| R10 | Debezium replication slot pins WAL (idle ERP, or a slot left behind) | Medium | High (disk fills) | heartbeat action query advances the slot; teardown drops slots and resets connector offsets; slot lag shown by `stream status` (alerting in Phase 5) |
| R11 | Lake mirrors two databases at once (identity values overlap) | Low | High | publisher refuses a table whose snapshots record another source database; tests use separate namespace prefixes |
| R9 | Overclaiming (exactly-once, "production ready") | Medium | High (credibility) | every guarantee in docs links to the test that demonstrates it |
