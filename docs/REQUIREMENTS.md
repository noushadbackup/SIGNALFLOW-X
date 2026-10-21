# SignalFlow X — Requirements

Status: Phase 0 baseline (2026-10-21). Owner: Noushad.

SignalFlow X is a **simulated** enterprise data platform for two fictional businesses that share one data
infrastructure:

* **Domain A — "Telecom"**: a fictional Bangladeshi mobile operator (prepaid-heavy subscriber base, recharges,
  bundles, usage records, billing, payment-channel settlements).
* **Domain B — "Manufacturing"**: a fictional Bangladeshi electronics manufacturer with factories, warehouses and a
  dealer network (production, inventory, dealer orders, shipments, invoices, settlements, returns).

No real company data, customer data or phone numbers are used. All business names, identifiers and amounts are
synthetic. Company names in this document describe the *kind* of employer the project targets, not a data source.

---

## 1. Business requirements

### 1.1 Telecom (Domain A)

| ID | Requirement | Business question it answers |
|----|-------------|------------------------------|
| T-1 | Track subscriber lifecycle (activation, region change, segment change, termination) with history | How many subscribers were active per region on a given day? Which segment did a subscriber belong to *when* they recharged? |
| T-2 | Record every recharge attempt with channel, amount (BDT), status and failure reason | What is daily gross recharge revenue by region and channel? What is the payment success rate? |
| T-3 | Record billing (balance credit) for every successful recharge | Did every successful recharge get credited? |
| T-4 | Record bundle/package purchases and their payment capture | Which bundles sell? Were any activated without captured payment? |
| T-5 | Record usage (data MB, voice seconds, SMS) with serving region | What is data traffic per region and hour? |
| T-6 | Ingest daily payment-channel settlement files | Does settled money equal expected revenue per channel and business day? |
| T-7 | Revenue assurance (Phase 4) | Where is revenue potentially leaking, how much is at risk, and is it real or just delayed? |

### 1.2 Manufacturing (Domain B)

| ID | Requirement | Business question it answers |
|----|-------------|------------------------------|
| M-1 | Record production batches per factory and SKU | What did each factory produce? Was output disrupted? |
| M-2 | Record every inventory movement with an authorizing source reference | What is on hand per warehouse and SKU? Is every adjustment authorized? |
| M-3 | Record dealer orders, backorders and cancellations | What do dealers demand by region and category? Where are we out of stock? |
| M-4 | Record shipments (dispatch and delivery) | Which orders are delayed in transit? |
| M-5 | Record invoices and dealer settlements | Which invoices are unpaid past their window? Do settlements match invoices? |
| M-6 | Record returns and refunds | Was any return refunded twice? |
| M-7 | Record supply disruptions | Which factories were down and what was the impact on stock cover? |

### 1.3 Reporting conventions

* Currency: **BDT**, stored as `numeric(14,2)`; generator works in integer poisha to avoid float drift.
* Storage timezone: **UTC** (`timestamptz`). Reporting timezone: **Asia/Dhaka** (UTC+6, no DST).
* Business date = event time converted to Asia/Dhaka, truncated to date.
* Weekend: **Friday and Saturday** (Bangladesh convention) in `dim_date`.

---

## 2. Platform (non-functional) requirements

### 2.1 Data architecture

| ID | Requirement | Acceptance test |
|----|-------------|-----------------|
| P-1 | **Bronze** stores every received record verbatim (byte-for-byte line text), with source stream, source position, arrival time and ingestion time | E2E: bronze row count = number of source lines, incl. duplicates and unparseable lines |
| P-2 | Bronze ingestion is idempotent per source position | E2E: re-ingesting all files adds 0 rows |
| P-3 | **Silver** accepts only records that pass the versioned data contract | E2E: every injected schema break is quarantined; no quarantined record reaches silver |
| P-4 | Silver removes technical duplicates (same `event_id`, same payload) and logs them | E2E: silver event-id set equals the generator's set of unique valid events |
| P-5 | Same `event_id` with a *different* payload is quarantined as `EVENT_ID_CONFLICT`, never silently overwritten | Unit + E2E |
| P-6 | Event-time processing with a per-stream watermark; late events are classified, not dropped | E2E: injected beyond-watermark events are classified `LATE_BEYOND_WATERMARK` and still present in silver |
| P-7 | Silver processing is restartable: a crash mid-batch leaves no partial state; re-run converges to the same result | E2E: fault-injected run + resume equals clean run (checksums) |
| P-8 | **Gold** dimensional model with documented grain and keys, SCD2 where history matters | dbt tests: unique / not_null / relationships |
| P-9 | Gold financial totals reconcile to silver | dbt singular tests + E2E against generator truth |
| P-10 | KPI finality is explicit: `PROVISIONAL` until the watermark passes the business day, `FINAL` after, `RESTATED` if beyond-watermark data later changed it | E2E |

### 2.2 Delivery semantics (stated precisely)

* Source → Bronze: **at-least-once** delivery, made **effectively-once per source position** by a unique key on
  `(source_stream, source_position)`.
* Bronze → Silver (lite profile, Postgres): rows, dedupe registry, watermark state and checkpoint are committed in
  **one database transaction**, so a micro-batch is applied entirely or not at all. This is an exactly-once
  *effect within one Postgres database*; it is **not** a claim of end-to-end exactly-once across systems.
* Kafka/Spark path (Phase 2+): at-least-once from Kafka with checkpointed offsets, plus idempotent sinks keyed by
  `event_id`. Claims will be limited to what the failure tests demonstrate.

### 2.3 Engineering standards

* Configuration only through environment variables (libpq `PG*` variables, `SIGNALFLOW_*`); no secrets in git.
* Services bind to `127.0.0.1` by default.
* Structured JSON logs; Python type hints; modular packages; unit + integration tests; container health checks.
* Ground-truth anomaly ledger is written to a separate directory and **never read by pipeline code** (enforced by
  a test that scans the source tree).
* No real phone numbers: subscribers are identified by `SUB-nnnnnnn` keys only (a test scans generated output for
  Bangladeshi MSISDN patterns).

### 2.4 Resource envelope

Target machine: Windows + WSL2, 16 GB RAM laptop, 4–8 cores. WSL2 should be given 10–12 GB.

| Profile | Services | Expected RAM |
|---------|----------|-------------|
| `lite` | Postgres + Python pipeline + dbt (+ Streamlit later) | ~1–2 GB |
| `stream` (Phase 2) | lite + Kafka (KRaft) + Kafka Connect/Debezium + Spark local | ~5–6 GB |
| `full` (Phase 3+) | stream + object storage + Trino + Airflow + Superset + Prometheus/Grafana | ~10–12 GB |

The RAM figures are planning estimates, not measurements; each phase report will replace them with measured values.

---

## 3. "Production ready" — what it means here

The owner asked for a production-ready system. This project will be built to production **standards** (contracts,
idempotency, restart safety, tests, observability, least-privilege config, documented runbooks). It will still be
a single-machine, synthetic-data deployment, so the following are explicitly **out of scope** and are listed in
every phase report as the remaining gap to a real deployment: HA/replication, secrets manager, TLS/mTLS between
services, SSO/RBAC integrated with a corporate IdP, capacity testing on real hardware, backup/DR drills, on-call
alert routing, data-retention and privacy compliance review.

## 4. Out of scope (by decision)

Kubernetes, paid cloud services, ML for problems solvable with deterministic rules, LLM agents in the core path,
real customer data.
