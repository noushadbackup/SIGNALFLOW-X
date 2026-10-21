# SignalFlow X — Data Contracts

The machine-readable contracts are the JSON Schema files under [`contracts/`](../contracts). This document explains
them; if the two disagree, the files win and this document has a bug. Contract behaviour is tested in
[`tests/unit/test_contracts.py`](../tests/unit/test_contracts.py).

## 1. Envelope (`contracts/envelope.json`, v1.0)

Every event, in every domain and on every transport (file, Kafka, CDC outbox), uses the same envelope.

| Field | Type | Rule | Why |
|-------|------|------|-----|
| `event_id` | UUID string | lowercase canonical UUID | Technical identity; the dedupe key in Silver |
| `event_type` | string | `^(telecom\|mfg)\.[a-z_]+$` | Routes to a payload contract and a stream |
| `schema_version` | `major.minor` | must be registered for the type | Contract resolution |
| `source_system` | enum | `tel-ocs`, `tel-billing`, `tel-crm`, `tel-mediation`, `tel-payment-gw`, `tel-provisioning`, `mfg-erp`, `mfg-mes`, `mfg-wms` | Lineage and blast-radius analysis |
| `event_time` | RFC 3339 UTC (`Z`) | when the business fact happened | Event-time processing, business date |
| `produced_at` | RFC 3339 UTC (`Z`) | when the producer emitted it; `event_time` may not exceed it by > 5 min | Detects corrupt clocks |
| `payload` | object | validated by the payload contract | Business content |

Transport metadata is **not** part of the envelope. The file transport wraps each event as
`{"arrival_time": ..., "event": {...}}`; Kafka will use the record timestamp, topic, partition and offset.
Bronze stores these as `arrival_time`, `source_stream`, `source_position`.

## 2. Payload contracts (v1.0 unless stated)

All payloads use `additionalProperties: false`, so an unexpected field is a contract violation rather than silently
dropped data. Money fields are BDT numbers with at most 2 decimal places (`x-decimal-places: 2`, enforced in code
because JSON Schema `multipleOf` is unreliable with binary floats).

### Telecom

| Event type | Business key | Producer | Key rules |
|------------|--------------|----------|-----------|
| `telecom.subscriber_lifecycle` | `event_id` (state snapshot) | `tel-crm` | action ∈ ACTIVATED/REGION_CHANGED/SEGMENT_CHANGED/TERMINATED; full state (segment, home_region, plan_type) on every event so SCD2 needs no stitching |
| `telecom.recharge` v1.0, **v1.1** | `recharge_id` | `tel-ocs` | amount > 0; `status=SUCCESS ⇒ failure_reason=null`, `FAILED ⇒ failure_reason` required; v1.1 adds optional `promo_code` |
| `telecom.billing_charge` | `charge_id` | `tel-billing` | references `recharge_id`; amount > 0 |
| `telecom.payment_capture` | `capture_id` | `tel-payment-gw` | status ∈ CAPTURED/FAILED; joins activation via `payment_txn_ref` |
| `telecom.package_activation` | `activation_id` | `tel-provisioning` | status ∈ ACTIVATED/REJECTED |
| `telecom.usage_record` | `usage_id` | `tel-mediation` | unit must match usage_type (DATA→MB, VOICE→SECOND, SMS→MESSAGE) |
| `telecom.channel_settlement` | `settlement_id` | `tel-payment-gw` | one per channel per business date; count ≥ 0 |

### Manufacturing

| Event type | Business key | Producer | Key rules |
|------------|--------------|----------|-----------|
| `mfg.production_batch` | `batch_id` | `mfg-mes` | quantity ≥ 1 |
| `mfg.inventory_movement` | `movement_id` | `mfg-wms` | signed, non-zero delta; `source_ref` names the authorizing document (batch, shipment, transfer, return, stock-count approval, opening balance) |
| `mfg.stock_count_approval` | `approval_id` | `mfg-wms` | authorizes exactly one ADJUSTMENT |
| `mfg.dealer_order` | `order_id` | `mfg-erp` | one SKU per order line |
| `mfg.order_status` | `event_id` | `mfg-erp` | BACKORDERED / CANCELLED with reason |
| `mfg.shipment` | `event_id` (`shipment_id` + status) | `mfg-wms` | DISPATCHED then DELIVERED |
| `mfg.invoice` | `invoice_id` | `mfg-erp` | references shipment and order; due date |
| `mfg.dealer_settlement` | `settlement_id` | `mfg-erp` | applied to one invoice |
| `mfg.product_return` | `return_id` | `mfg-erp` | refund amount stated by the return |
| `mfg.refund` | `refund_id` | `mfg-erp` | references `return_id` |
| `mfg.supply_disruption` | `disruption_id` | `mfg-mes` | reason and severity enums |

Identifiers are synthetic and carry a type prefix (`SUB-`, `RCH-`, `DLR-`, `WH-DHK-01`, ...). No phone number
(MSISDN) is ever generated; subscribers are known only by `SUB-nnnnnnn`.

## 3. Versioning and evolution policy

* **Minor version** (`1.0 → 1.1`): only optional properties may be added. Required set, existing property
  definitions, conditional rules and `additionalProperties` must be unchanged. Enforced by
  `compatibility_problems()` and `test_all_minor_versions_are_backward_compatible`.
* **Major version** (`1.x → 2.0`): anything else. A new major is a new contract file *and*, from Phase 2, a new Kafka
  topic and Silver projection. Until a consumer registers it, events with that version are quarantined as
  `UNSUPPORTED_SCHEMA_VERSION`. That is the designed behaviour for Chaos Lab Incident C.
* Contracts are immutable once released: fix a mistake with a new version, never by editing a released file.

## 4. Validation outcome and quarantine policy

Validation runs in order; the first failing step decides the reason code.

| Order | Reason code | Meaning | Policy |
|-------|-------------|---------|--------|
| 0 | `UNPARSEABLE` | line is not JSON / has no `event` object | quarantine, raw text kept in Bronze |
| 1 | `ENVELOPE_VIOLATION` | envelope schema failed | quarantine |
| 2 | `INVALID_TIMESTAMP` | timestamps unparseable or event_time > produced_at + 5 min | quarantine |
| 3 | `UNKNOWN_EVENT_TYPE` | no contract for type | quarantine |
| 4 | `UNSUPPORTED_SCHEMA_VERSION` | type known, version not registered | quarantine |
| 5 | `CONTRACT_VIOLATION` | payload schema or business rule failed | quarantine |
| 5b | `INVALID_TIMESTAMP` | event_time more than 5 min after the record's arrival time (would poison the stream clock) | quarantine |
| 6 | `EVENT_ID_CONFLICT` | same `event_id` already in Silver with a different payload hash | quarantine; the first accepted version stays |
| — | duplicate | same `event_id`, same payload hash | not quarantined; logged in `ops.duplicate_events` |

Quarantine is per record, never per stream: one bad producer cannot stop other event types. Quarantined records
remain in Bronze, so they can be replayed after a contract is added or the producer is fixed.

## 5. Lateness policy (per stream)

Streams: `telecom` and `mfg` (one watermark each). Watermark = max accepted `event_time` in the stream minus
**allowed lateness = 6 hours**. Watermarks advance only at the end of a micro-batch, the same semantics Spark
Structured Streaming uses, so results do not depend on row order inside a batch.

| Class | Condition (measured against stream max event time at batch start) | Policy |
|-------|----------------------------------------|--------|
| `ON_TIME` | delay ≤ 5 min | accept |
| `LATE_WITHIN_WATERMARK` | 5 min < delay ≤ 6 h | accept; KPI for that day is still provisional |
| `LATE_BEYOND_WATERMARK` | delay > 6 h | **accept** (financial facts are never dropped), log to `ops.late_events`, mark the business date `RESTATED` |

Why not drop beyond-watermark events like a streaming aggregation would: a recharge that arrives 10 hours late is
still money. Dropping it would create exactly the kind of revenue discrepancy the platform is meant to catch. The
watermark therefore decides **KPI finality**, not acceptance.

## 6. Ground truth

The generator writes injected anomalies and expected totals to `truth/` next to (not inside) the arrival files.
Pipeline code never reads it; tests do. `tests/unit/test_isolation.py` fails if any module under
`src/signalflow/` other than the generator references the truth directory.
