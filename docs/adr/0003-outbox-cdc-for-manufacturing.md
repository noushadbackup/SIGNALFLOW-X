# ADR-0003: Transactional outbox + Debezium for ERP; direct producers for telecom

**Status:** Accepted (implementation in Phase 2)

## Context
The brief asks for Debezium CDC. Raw table-level CDC from an ERP produces row-change events (`before/after`), which
would need a second contract family and per-table reconstruction of business meaning. Telecom charging/mediation
systems, in contrast, are naturally event producers.

## Decision
* The simulated ERP (Postgres) writes business rows **and** an `outbox` row in the same transaction. Debezium's
  Outbox Event Router publishes outbox rows to Kafka as envelope-shaped domain events.
* Master-data tables (dealers, products, warehouses) are captured with plain table CDC to feed SCD dimensions.
* Telecom events are produced directly to Kafka topics, one per event type and major version.

## Consequences
* Same contracts for both domains; CDC still demonstrably used (outbox + master data).
* The outbox avoids the dual-write problem (DB commit succeeds but Kafka publish fails).
* Requires `wal_level=logical` and a replication slot; slot lag must be monitored (Phase 5 observability).

## Alternatives considered
* **Table CDC only**: shows Debezium but makes Silver logic table-specific and harder to test.
* **App publishes to Kafka after commit**: the classic dual-write bug; useful later as a Chaos Lab teaching example.
