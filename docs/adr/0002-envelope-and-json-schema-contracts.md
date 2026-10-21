# ADR-0002: One event envelope, JSON Schema contracts shared by all transports

**Status:** Accepted (2026-10-21)

## Context
Events reach the platform by file (lite), Kafka producers (telecom) and CDC outbox (ERP). Validation must behave
identically regardless of transport and engine.

## Decision
* A single envelope (`event_id`, `event_type`, `schema_version`, `source_system`, `event_time`, `produced_at`,
  `payload`), transport metadata kept outside it.
* Payload contracts in JSON Schema 2020-12, one immutable file per `major.minor`, `additionalProperties: false`.
* Business rules expressible in JSON Schema (`if/then`, enums, ranges) live in the contract so Spark can reuse them;
  the only code-side rule is "max 2 decimal places" because float `multipleOf` is unreliable.
* Minor versions must be mutually compatible (checked by a test); a major version is a new contract and, in the
  streaming path, a new topic.

## Consequences
* One validation library, testable without any infrastructure.
* JSON Schema is verbose and slower than Avro/Protobuf binary validation. Accepted at this scale; a Schema Registry
  with Avro is the documented production upgrade.

## Alternatives considered
* **Avro + Confluent Schema Registry**: industry standard for Kafka, but adds a service and a licence question
  (Confluent Community License) for one developer; JSON keeps Bronze human-readable for the Time Machine demo.
* **Pydantic models**: good ergonomics but Python-only; Spark could not reuse them.
