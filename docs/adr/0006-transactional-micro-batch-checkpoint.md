# ADR-0006: Transactional micro-batch checkpoint for lite Silver

**Status:** Accepted (2026-10-21)

## Context
Silver must survive crashes without duplicating or losing events, and the claim must be testable.

## Decision
The lite Silver processor reads Bronze rows after a stored checkpoint (`bronze_id`), and in **one transaction**
writes Silver rows, quarantine rows, duplicate/late logs, stream watermark state and the new checkpoint. A
row lock on the checkpoint row prevents two processors from running concurrently.

## Consequences
* Crash at any point ⇒ rollback ⇒ the next run reprocesses the same rows. Verified by a fault-injection test that
  aborts mid-batch and compares the resumed result with a clean run.
* Silver writes are also idempotent (`ON CONFLICT (event_id) DO NOTHING`), so even a checkpoint reset (replay)
  cannot double-insert.
* Throughput is bounded by single-writer Postgres; measured in the Phase 6 benchmark, compared against Spark.

## Alternatives considered
* **Checkpoint after commit in a separate step**: opens an at-least-once window that then relies solely on
  idempotent writes; acceptable, but the atomic version is simpler to reason about.
