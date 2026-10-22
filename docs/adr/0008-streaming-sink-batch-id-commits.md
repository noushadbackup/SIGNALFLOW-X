# ADR-0008: Spark streaming sink — one Postgres transaction per micro-batch, keyed by batch id

**Status:** Accepted 2026-10-22 (Phase 2). The planned Iceberg `MERGE` sink was not built; see ADR-0009.

## Context
Phase 2 adds a streaming path: telecom producers and the ERP outbox (via Debezium) write to Kafka, and Spark
Structured Streaming must produce the same Bronze and Silver as the lite reference (ADR-0001), with the same rules
(dedupe by event_id, first arrival wins, quarantine with reason codes, event-time lateness, ADR-0004).

Spark's Kafka source is replayable and Spark's checkpoint guarantees that a re-executed micro-batch has the same
batch id and the same offsets. Spark does not make an external sink exactly-once: after a crash between the sink's
write and Spark's commit log entry, the batch runs again.

## Decision
* `foreachBatch` sink. Validation (JSON Schema, the CPU-heavy step) runs on the executors in `mapInPandas`; the
  validated batch is then applied on the driver by the **same** `apply_validated` function the lite processor uses.
* Each micro-batch is **one Postgres transaction** that writes Bronze rows (`source_stream = kafka:<topic>`,
  `source_position = <partition>:<offset>`), Silver, quarantine, duplicate/late/restatement evidence, stream clocks,
  `ops.processing_runs` and a marker row `ops.stream_commits(query_name, batch_id)`.
* If the marker exists, the batch is a replay: nothing is written except an `ops.stream_replays` row (evidence).
* A second line of defence: Bronze's unique `(source_stream, source_position)`. If the Spark checkpoint is lost and
  Kafka is re-read from the start, already-stored positions are skipped and never reach Silver twice.
* Messages are keyed by `event_id` (telecom producer key; outbox `aggregateid`), so every copy of an event lands on
  one partition in arrival order, which keeps "first arrival wins" identical to the file path.
* No `maxOffsetsPerTrigger`: a catch-up after downtime is one batch. Bounding batches per partition would let a
  low-volume partition run ahead and move the global stream clock, mislabelling on-time events as late. Batches
  are therefore unbounded in size; this is acceptable at portfolio volumes and is listed as a limitation.
* A single writer per query is enforced with `pg_try_advisory_xact_lock` (`ProcessorBusy`).
* The simulated arrival time travels in a Kafka header (`sf_arrival_time`), never as the record timestamp:
  weeks-old timestamps make the broker's time-based retention delete the data within minutes (observed in testing).

## Consequences
* Result in Postgres is effectively-once per Kafka record, demonstrated by tests that SIGKILL the job mid-stream,
  force an exit between our commit and Spark's, and fail inside a batch (`tests/stream/`). Kafka → Spark itself is
  at-least-once; no end-to-end exactly-once claim is made.
* The driver collects each batch. Throughput is bounded by the single Postgres writer anyway; Phase 3 replaces this
  sink with Iceberg `MERGE` written by the executors (not done: ADR-0009 publishes committed Postgres state instead).
* Arrival classes and KPI finality depend on micro-batch boundaries (by design, ADR-0004), so they can differ from
  the lite run while Silver content, quarantine and Gold totals are identical. Tests check lateness against the
  generator's injected delays instead of against the lite classification.

## Alternatives considered
* **Spark-native dedupe (`dropDuplicatesWithinWatermark`) + append sink**: bounded state but cannot detect an
  event-id conflict against events older than the watermark, and still needs an idempotent sink.
* **Kafka transactions / exactly-once producer into a Silver topic**: covers Kafka → Kafka only; the Postgres write
  would still need the same marker technique.
* **JDBC writes from executors**: distributed, but cannot share one transaction with the marker row without a
  staging-table protocol; deferred to the Iceberg design.
