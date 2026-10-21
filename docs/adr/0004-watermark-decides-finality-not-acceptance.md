# ADR-0004: Watermark decides KPI finality, not acceptance

**Status:** Accepted (2026-10-21)

## Context
Streaming engines commonly drop records older than the watermark from stateful aggregations. For financial
events (recharges, settlements, invoices) dropping data creates false revenue gaps.

## Decision
* Per-stream watermark = max accepted event time − 6 h, advanced at the end of each micro-batch.
* All contract-valid events are accepted. Each is classified `ON_TIME`, `LATE_WITHIN_WATERMARK` or
  `LATE_BEYOND_WATERMARK`; the last class is logged and its business date marked `RESTATED`.
* Gold marts carry `kpi_status`: `PROVISIONAL` (watermark has not passed the end of the Dhaka business day),
  `FINAL`, or `RESTATED` (a beyond-watermark event changed an already-final day).

## Consequences
* Dashboards can show "not final yet" instead of presenting partial numbers as truth — required by the Revenue Leak
  Hunter to avoid treating delays as losses.
* Gold must be recomputable for past dates (full rebuild now; partition-level recompute in Phase 3).
* In the Spark path, windowed streaming aggregates may still use the watermark for state eviction; the authoritative
  financial marts are recomputed from Silver, not from streaming state.

## Alternatives considered
* **Drop late data**: simplest, wrong for money.
* **Infinite allowed lateness**: never-final KPIs and unbounded streaming state.
