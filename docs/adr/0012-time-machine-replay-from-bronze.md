# ADR-0012: Data Time Machine: replay a window from Bronze into an isolated preview, promote atomically, undo exactly

**Status:** Accepted 2026-10-23 (Phase 5).

## Context
Phase 5 asks for a Data Time Machine:

* inspect an event-time window;
* reprocess it in isolation;
* compare before and after, and verify counts, money and duplicates;
* inspect Iceberg snapshots and roll back.

The typical case is a faulty processor release that wrote wrong Silver rows (amounts multiplied, events dropped)
while Bronze kept the raw bytes. Repairing it must not race the live processor, must be reviewable before
anything changes, and must be reversible.

## Decision

### Bronze is the source of truth
A replay reads every Bronze record whose event time is in the window, plus the Bronze rows behind Silver events in
the window. It applies the **current** validation code, the same functions the processors use: `_validate_file_record`
for files, `validate_message` for Kafka rows. First arrival wins, including against an accepted Silver row of the
same event id **outside** the window, so a replay cannot create a duplicate.

### Lateness is reconstructed, not recomputed from scratch
An event's arrival class depends on the stream clock when its micro-batch started. The replay rebuilds that clock
from `ops.processing_runs`: for each Bronze row, it takes the run that read it (`from_bronze_id`) and computes the
largest accepted event time at or before that run's first row. With no defect, the replay reproduces Silver
exactly: on the demo database, 4,011 telecom records came out UNCHANGED.

### Isolation and review
* `preview` writes the replay to `ops.replay_events`, one row per Bronze record with its outcome and its change
  (UNCHANGED, CHANGED, INSERT, REMOVE, NONE). Silver is not touched.
* The run stores a per-day, per-type metrics diff (event counts and amounts), checks on duplicates and quarantine,
  a Gold snapshot of the touched days, and two fingerprints: the affected Silver rows now, and the same rows as
  the replay produces them.
* A window is at most 3 days, so a person can review the preview.

### Promotion
* `promote` runs as one transaction:
  1. It takes the lite checkpoint row with `FOR UPDATE NOWAIT` and the Spark sink's advisory lock. If a
     processor is running, the promotion is refused rather than racing it.
  2. It re-checks that Silver still matches the preview fingerprint. If not, the preview is stale and the
     promotion is refused.
  3. It writes the four targets (`silver.events`, `silver.quarantine`, `ops.duplicate_events`,
     `ops.late_events`).
  4. It archives every row it replaces, removes or inserts in `ops.replay_archive`. The archive is copied with
     `to_jsonb` inside Postgres, so numeric scale is preserved byte for byte.
  5. It marks already-final days RESTATED in `ops.kpi_restatements`.
  6. It checks that Silver now equals the replay fingerprint, and writes the audit row.
* `rebuild_gold` then runs dbt only for the models downstream of the changed event types. It takes them from the
  lineage graph (ADR-0013), intersected with the relations this database has built. Afterwards it reconciles
  Gold with Silver for the touched days:
  * telecom: gross recharge revenue;
  * manufacturing: ordered value and net stock movement.

### Undo
`undo` needs the promoted rows to be unchanged since the promotion (fingerprint check), so it never overwrites
newer data. It restores the archive inside SQL (`jsonb_populate_record`), checks that Silver equals the pre-promotion
fingerprint, and is followed by the same Gold rebuild.

### The Iceberg copies
* The lake publisher appends by high-water mark, so it never sees rows changed below the mark.
* `lake.sync` deletes and re-inserts exactly the touched keys through Trino. That makes two Iceberg snapshots per
  table, and their ids are recorded on the replay.
* `lake.undo_lake` checks whether a table's current snapshot is still the one the sync produced. It reads this
  from `$refs` (`main`), not from `$snapshots` or `$history`, because a rollback does not log a new snapshot.
  * If it is, the table is rolled back with `rollback_to_snapshot`.
  * If anything was committed since, it re-syncs the keys from Postgres instead, so later data is not discarded.
* Both paths are verified row by row against Postgres.

### Defects for demonstration
`defects.inject` damages Silver in the window (inflated amounts, deleted events). It refuses any database that is
not a lab database (ADR-0011) and needs `chaos.run`.

### Permissions
Preview needs `replay.preview` (analyst). Promote and undo need `replay.promote` and `replay.undo` (operator). Each
action is audited, including refusals.

## Consequences
* A replay cannot recover a record Bronze never received. For that, the source has to resend it (Incident C shows
  this path).
* The replay uses the current code. To reproduce an *old* bug on purpose, run the old code.
* While a promotion runs it holds the processor locks, so processors wait for it. Promotions are short: 20 changed
  rows take well under a second; the preview is the slow part.
* Gold for the whole demo is not rebuilt, only the downstream models. A model outside the lineage graph would be
  missed, which is why the graph is derived from the dbt manifest rather than declared by hand.

## Alternatives considered
* **Iceberg time travel as the repair.** Rejected as the primary path. Rolling the lake back would not repair the
  Postgres Silver the control tower reads, and it would discard every later append. Time travel is used to inspect
  snapshots and undo a lake sync.
* **Re-running the processor over a reset checkpoint.** Rejected: it would reprocess everything after the window,
  race the live processor, and give no preview.
* **A shadow schema with a full Silver copy.** Rejected: copying all of Silver to review a 3-day window is
  wasteful. `ops.replay_events` holds exactly the window.
