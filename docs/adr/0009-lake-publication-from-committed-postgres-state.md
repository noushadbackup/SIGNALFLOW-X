# ADR-0009: The lake is published from committed Postgres state; Gold is rebuilt by the same dbt project on Trino

**Status:** Accepted 2026-10-22 (Phase 3). Supersedes the "Spark → Iceberg `MERGE`" line in ADR-0008 and
ARCHITECTURE §2.

## Context
Phase 3 adds Iceberg tables on S3-compatible storage, Trino, dbt-trino, Airflow and Superset. The streaming sink
(ADR-0008) already gives effectively-once Silver in Postgres, with one transaction per micro-batch that decides
dedupe, quarantine and lateness against Postgres state. The question is how Silver reaches the lake.

Options:
1. **Second sink inside `foreachBatch`**: commit Postgres, then append the same batch to Iceberg. A crash between
   the two commits leaves Postgres ahead; on replay the Postgres side is skipped (marker row) and the Iceberg side
   must then be rebuilt from Postgres anyway. Two commit points for one decision.
2. **Iceberg as the only Silver store** (`MERGE` from the executors). Dedupe and lateness would need Iceberg
   lookups per batch (slow at small batch sizes, and concurrent `MERGE`s conflict), and the lite profile, which is
   the reference implementation for differential testing, would no longer share Silver rules with the stream path.
3. **Publish committed state**: a separate Spark job copies what Postgres has committed to Iceberg, incrementally.

## Decision
Option 3.

* `signalflow.lakehouse.publish` (Spark 4.0.3 + Iceberg 1.10.1 runtime, JDBC catalog `lake` in Postgres database
  `signalflow_catalog`, `S3FileIO` to SeaweedFS) publishes 15 tables:
  * **append** (Bronze, Silver, quarantine, duplicates, late events, processing runs, master-data CDC): rows whose
    mark column is above the table's current high-water mark, bounded above by the source max read at the start
    of the run. The high-water mark is `max(mark column)` **in the Iceberg table itself**: rows and mark commit
    together, and the mark survives compaction and snapshot expiry. Each append snapshot also records it in its
    summary (`signalflow.hwm`) for audit.
  * **replace** (stream clocks, restatement log, reference data): whole table in one snapshot, only when the
    content hash changed (`signalflow.content-md5` in the summary), so history shows real changes only.
* The append mark columns are identity values written by a **single writer** (Silver processor in lite; the
  advisory-locked streaming sink), so ids commit in increasing order and a bounded range read is complete. With
  concurrent writers, a higher id could commit before a lower one and be skipped; that is why the writers stay
  single and why `lake compare` and the tests check counts and keys against Postgres.
* Gold is rebuilt on Trino by the **same dbt project**; engine differences live in adapter-dispatched macros
  (`dhaka_date`, `dhaka_day_end`, `p`, `div`, `epoch_seconds_between`, `md5_hex`, `open_end_ts`, `day_series` …).
  Gold tables use `on_table_exists: replace` (`CREATE OR REPLACE TABLE`), so each rebuild is a new snapshot of the
  same Iceberg table.
* `signalflow lake compare` compares every Gold model on both engines row by row (multiset, after normalising
  UUID/text, decimal scale and time zones) and records the result in `ops.lake_differential_checks`.
* Airflow runs publish → dbt (Trino) and dbt (Postgres) → compare every 30 minutes, one run at a time; a separate
  DAG compacts files (`rewrite_data_files`) and expires snapshots older than 7 days while keeping the last 50.

## Consequences
* One commit point decides Silver (Postgres); the lake is a derived, replayable copy. A publisher crash at any
  point leaves each table either untouched or fully published for that run (tested: SIGKILL during the Silver
  write, exit between tables).
* Lake freshness = Airflow interval + run time (about 1 minute at seed-7 volume), not seconds. Acceptable for
  analytics; the operational control tower reads Postgres.
* Killed writes can leave unreferenced data files in S3. Orphan removal is not automated because it is unsafe
  while a writer is active; it is a manual `remove_orphan_files` with an age threshold.
* Iceberg has no multi-table transaction: between two table commits of one run, Silver can be ahead of quarantine.
  dbt runs only after publication finishes (DAG order), so Gold never sees a half-published run.
* AWS SDK v2 (2.30+) sends `aws-chunked` uploads with trailing checksums by default; SeaweedFS 3.97 stored the chunk
  framing inside objects and every metadata file read back corrupt. Both Spark and Trino run with
  `AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED` / `AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_REQUIRED`.
