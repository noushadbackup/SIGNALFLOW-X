# ADR-0013: Lineage derived from code and artifacts at table level; metrics read from Postgres at scrape time

**Status:** Accepted 2026-10-23 (Phase 5).

## Context
Phase 5 asks for practical lineage from source to KPI, with impact analysis, and without claiming column-level
lineage. It also asks for Prometheus and Grafana. Two things make lineage go wrong in practice:

* A hand-drawn graph drifts from the code.
* A graph that ignores how the platform runs (with or without Kafka, Spark and the lake) gives the wrong impact.

For metrics, counters kept in a process's memory reset on restart and disagree with the tables the control tower
reads.

## Decision

### Lineage
* The graph is built from what the platform already has:
  * the contract registry (event types);
  * the topic router (Kafka topics);
  * the dbt manifest from `dbt parse` into its own target path, `target/lineage`. Lab and lake runs overwrite
    `target/manifest.json`, so it uses a separate one;
  * the assurance rule SQL (which event types each rule reads);
  * the control tower code (string literals and `data.*` queries reachable from each page function, followed with
    `ast`);
  * the Superset chart definitions.
* Only jobs and KPIs are declared (`lineage/catalog.py`). A KPI's model and column must exist in the manifest. A KPI
  is linked to a dashboard only when that dashboard reads its model **and** mentions its column.
* Every edge names the job that moves the data. A job belongs to the lite profile, the stream profile or both, so
  impact is always computed for one profile. A stopped Spark job affects nothing in lite.
* Shared ops tables that hold one row set per stream (`stream_state`, `late_events`, `kpi_restatements`) are split
  per stream. Without the split, every telecom alert would fan out to manufacturing.
* Paths prefer data edges over clock and evidence edges (weight 4, Dijkstra). The path shown is the one a reader
  expects. Reachability ignores the weights.
* Granularity is the table, and the event type inside the shared Bronze and Silver tables. `COLUMN_LEVEL = False`,
  and the graph's notes say that column-level lineage is not captured. A unit test asserts both.
* Live state on top of the graph (`lineage/status.py`):
  * freshness per node (row counts, data-through dates, last hunt);
  * the likely impact of each firing diagnostic alert. An alert subject that does not identify a node maps to
    nothing rather than a guess.

### Metrics
* `observability/exporter.py` serves the Prometheus text format using only the standard library. On every scrape
  it reads Postgres: Bronze, Silver, quarantine, duplicates, backlog, processor runs, stream clocks, restatements,
  firing alerts, Leak Hunter cases and exposure, lake publish age, lake/Gold differential mismatches, Chaos Lab
  runs and replays.
* A failed read reports `signalflow_up 0` instead of serving stale numbers.
* Prometheus rules: ExporterDown, DiagnosticAlertFiring, ProcessingBacklog, QuarantineIncrease, LeakHunterStale,
  LakeGoldMismatch. A provisioned Grafana dashboard (15 panels) reads only these series.
* Compose binds the exporter, Prometheus and Grafana to 127.0.0.1. Grafana needs `GRAFANA_ADMIN_PASSWORD` from the
  environment. No password is committed.

## Consequences
* A new dbt model, rule or dashboard query appears in the lineage without anyone editing a list. A new *job* or
  *KPI* needs one line in the catalog, and a unit test fails if an edge names an unknown job.
* Dashboard reads are found by static analysis. A table name built at run time from parts would be missed. The
  control tower does not do that today. The unit tests pin the expected links for the main pages.
* The operator tools (Chaos Lab, Time Machine, Lineage) are not modelled as dashboards. They are not KPI consumers.
* Scrape cost grows with table size (count queries). On the demo database (about 50,000 Silver events) a scrape takes about 0.1 s. At
  production volumes these counts would come from maintained counters or `pg_stat` estimates.
* The optional AI explanation module was not built. Everything above is deterministic.

## Alternatives considered
* **OpenLineage/Marquez.** It would need emitters in every job plus another service, and it would still not know the
  control tower's reads. Table-level lineage derived from code covers the stated need with nothing extra to run.
* **Column-level lineage from SQL parsing.** Not attempted. Claiming it without parsing every model correctly would
  break the "no fake implementation" rule.
* **`prometheus_client` with in-process counters.** Rejected: counters reset on restart and would differ from the
  tables the reports cite.
