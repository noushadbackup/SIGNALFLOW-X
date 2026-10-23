# ADR-0011: Chaos Lab: predefined incidents in a disposable lab database, verified against a clean run

**Status:** Accepted 2026-10-23 (Phase 5).

## Context
Phase 5 asks for a Chaos Lab with five incidents: a duplicate storm, a late surge, a schema break, a streaming
interruption and a stock discrepancy. Each one must be started, observed, recovered and verified, and must produce
an incident report. Two non-negotiables shape it:

* "All destructive actions must be isolated to synthetic local test data."
* "No claiming that a pipeline is fault-tolerant without testing its failure behavior."

So an incident cannot run against the demo database, and "recovered" has to mean something you can check, not a
status a script sets.

## Decision

### Where an incident runs
* Every run rebuilds a lab database (`SIGNALFLOW_LAB_DB`, default `signalflow_lab`) from a deterministic generator
  config: drop, create, migrate, load reference data, generate, load, process, dbt, hunt.
* `lab.lab_database_name` refuses any name without `lab` in it and any name configured for the demo, streaming,
  ERP, catalog, Airflow or Superset databases. The check runs before anything is dropped, and an integration test
  covers it.
* Run records, reports and the audit trail live in the **control** database (`ops.chaos_runs`, `ops.audit_log`).
  The lab can be dropped at any time without losing the history.

### What "recovered" means
* The lab first computes a **reference**: what a clean, uninterrupted run of the same dataset produces. It holds
  the Silver fingerprint (event id and payload hash of every accepted event) and the Gold figures the incidents
  touch (revenue per day, ordered value, stock per position).
* An incident holds back the records it will replay, damage or delay, so it starts from a state that differs from
  the reference only by the incident.
* Verification compares the recovered lab with the reference. Each check stores what was expected, what was
  observed and whether they matched. A run is PASSED only if every check passes.
* Verification uses only what the incident injected and the reference. Detection code (the Silver validator, the
  Leak Hunter rules, the alert checks) never sees either.

### The steps
`runner.run` follows the same steps for every incident: authorise → build the lab → baseline → inject → observe →
collect evidence → recover → verify → report. `ops.chaos_runs.phase` shows the current step while it runs. Each
step's duration goes into the report's timeline.

| Incident | Injected | Observed | Recovery |
|---|---|---|---|
| A duplicate storm | 3 extra copies of recent recharges | `ops.duplicate_events`, DUPLICATE_SURGE alert, no TEL-02 case | none needed: first arrival wins (ADR-0006) |
| B late surge | withheld recharges and billing, delivered after the watermark | lateness classes, `ops.late_events`, RESTATED days, auto-cleared TEL-01 cases | dbt rebuild of the affected Gold models |
| C schema break | the same transactions in a renamed or retyped v1 shape | quarantine reasons, CONTRACT_VIOLATIONS alert, contract report | producer fix: the same records re-sent in the registered shape |
| D interruption | SIGKILL of the Silver processor (its own OS process) mid-backlog | checkpoint stops, backlog stays, PROCESSING_STALLED alert, no row past the checkpoint | restart; resume from the checkpoint |
| E stock discrepancy | an approval applied twice, and an adjustment that contradicts its approval | MFG-04 cases with source records, UNCERTAIN positions | approved recount per position plus an analyst resolution, both audited |

### Permissions
Running an incident needs the `chaos.run` permission (operator role). The check runs in the database layer, so the
CLI and the UI share it. A refused attempt is audited.

## Consequences
* Each run takes about 20 to 30 s on the 4-vCPU build machine, most of it rebuilding the lab from scratch. That is the price of reproducible
  results. The dataset is small on purpose (`LabDataset`).
* Incident D proves resumption **for this run**. The report says so, and it does not claim zero loss in general.
* The lab uses a 2 s stall threshold so PROCESSING_STALLED can fire inside a test. The production default is
  2 min.
* The incidents run on the lite profile (files and the Python processor). Interrupting the Spark job is not
  automated here: the Phase 2 stream tests cover Spark restarts from its checkpoint.

## Alternatives considered
* **Inject into the demo database and roll back.** Rejected: one bug would damage the only database the demo
  shows, and some incidents (D) cannot be rolled back.
* **Docker-level chaos (killing containers).** Kept out of scope. It needs the full stack in CI and adds no
  verification the process-level kill does not already give.
* **Fixed expected numbers per incident.** Rejected: the reference comparison stays valid when the generator or
  the dataset size changes.
