# ADR-0010: Revenue Leak Hunter: SQL rules, event-time case lifecycle, evaluation kept outside detection

**Status:** Accepted 2026-10-22 (Phase 4).

## Context
Phase 4 asks for a Revenue Leak Hunter over telecom and manufacturing data. The rules must be transparent and keep
transaction ids. They must also tell a real leak apart from a transaction that is only late. The platform accepts
events up to 30 h late on the telecom stream and 48 h late on the manufacturing stream (ADR-0004, generator
`LATE_BEYOND`). Billing can trail a recharge by up to 20 h, and a dealer can pay up to 3 days after an invoice is
due. A hunter that confirms every gap it sees would raise false alarms on all of these.

The design had four questions to answer:

1. Where the rules live and what they read.
2. How long a gap waits before it counts as a leak, and which clock measures the wait.
3. How a case moves through its states, and who may change it.
4. How to measure detection quality without leaking the generator's scenario labels into detection.

## Decision

### Rules
* There are ten rules, five per domain. Each one is a reviewed SQL file in `sql/assurance/` that reads committed
  Silver and reference data only.
* Every rule returns the same columns: `subject_key`, `anchor_time`, `business_date`, `business_unit`,
  `region_code`, `exposure_bdt`, `summary` and `evidence`. The evidence JSON lists the transactions involved
  (their source ids, event ids and amounts) and the facts the rule compared.
* `signalflow.assurance.rules` holds the metadata for each rule: title, exposure kind and the two windows.
  `assurance.rules` stores each rule's SQL sha256, so a case can be traced back to the exact rule text that
  produced it.
* The only parameter is the clock (`%(as_of)s`). A unit test rejects any other `%` sign in a rule file.

### Windows and the clock
* A case's age is the stream clock minus the rule's `anchor_time`. The stream clock is
  `ops.stream_state.max_event_time` for the rule's domain, the same clock the Silver watermark uses.
* The wall clock is never used. Replaying the same arrivals therefore produces the same statuses, whatever the
  hunting schedule. One integration test checks this: hunting after every file and hunting once at the end give
  the same open cases.
* `expected_within` is the normal processing time. While a case is younger than that, it is **PENDING**.
* `confirm_after` is at least the domain's late-data horizon: 36 h for telecom and 52 h for manufacturing. Both
  horizons are longer than any delay the platform accepts. Between the two windows a case is **SUSPICIOUS**.
  Past `confirm_after` it is **CONFIRMED_BY_RULE**.
* MFG-05 (invoice unpaid) starts its windows at the end of the invoice's due date. It is pending for 3 days, the
  late-payment allowance, and confirmed after 3 days plus 52 h.
* A unit test asserts every inequality above against the generator's own constants, so the "never confirm a
  normal delay" guarantee cannot drift silently.

### Lifecycle
* Each hunt is one transaction under `pg_advisory_xact_lock`. Overlapping runs wait for each other, and a failed
  run changes nothing. An integration test injects a failure in the eighth rule to check this.
* A case is keyed by `(rule_id, subject_key)` and its id is deterministic. The hunter moves cases through these
  states:
  * OPENED (as pending, suspicious or confirmed) when the rule first sees the subject.
  * STATUS_CHANGED as the case ages.
  * AUTO_CLEARED (RESOLVED) when the rule no longer sees it, because the counterpart arrived or the amounts now
    agree.
  * REOPENED if an auto-cleared subject is seen again.
* Analysts can add notes, assign a case, resolve it and reopen it. Every resolution needs a code and a note; the
  codes are RECOVERED, CORRECTED_AT_SOURCE, WRITTEN_OFF, FALSE_POSITIVE and ACCEPTED_RISK. Each action uses
  optimistic versioning (`expected_version`).
* An analyst's resolution stands: later hunts only refresh the case's evidence.
* Every change is appended to `assurance.case_events`. Every attempt, including denied ones, goes to
  `ops.audit_log`.
* Roles are viewer, analyst, operator and admin, in that order. Passwords are stored as scrypt hashes. Case
  actions need the analyst role.

### Exposure
* `exposure_bdt` is a rule-based estimate of money at risk. The UI and the CLI label it "estimate". It is never
  summed into revenue and never called an audited loss.

### Evaluation
* The scenario ledger produced by the generator is ground truth. Only `src/signalflow_eval` reads it, and that
  package is outside `signalflow`.
* Two tests guard the separation. A unit test fails if any file in `src/signalflow` mentions `signalflow_eval`.
  The Phase 1 isolation test still fails on the word "truth" anywhere in detection code.
* Each evaluation is written to `assurance.evaluation_runs`. The UI shows that table read-only.

## Consequences
* Statuses are reproducible and can be explained. An analyst can see the rule text, its hash, the transactions
  involved, the clock and both windows.
* A real leak is confirmed no sooner than the late-data horizon: 36 h for telecom and 52 h for manufacturing. This
  is the price of never confirming a normal delay. Suspicious cases are visible within minutes, so an analyst can
  act early.
* Leaks near the end of a dataset cannot be confirmed yet. The evaluation reports them as "not yet confirmable",
  not as misses.
* Each hunt runs a full scan of the rules' inputs. On the 12-day demo database (50 k Silver events) a hunt takes
  about 0.2 s. A larger history would need incremental rules, which are the Phase 6 benchmark candidate.
* Airflow runs `signalflow assurance hunt` every 10 minutes (DAG `signalflow_leak_hunter`). The lite pipeline
  hunts after every arrival file.

## Alternatives considered
* **Wall-clock aging.** Simpler, but a replay or a paused stream would confirm cases that only look old. Rejected.
* **Confirming immediately and auto-clearing later.** This produces false confirmations for every delay, which the
  requirements forbid. Rejected.
* **Machine-learning anomaly scoring.** Each scenario has an exact definition, a missing or duplicated
  counterpart, so deterministic rules are more accurate and easier to explain. The requirements also say not to
  use ML where rules fit. Rejected.
* **Rules in Python over DataFrames.** SQL keeps the rules next to the data, lets them be reviewed as text, and
  runs unchanged in `psql`. Rejected.
