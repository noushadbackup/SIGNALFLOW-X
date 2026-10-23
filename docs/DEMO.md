# SignalFlow X: demonstration guide

Three demonstrations, each runnable from a fresh clone on one machine (WSL2 or Linux, 16 GB RAM is enough for the
lite profile). Each one says what to show, what to say, and what the audience should check for themselves. Times are
from the 4-vCPU build machine.

Before any demo:

```bash
cp .env.example .env            # choose a local Postgres password
make setup up                   # venv, dependencies, Postgres 16 on 127.0.0.1
make generate DATA=data/seed7-12d DAYS=12 SUBSCRIBERS=1000 && make run DATA=data/seed7-12d   # ~1 minute
make operator OPERATOR=nadia ROLE=analyst NAME="Nadia Rahman"       # password prompted, scrypt-hashed
make operator OPERATOR=tanvir ROLE=operator NAME="Tanvir Ahmed"
make ui                         # control tower on http://127.0.0.1:8501
```

All data is synthetic. Nothing on screen is sample data: every figure is a query against Postgres (or Trino on the
Lakehouse page), and each chart names its source table underneath.

---

## Demo 1: normal operations with continuously updating data (about 8 minutes)

**Run**

```bash
scripts/demo/demo1_live_operations.sh             # terminal 1: recreates signalflow_live, feeds 157 files, one every 2 s
make ui PGDATABASE=signalflow_live                # terminal 2 (stop the other UI first, or use --server.port)
```

In the sidebar, turn on **Live refresh (every 15 s)**.

**Show**

1. *Overview*: Bronze, Silver, quarantine and duplicate counters rise; the last micro-batch time moves. Revenue by
   day fills in, and KPI finality moves from Provisional to Final as the stream watermark passes the end of each day.
2. *Pipeline observatory*: each micro-batch is one transaction (batch durations from `ops.processing_runs`);
   quarantine reasons appear as the generator's schema breaks arrive; duplicates are logged, not loaded.
3. *Telecom* and *Manufacturing*: the digital twins are rebuilt from processed events. Each section says whether it
   is a fact, a derived figure, a projection or an estimate.
4. *Revenue Leak Hunter*: cases open as PENDING, some clear themselves when the late billing record arrives
   (AUTO_CLEARED), and the rest become SUSPICIOUS and then CONFIRMED_BY_RULE only after the late-data horizon.

**Say**

* Ingestion never rejects: Bronze keeps every line byte for byte. Validation and deduplication happen on the way to
  Silver, inside the same transaction that moves the checkpoint (ADR-0006).
* A late event is accepted and labelled. If its day was already final, the day is marked RESTATED (ADR-0004).

**Audience can check:** `make status`, or `psql` against `signalflow_live`: Silver equals the generator's valid
events once the feed ends (the integration tests assert exactly this).

**Streaming variant** (Kafka, Debezium, Spark; needs Java 17 and about 3 GB more RAM): `make stream-up stream-init`,
then `make stream-silver` in one terminal and `make stream-replay` in another, and point the UI at the streaming
database with `make ui PGDATABASE=signalflow_stream`.

---

## Demo 2: telecom revenue discrepancy, detection to decision (about 5 minutes; the script itself takes 3 s)

**Run**

```bash
ANALYST=nadia DATA=data/seed7-12d scripts/demo/demo2_revenue_leak.sh
```

**Show**

1. The hunter runs ten reviewed SQL rules over committed Silver. `TEL-01` finds successful recharges with no billing
   record; `TEL-02` finds a payment captured twice.
2. The most severe confirmed telecom case: its evidence (the recharge, the missing billing record, the stream clock
   when it was confirmed) and its append-only history.
3. As the analyst: a note, an assignment and a resolution. Each action checks the role, carries a version (two
   analysts cannot overwrite each other), and is written to the audit log, including refused attempts.
4. The offline evaluation: precision and recall per rule against the generator's scenario ledger. This lives in
   `signalflow_eval`, a separate package; a test fails the build if detection code ever imports it.
5. In the control tower: *Revenue Leak Hunter* shows the same case, its raw Bronze lines, and exposure by business
   unit. Sign in as `nadia` to act on it from the page.

**Say**

* A case is confirmed by the data and the event-time clock, never by how often the hunter runs: the integration test
  runs it at two cadences and compares the final case state.
* Delayed but complete transactions (late billing, late dealer payments) are in the ledger as benign. None is ever
  confirmed; the test fails if one is.
* Exposure is a rule-based estimate of money at risk, not an audited loss.

---

## Demo 3: failure, recovery, replay and correctness (about 6 minutes; the script itself takes under a minute)

**Run**

```bash
OPERATOR=tanvir scripts/demo/demo3_failure_recovery.sh
```

**Show**

1. **Incident D**: the Silver processor runs as its own OS process and is killed with SIGKILL mid-backlog. The report
   shows the checkpoint at the kill, no Silver, quarantine or duplicate row past it, the PROCESSING_STALLED alert,
   the restart, and a final comparison with a clean run of the same dataset (Silver fingerprint and Gold figures).
2. **Incident B**: withheld recharges and billing arrive after the watermark. They are accepted, classed
   LATE_BEYOND_WATERMARK, the affected days are RESTATED, Gold is rebuilt and equals the clean run, and the Leak
   Hunter cases those records would have opened clear themselves.
3. **Time Machine**: the script inflates 20 recharge amounts in the lab database's Silver and rebuilds Gold from them,
   as a faulty release would, so the day's gross recharge reads wrong (BDT 66,680 instead of 48,500 in the
   recorded run). It then inspects the window, replays it from Bronze into an isolated preview (Silver untouched),
   promotes it in one transaction under the processor locks, rebuilds only the downstream Gold models (back to
   48,500) and reconciles Gold with Silver.
4. In the control tower: *Chaos Lab* shows both reports with every check (expected, observed, pass); *Time Machine*
   shows the replay, the changed rows and the Gold before and after. *Undo and rebuild Gold* restores the previous
   state exactly.
5. *Benchmark Lab*: the recovery experiment repeats the SIGKILL on a larger backlog and records the restart-to-drained
   time.

**Say**

* Everything destructive runs in `signalflow_lab`, rebuilt from a deterministic dataset on every run; the code refuses
  any protected database name before it drops anything.
* "Recovered" means equal to a clean run of the same data, check by check. It is shown, not asserted.
* The streaming path has its own failure tests: `make test-stream` kills the Spark job mid-stream and requires the
  same Silver as the lite reference and Gold equal to truth.

---

## Benchmark Lab (optional, 2 minutes)

`make bench WORKLOAD=S` (about 6 minutes) records a run; *Benchmark Lab* shows throughput by workload size, baseline
against optimized (jsonschema against compiled validation, row-by-row INSERT against COPY, Silver queries against Gold
marts), latency under paced load, the processor profile, recovery, and every correctness check. The numbers belong to
the machine they were measured on; the page shows which.
