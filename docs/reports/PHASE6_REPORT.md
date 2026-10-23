# Phase 6 Report: Control Tower, Benchmark Suite, Demonstrations and Presentation

Date: 2026-10-23. Environment: cloud Linux container, 4 vCPU (Intel Xeon @ 2.10 GHz), 15.7 GB RAM, Docker 27,
Python 3.12, PostgreSQL 16.15 (shared_buffers 256 MB, synchronous_commit on), dbt-core 1.11, Streamlit 1.50.

## What now works

| Component | Path | Notes |
|-----------|------|-------|
| Benchmark suite | `src/signalflow_bench/` | Twelve experiments on four workload sizes (13,037 to 1,003,875 arrival lines). Each one runs the production code in a dedicated `signalflow_bench*` database and returns checks next to its measurements; a run with a failed check is recorded as FAILED. `make bench WORKLOAD=S`, `make bench-list`, `python -m signalflow_bench.report`. |
| Results | `ops.benchmark_runs` (migration 006), `benchmarks/results/*.json` | The machine (CPUs, memory, Postgres settings, git commit, running containers, load average) is stored with every run. |
| Optimized implementation | `contracts.py` | Contracts are also compiled with fastjsonschema. A record the compiled check accepts is accepted; a record it rejects is validated again by jsonschema, which gives the reasons and decides. `SIGNALFLOW_VALIDATOR=reference` is the baseline. ADR-0014. |
| Benchmark Lab | `ui/benchmark_lab.py` | Scaling across workloads, one run in detail (baseline against optimized, latency and backlog, processor profile, recovery, Gold, Leak Hunter, Time Machine, storage) and every correctness check with its expected and observed value. |
| Control tower | `ui/app.py` | The overview gains a reliability strip (alerts, Chaos Lab results, replays, latest benchmark), each tile read from the table its tool writes. The "Planned" section is gone: every page is implemented. |
| Continuous feed | `signalflow run --pace-seconds N --gold-every K` | Feeds arrival files one at a time, runs the Leak Hunter after each and rebuilds Gold every K files, so the control tower can be watched while data arrives. |
| Demonstrations | `scripts/demo/demo1..3*.sh`, `docs/DEMO.md` | Live operations; telecom revenue leak from detection to an audited analyst decision; failure, recovery, replay and correctness. All three were run end to end (results below). |
| Presentation | `docs/assets/architecture.svg`, `docs/INTERVIEW_GUIDE.md`, README | Architecture diagram, the design decisions with their trade-offs and evidence, stories of problems found by testing, likely interview questions, and what not to claim. |

The optional AI investigator was not built (see the last section).

## Headline results

Machine as above; only the Postgres container was running (every other compose service was stopped). Commit
`c448e7a`. One run per workload: `BENCH-XS-20261023T173953`, `BENCH-S-20261023T174053`, `BENCH-M-20261023T174519`,
`BENCH-L-20261023T175818`. All four runs completed with **61 of 61 checks passed**.

| | XS | S | M | L |
|---|---|---|---|---|
| Arrival lines | 13,037 | 142,472 | 560,166 | 1,003,875 |
| Silver processor, jsonschema (baseline), rows/s | 4,189 | 4,714 | 4,614 | 4,629 |
| Silver processor, compiled (best chunk), rows/s | 9,272 | 12,334 | 11,618 | 11,756 |
| Validator speed-up at the same chunk size | 2.21× | 2.55× | 2.49× | 2.45× |
| End to end, file by file (load + process), events/s | 4,557 | 6,667 | 7,218 | 7,368 |
| Latency at 50% of capacity, Bronze → Silver, p50 / p95 / p99 ms | 39 / 91 / 96 | 116 / 167 / 305 | 200 / 282 / 931 | 334 / 500 / 1,409 |
| Gold freshness after the last arrival (s) | 4.9 | 5.3 | 6.4 | 6.5 |
| Recovery after SIGKILL: restart to drained (s) / backlog | 0.60 / 3,037 | 6.73 / 82,472 | 29.90 / 335,166 | 52.86 / 598,875 |
| Gold mart vs Silver at query time, usage query p50 | 105× | 550× | 1,625× | 2,488× |
| Bronze COPY vs one INSERT per line | 4.2× | 6.9× | 7.4× | 8.2× |

What these numbers say, and what they do not:

* **The optimization is real and correct.** Compiled validation makes the Silver processor 2.2 to 2.6 times faster at
  every size, and every configuration (baseline and four chunk sizes) produced the same Silver fingerprint as the
  continuous run.
* **Per-row cost stayed flat over a 7× range.** From S (142k lines) to L (1.0M lines) the compiled processor ran at
  11,300 to 12,300 rows/s and the baseline at 4,600 to 4,700. XS is lower because fixed costs (process start, the
  first batches) weigh more on 13k rows. This is a measurement of this machine over this range. It is not a claim
  about larger volumes or other hardware.
* **The limit is one serial processor, not the machine.** During the compiled backfill the worker used about 0.6 of
  a core and Postgres about 0.5, and they mostly take turns. Three of the four cores were idle (details below).

## Method

* **Workloads.** The deterministic generator (seed 11) with 3, 7, 14 and 14 days and 1,000, 5,000, 10,000 and
  18,000 subscribers. Each workload contains the same kinds of defects as the demo data: duplicates, late and very
  late arrivals, schema breaks, conflicting ids, an unparseable line and business anomalies. Sizes are reported as
  arrival lines actually processed, never as file sizes.
* **Isolation.** Every experiment uses `signalflow_bench`, `signalflow_bench_bronze` (Bronze loaded once, then copied
  with `CREATE DATABASE … TEMPLATE … STRATEGY FILE_COPY`) or `signalflow_bench_work`. The suite refuses any name
  without `bench` and any database the platform uses, and drops all three at the end.
* **Clocks.** Wall time from `time.perf_counter`. Per-row latency uses two Postgres clocks: the commit time of the
  Bronze load that wrote the row (`clock_timestamp()` read right after the load commits) and the `finished_at` the
  processor writes (with `clock_timestamp()`) for the batch that read it. Rows are attributed to batches by Bronze id
  range, and a check requires contiguous Bronze ids so that the attribution is exact.
* **Percentiles.** Nearest rank, weighted by rows (a 5,000-row batch counts 5,000 times).
* **CPU and memory.** The worker process's CPU from its rusage; Postgres's CPU from the container's cgroup
  (`cpuacct.usage` on this cgroup v1 host); worker peak memory from `VmHWM` in `/proc`, sampled while it runs.
  Postgres I/O from `pg_stat_database` deltas (WAL bytes, cache hit ratio).
* **Load test.** The capacity is the best compiled backfill rate measured in the same run. A producer thread loads
  arrival files into Bronze at 50% and 90% of that rate for up to 60 s while the processor runs continuously (it
  polls every 50 ms when idle). A sampler records the backlog (rows in Bronze not yet processed) every 0.25 s.
* **Queries.** 2 warm-up runs, then 20 timed runs of each query on Silver and on the Gold mart, and a check that both
  return the same answer.
* **One run per workload.** There are no repeated runs and no confidence intervals. Differences of a few percent
  between workloads (M against L, for example) are within what a second run could change.

## Correctness checks (61 per run, all passed in all four runs)

| Experiment | What is checked | Against |
|---|---|---|
| Bronze loading | Row-by-row INSERT and COPY store identical Bronze rows | each other |
| Continuous | Silver holds every unique valid event once; event ids equal the valid ids; technical duplicates logged, not loaded; quarantine count per reason | generator records |
| Gold | dbt build with no error, warning or skip; daily revenue per date, channel and region to the poisha; total revenue; Gold total equals Silver; stock per warehouse and SKU; settlement reconciliation flags exactly the injected mismatches | generator records, Silver |
| Leak Hunter | Offline evaluation passes: no false confirmation, benign delays never confirmed | scenario ledger |
| Serving | Silver and Gold queries return the same answer (3 queries) | each other |
| Time Machine | Replaying an undamaged 72 h window changes, inserts and removes nothing; the replay fingerprint equals Silver | Silver |
| Backfill (×5 configurations) | Worker exited cleanly; backlog drained; Silver fingerprint equals the continuous run; duplicates; quarantine; processor runs cover Bronze without gaps or overlaps | continuous run, generator records |
| Recovery | Killed while work remained; no Silver, quarantine or duplicate row past the checkpoint; clean restart; drained; no gaps or overlaps; duplicates once; quarantine; Silver fingerprint equals the continuous run | continuous run, generator records |
| Load (×2 rates) | Every Bronze row processed exactly once; Bronze ids contiguous; no gaps or overlaps | Bronze |

Duplicate handling, reconciliation and financial consistency are therefore measured on every workload, not only in
the test suite: at L, 1,003,875 lines produced exactly the expected Silver events, Gold revenue equal to the ledger
to the poisha, and stock positions equal to the ledger.

## Full results

The tables below are the output of `python -m signalflow_bench.report` for the four runs.

### Throughput and correctness by workload

| Workload | Lines | Continuous events/s | Continuous latency p50/p95/p99 (ms) | Backfill jsonschema rows/s | Backfill compiled rows/s by chunk | Validator speed-up | dbt build (s) | Checks |
|---|---|---|---|---|---|---|---|---|
| XS | 13,037 | 4,557 | 42 / 119 / 119 | 4,189 | 500: 8,113 · 2,000: 8,657 · 5,000: 9,266 · 20,000: 9,272 | 2.21× | 7.6 | 61/61 |
| S | 142,472 | 6,667 | 145 / 229 / 541 | 4,714 | 500: 9,336 · 2,000: 10,824 · 5,000: 12,026 · 20,000: 12,334 | 2.55× | 8.2 | 61/61 |
| M | 560,166 | 7,218 | 270 / 411 / 1,204 | 4,614 | 500: 9,605 · 2,000: 10,828 · 5,000: 11,475 · 20,000: 11,618 | 2.49× | 9.9 | 61/61 |
| L | 1,003,875 | 7,368 | 478 / 724 / 2,454 | 4,629 | 500: 9,322 · 2,000: 11,042 · 5,000: 11,322 · 20,000: 11,756 | 2.45× | 11.9 | 61/61 |

"Continuous" is the lite profile as it runs: load one arrival file into Bronze, process the backlog, next file. Its
latency is per file, from the start of the file's load to the commit of its last Silver batch, with no backlog.

### Latency under paced load (Bronze commit to Silver commit, per row)

| Workload | Load | Target rows/s | Achieved rows/s | p50 ms | p95 ms | p99 ms | Backlog p95 (rows) | Drain after last arrival (s) | Gold freshness (s) |
|---|---|---|---|---|---|---|---|---|---|
| XS | 50% | 6,574 | 6,644 | 39 | 91 | 96 | 548 | 0.01 | 4.9 |
| XS | 90% | 11,833 | 9,172 | 53 | 84 | 111 | 1,069 | 0.01 | – |
| S | 50% | 6,238 | 6,273 | 116 | 167 | 305 | 1,392 | 0.01 | 5.3 |
| S | 90% | 11,228 | 11,273 | 161 | 340 | 350 | 4,803 | 0.02 | – |
| M | 50% | 5,792 | 5,852 | 200 | 282 | 931 | 2,835 | 0.27 | 6.4 |
| M | 90% | 10,425 | 10,430 | 260 | 530 | 727 | 5,979 | 0.03 | – |
| L | 50% | 5,690 | 5,811 | 334 | 500 | 1,409 | 5,039 | 0.46 | 6.5 |
| L | 90% | 10,243 | 10,402 | 405 | 873 | 1,553 | 10,156 | 0.47 | – |

The backlog is the ingestion lag in rows. Gold freshness is the time from the last arrival's Bronze commit until the
revenue mart includes it (drain plus `dbt run --select +mart_telecom_revenue_daily`); it is measured at 50% load.

### Recovery after SIGKILL

| Workload | Killed at | Backlog at kill | Restart to drained (s) | Rows/s after restart | All checks passed |
|---|---|---|---|---|---|
| XS | 77% | 3,037 | 0.60 | 5,036 | yes |
| S | 42% | 82,472 | 6.73 | 12,262 | yes |
| M | 40% | 335,166 | 29.90 | 11,210 | yes |
| L | 40% | 598,875 | 52.86 | 11,329 | yes |

The kill is aimed at 40% of the rows. The checkpoint moves in 5,000-row batches, so on XS (13,037 rows) the first
point past 40% was 77%.

### Serving queries: Silver at query time vs Gold mart (ms, p50 / p95)

| Workload | Query | Silver | Gold | Speed-up (p50) |
|---|---|---|---|---|
| XS | Recharge revenue by day, channel and region (whole period) | 2.8 / 3.6 | 0.32 / 0.96 | 8.8× |
| XS | Usage by day, region and type (whole period) | 25.4 / 31.5 | 0.24 / 0.45 | 104.7× |
| XS | One day's recharge revenue (a dashboard tile) | 0.7 / 1.0 | 0.15 / 0.42 | 4.6× |
| S | Recharge revenue by day, channel and region (whole period) | 19.1 / 23.0 | 0.55 / 1.29 | 34.6× |
| S | Usage by day, region and type (whole period) | 376.1 / 427.7 | 0.68 / 1.54 | 549.6× |
| S | One day's recharge revenue (a dashboard tile) | 2.2 / 2.8 | 0.21 / 0.97 | 10.3× |
| M | Recharge revenue by day, channel and region (whole period) | 84.0 / 107.9 | 0.96 / 1.24 | 87.7× |
| M | Usage by day, region and type (whole period) | 1,025.9 / 1,137.2 | 0.63 / 1.12 | 1624.9× |
| M | One day's recharge revenue (a dashboard tile) | 4.3 / 5.2 | 0.18 / 0.54 | 24.1× |
| L | Recharge revenue by day, channel and region (whole period) | 279.1 / 331.4 | 0.97 / 1.52 | 287.8× |
| L | Usage by day, region and type (whole period) | 1,806.4 / 1,968.6 | 0.73 / 0.95 | 2487.5× |
| L | One day's recharge revenue (a dashboard tile) | 5.2 / 7.2 | 0.24 / 0.74 | 21.9× |

### Bronze loading

| Workload | Lines compared | INSERT per line (lines/s) | COPY (lines/s) | Speed-up | Full load, COPY (lines/s) | Database copy for a backfill (s) |
|---|---|---|---|---|---|---|
| XS | 13,037 | 3,018 | 12,536 | 4.2× | 12,440 | 0.10 |
| S | 21,417 | 2,834 | 19,505 | 6.9× | 20,058 | 0.26 |
| M | 21,177 | 3,101 | 22,993 | 7.4× | 20,534 | 1.07 |
| L | 20,704 | 2,505 | 20,519 | 8.2× | 20,696 | 1.78 |

The comparison uses the first files of each workload (about 20,000 lines; all 13,037 on XS), one transaction per file
in both loaders.

### Backfill resources

| Workload | Validator / chunk | Worker cores | Postgres cores | Worker peak RSS (MB) | WAL bytes per row | Cache hit ratio | Batch p50 (ms) | Batch p95 (ms) |
|---|---|---|---|---|---|---|---|---|
| XS | reference / 5,000 | 0.83 | 0.16 | 64 | 1,435 | 0.9883 | 998 | 1,008 |
| XS | compiled / 500 | 0.61 | 0.35 | 45 | 1,451 | 0.9898 | 45 | 57 |
| XS | compiled / 2,000 | 0.67 | 0.32 | 52 | 1,435 | 0.9884 | 156 | 178 |
| XS | compiled / 5,000 | 0.69 | 0.35 | 64 | 1,435 | 0.9883 | 345 | 392 |
| XS | compiled / 20,000 | 0.67 | 0.42 | 98 | 1,445 | 0.9882 | 979 | 979 |
| S | reference / 5,000 | 0.84 | 0.17 | 65 | 1,440 | 0.9878 | 1,053 | 1,149 |
| S | compiled / 500 | 0.53 | 0.43 | 46 | 1,455 | 0.9923 | 48 | 61 |
| S | compiled / 2,000 | 0.56 | 0.45 | 52 | 1,449 | 0.9921 | 160 | 211 |
| S | compiled / 5,000 | 0.61 | 0.46 | 65 | 1,445 | 0.9918 | 388 | 451 |
| S | compiled / 20,000 | 0.61 | 0.45 | 131 | 1,441 | 0.9907 | 1,483 | 1,635 |
| M | reference / 5,000 | 0.82 | 0.21 | 66 | 1,474 | 0.9889 | 1,063 | 1,158 |
| M | compiled / 500 | 0.50 | 0.45 | 46 | 1,471 | 0.9933 | 47 | 61 |
| M | compiled / 2,000 | 0.55 | 0.46 | 52 | 1,468 | 0.9933 | 171 | 202 |
| M | compiled / 5,000 | 0.58 | 0.48 | 66 | 1,466 | 0.9932 | 403 | 485 |
| M | compiled / 20,000 | 0.60 | 0.45 | 134 | 1,463 | 0.9923 | 1,652 | 1,844 |
| L | reference / 5,000 | 0.82 | 0.21 | 66 | 1,491 | 0.9857 | 1,060 | 1,206 |
| L | compiled / 500 | 0.50 | 0.46 | 48 | 1,488 | 0.9903 | 49 | 62 |
| L | compiled / 2,000 | 0.55 | 0.48 | 53 | 1,486 | 0.9896 | 166 | 201 |
| L | compiled / 5,000 | 0.57 | 0.49 | 66 | 1,484 | 0.9894 | 409 | 481 |
| L | compiled / 20,000 | 0.59 | 0.47 | 134 | 1,480 | 0.9884 | 1,652 | 1,838 |

### Processor profile (L, 51,647 lines per validator, cProfile)

| Category | jsonschema | compiled |
|---|---|---|
| JSON parse and serialise | 1.14 s (5%) | 1.08 s (15%) |
| Postgres driver and waiting on the server | 2.65 s (11%) | 2.73 s (38%) |
| SignalFlow processor code | 1.64 s (6%) | 1.26 s (17%) |
| contract validation (compiled) | – | 0.97 s (13%) |
| contract validation (jsonschema) | 15.37 s (61%) | 0.04 s (1%) |
| hashing | 0.05 s (0%) | 0.06 s (1%) |
| other (interpreter, stdlib) | 4.10 s (16%) | 0.99 s (14%) |
| timestamps | 0.10 s (0%) | 0.09 s (1%) |

cProfile adds overhead to every Python call, so the Python-heavy categories are overstated; the shares are used to
find the bottleneck, not as timings. The 0.04 s of jsonschema in the compiled run is the re-validation of the
rejected records.

### Storage, Leak Hunter and Time Machine

| Workload | Database (MB) | Bronze bytes/line | Silver bytes/event | Database / files | Leak Hunter run (s) | Time Machine 72 h preview |
|---|---|---|---|---|---|---|
| XS | 44 | 1,010 | 576 | 7.58× | 0.24 | 11,583 in 2.5 s |
| S | 308 | 1,017 | 576 | 4.83× | 2.53 | 57,745 in 16.8 s |
| M | 1,061 | 1,016 | 576 | 4.22× | 2.29 | 115,656 in 31.7 s |
| L | 1,894 | 1,019 | 575 | 4.21× | 4.84 | 207,028 in 58.6 s |

Generating the data ran at 19,800 to 23,700 lines/s (50.6 s for L).

## Bottlenecks

1. **Contract validation (fixed).** With jsonschema, validation took 61% of the processor's profiled time and the
   worker was CPU-bound (0.82 cores). Compiling the contracts removed almost all of it (2.45× at L). Rejections are
   still decided by jsonschema, and a unit test runs both validators on 1,930 crafted variants and requires identical
   decisions, so the speed-up changes no outcome.
2. **One serial processor (the current limit).** After the fix the worker uses about 0.57 cores and Postgres about
   0.49, and they alternate: the worker builds a batch, then waits while Postgres writes it. The profile agrees: the
   largest share (38%) is the Postgres driver and waiting on the server. About one core of four is in use. The design
   allows one writer per stream (the checkpoint is a row lock, ADR-0006), so more throughput needs either pipelining
   inside the processor (prepare the next batch while the current one commits) or a checkpoint partitioned by stream
   or key range so that several processors can run. Neither is built.
3. **Batch size trades latency and memory for throughput.** From 500 to 20,000 rows per batch, throughput rises 26%
   at L (9,322 to 11,756 rows/s), batch time rises from 49 ms to 1.65 s and worker peak memory from 48 MB to 134 MB.
   The default of 5,000 keeps 96% of the best throughput with 0.4 s batches.
4. **Bronze loading shares the loop.** In the file-by-file run, 35% of the time goes to loading Bronze, so the end
   to end rate (7,368 events/s at L) is below the processor alone (11,456 rows/s in the same run). Bronze COPY itself
   runs at about 20,700 lines/s; one INSERT per line would be 8 times slower.
5. **Latency follows file size.** Files are processed whole, and larger workloads have larger arrival files (median
   178 lines at XS, 3,234 at L; the largest is the opening file with 1,048 to 18,048 lines). Per-row latency is
   therefore roughly file size divided by throughput, and the p99 values come from the opening file. At 90% of
   capacity the backlog stayed bounded for the 60 s measured (at L its mean was 6,166 rows in the first quarter and
   4,714 in the last). Loads above capacity were not run; there the backlog grows by definition.
6. **Gold is a full rebuild.** `dbt build` grows from 7.6 s to 11.9 s, and Gold freshness after the last arrival
   from 4.9 s to 6.5 s. Queries on Gold stay under 1 ms at p50 at every size, while the same aggregate computed from
   Silver grows with the data (1.8 s at L). Incremental dbt models for the large facts are the next step; they are
   not built.
7. **Recovery costs the backlog, not more.** From S to L, the backlog divided by the restart-to-drained time gives
   11,210 to 12,262 rows/s, the same as the backfill capacity. The restart itself (a new process reading the checkpoint) is under a second on
   XS, including its 3,037 rows.

Two results look odd and are reported as measured: on XS at 90% load the producer reached 9,172 of the 11,833 rows/s
it aimed for (the whole XS dataset arrives in about 1.4 s, and Bronze loads of its small files run at about 12,400
lines/s, so the producer itself could not go faster); and the Leak Hunter took 2.53 s on S and 2.29 s on M (one run
each; a second run would show whether this is noise).

## Problems found while building, and fixes

* **An idle CDC slot filled the disk (before the benchmarks).** The master-data connector had no heartbeat, and
  Postgres kept 6.5 GB of WAL for its slot. Both connectors now emit heartbeats, the exporter reports retained WAL
  per slot and Prometheus alerts above 1 GiB (commit `10bcfd3`).
* **A stopped stream profile pins WAL too.** For the benchmarks, Kafka Connect was stopped. Its two slots became
  inactive and Postgres kept 5.6 GB of WAL, which nearly filled the disk during the first benchmark attempt. The
  slots were dropped on the benchmark machine. This is how logical replication slots behave; the new alert reports
  it, and a new `make stream-teardown` (documented in the README) removes connectors and slots before a long stop.
* **The dry run measured the wrong process's memory.** `getrusage(RUSAGE_CHILDREN)` reported the parent's peak RSS
  (the same number for every configuration). The suite now samples `VmHWM` of the worker process. All dry-run
  results were deleted, and every number in this report comes from the final runs.
* **The Time Machine check counted records with no Silver row.** Records whose replay outcome is a duplicate or a
  quarantine have change kind NONE; the "replay changes nothing" check now excludes them.
* **Container CPU on cgroup v1.** `cpu.stat` does not exist on this host; the suite reads `cpuacct.usage` instead.
* **A fixed number in the lake test.** It asserted `PASS=100` dbt nodes; Phase 5 added models. It now compares the
  lite and lake build summaries.
* **Demo scripts ignored the caller's database.** They sourced `.env`, which reset `PGDATABASE`.
  `scripts/demo/_env.sh` now loads `.env` without overriding what the caller set. For the same reason the docs use
  `make ui PGDATABASE=…` (a command-line variable overrides the Makefile's `include .env`; an environment variable
  does not).
* **`signalflow replay defect --database <lab>` was refused.** The CLI's `--database` replaced `PGDATABASE`, so the
  guard saw the lab as the environment's protected database. The original database is now remembered and stays
  protected; the lab is usable. Found by running Demo 3; a unit test covers it (`test_lab_guard.py`).

## Test results (real runs, 2026-10-23)

```
ruff check src tests                    -> All checks passed!
mypy src (strict)                       -> Success: no issues found in 76 source files
pytest tests/unit -q                    -> 223 passed in 4.08s
pytest tests/integration -q             -> 36 passed in 298.47s   (32 from Phases 1-5 + 4 Phase 6 benchmark)
GitHub Actions (test, stream, lake)     -> all green on c448e7a; tests/stream and tests/lake run there
```

| Phase 6 test | What it proves |
|------|----------------|
| `tests/integration/test_phase6_bench.py` (4 tests) | A full benchmark run on XS completes with every check passing and measurements from every experiment, records the same result in `ops.benchmark_runs` and the JSON file, and leaves no bench database or data behind. |
| `test_compiled_and_reference_validators_decide_identically` | Both validators accept and reject the same records over 1,930 crafted variants of every contract sample. |
| `test_contracts_use_only_keywords_both_validators_agree_on` | The contracts use only keywords with the same meaning in draft-07 and 2020-12. |
| `test_valid_events_pass_contracts_and_invalid_fail_as_expected` (both validators) | Every generated valid event passes under both validators. |
| `test_bench_units.py` | Weighted percentiles, latency attribution by Bronze id range, the bench database name guard, workload sizes grow. |
| `test_lab_guard.py` | The lab guard still protects the environment's database after `--database` switches to a lab. |

## Demonstration results

All three scripts were run on the build machine on 2026-10-23.

* **Demo 1** (`PACE=0.5`, a fresh `signalflow_live`): 157 arrival files, 37,986 lines, 37,799 Silver events; the
  Leak Hunter ran after each file (157 runs: 305 cases opened, 263 cleared by data); final `dbt build` PASS=102,
  no warnings or errors; 4 min 4 s. At the default pace of 2 s per file it takes about 8 minutes. A second feed at
  `PACE=1` ended with the same Silver count and the same case counts (305 opened, 263 cleared), as the event-time
  clock requires. The control tower was screenshotted during that feed: 8,033 then 17,685 Bronze records, revenue days
  filling in and moving from Provisional to Restated as late events arrived.
* **Demo 2** (against `signalflow_live`, analyst `nadia`): the most severe confirmed TEL-01 case was opened with its
  evidence and history, then noted, assigned and resolved as RECOVERED, each step audited. The offline evaluation
  passed: precision 1.0 for every rule that confirmed a case, no false confirmation, and none of the 14 delayed
  billing and 7 delayed dealer payments confirmed. Recall is below 1 for several rules because the 6-day feed ends
  before some positives pass their late-data horizon; they are still open, as designed.
* **Demo 3** (operator `tanvir`): Incident D passed 7 of 7 checks, Incident B 8 of 8. The Time Machine step inflated
  20 recharges and rebuilt Gold from them: the day's gross recharge read BDT 66,680. The replay preview found exactly
  the 20 changed records (3,737 unchanged, 25 without a Silver row of their own), its three verifications passed,
  and the promotion brought Gold back to BDT 48,500, equal to the Silver sum. 47 s in total.

## Limitations (honest list)

* **Only the lite path is benchmarked.** Kafka, Debezium, Spark and the lake are covered by failure and correctness
  tests, not by throughput measurements.
* **One machine, one run per workload.** No repetitions, no confidence intervals, no other hardware. The numbers
  belong to this 4-vCPU container.
* **The largest workload is 1.0M lines and 1.9 GB of database.** Nothing larger was run, and nothing is claimed
  beyond it.
* **The processor is one serial process.** Parallel processing (partitioned checkpoint) and pipelined batches are
  described as next steps, not implemented.
* **Gold is rebuilt in full.** Incremental models are not built.
* **Load was tested at 50% and 90% of capacity for up to 60 s.** Sustained or overload behaviour was not measured.
* **Latency is measured inside Postgres,** from Bronze commit to Silver commit. The time an event spends before
  reaching Bronze (the simulated source) is not part of it.
* **The control tower's live refresh polls every 15 s.** It is a Streamlit page, not a streaming dashboard.
* **The Benchmark Lab shows recorded runs.** It does not start benchmarks itself; that is a CLI step (`make bench`).

## The optional AI investigator

Not built. The master plan allowed it only if the core system was correct and complete. The core is complete and its
claims are tested, but every number and every case decision in the platform is deterministic and checkable, and an
investigator that explains cases or drafts incident summaries would add a component whose output cannot be tested
the same way. The interview guide describes where it would fit (on top of the existing evidence, never deciding a
case).

## Demo instructions

See [DEMO.md](../DEMO.md) for all three demonstrations and the Benchmark Lab. In short:

```bash
make bench WORKLOAD=S                                    # ~4.5 min on the build machine; recorded in ops.benchmark_runs
python -m signalflow_bench.report                        # Markdown tables from the latest run per workload
scripts/demo/demo1_live_operations.sh                    # terminal 1
make ui PGDATABASE=signalflow_live                       # terminal 2, then turn on Live refresh
ANALYST=nadia DATA=data/seed7-12d scripts/demo/demo2_revenue_leak.sh
OPERATOR=tanvir scripts/demo/demo3_failure_recovery.sh
make ui                                                  # Platform → Benchmark Lab
```
