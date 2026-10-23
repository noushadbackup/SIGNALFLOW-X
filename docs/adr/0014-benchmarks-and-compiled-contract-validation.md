# ADR-0014: Benchmarks run the real pipeline and check their own results; contracts are also compiled

**Status:** Accepted 2026-10-23 (Phase 6).

## Context
Phase 6 asks for a benchmark suite that measures throughput, latency percentiles, lag, freshness, resource use,
duplicate and reconciliation correctness, recovery, backfill and query performance; compares at least one baseline
with an optimized implementation; uses several workload sizes; and explains bottlenecks. It must not claim
scalability from the size of generated files. The non-negotiables add: no fake metrics and no untested success
claims.

The first profile of the Silver processor (cProfile over 10,000 records) showed where its time went: about 64% in
`jsonschema` validation, 9% in the Postgres driver and waiting on the server, under 5% each for JSON parsing and
hashing. The worker used about 0.85 of one CPU core while Postgres used about 0.25. The processor was CPU-bound in
Python, in validation.

## Decision

### The suite (`src/signalflow_bench`)
* It is a separate package, like `signalflow_eval`, because it reads the generator's truth manifest. The isolation
  test keeps such reads out of `signalflow`.
* It runs the production code paths: the Bronze loader, the Silver processor (in-process and as its own OS process),
  dbt, the Leak Hunter and the Time Machine. Nothing is re-implemented for the benchmark except the baseline loader
  (one INSERT per line), which exists only to be compared with COPY.
* Every experiment runs in a dedicated database (`signalflow_bench`, `_bronze`, `_work`). The name must contain
  `bench` and must not be any database the platform uses; the suite drops them at the end. Backfills start from a
  file-level copy (`CREATE DATABASE … TEMPLATE … STRATEGY FILE_COPY`) of a database with Bronze loaded, so each
  configuration starts from the same state in a few seconds.
* Every experiment returns checks next to its measurements. A check compares an observed value with the truth
  manifest (Silver event ids, duplicates, quarantine by reason, Gold revenue to the poisha, stock per position,
  injected settlement mismatches, the Leak Hunter's offline evaluation) or with another run of the same data (the
  Silver fingerprint of the continuous run). A run with any failed check is recorded as FAILED.
* Latency under load is measured per row from two Postgres clocks: the commit time of the Bronze load that wrote the
  row and the `clock_timestamp()` the processor records when it finishes the batch that read it. Arrivals are paced at
  a share of the capacity measured in the same run.
* Results go to `ops.benchmark_runs` (the Benchmark Lab page reads it) and to `benchmarks/results/<run_id>.json`,
  with the machine description: CPUs, memory, Postgres version and settings, git commit, which containers were
  running, and the load average at the start.

### Compiled validation
* `ContractRegistry` also compiles every contract with fastjsonschema (draft-07 semantics). A record the compiled
  check accepts is accepted. A record it rejects is validated again by jsonschema (2020-12), which produces the
  reasons and decides. The compiled code can therefore make validation faster, never stricter.
* The contracts use only keywords whose meaning is the same in draft-07 and 2020-12. A unit test pins that list, and
  another runs both validators on 1,930 variants of every contract sample (fields removed, nulled, given values that
  lax and strict validators treat differently) and on the generated datasets, and requires identical results.
* `SIGNALFLOW_VALIDATOR=reference` switches to jsonschema only. The benchmark uses it as the baseline; nothing else
  does.

## Consequences
* The processor's backfill throughput rose 2.2× (XS) to 2.55× (S) at the same chunk size. Measured figures for all
  workloads are in the Phase 6 report and the Benchmark Lab; they belong to the machine that recorded them.
* After the change, the processor's largest costs are its own Python code (building rows, hashing, lateness
  classification) and the Postgres round trips. The next step would be a second processor, which needs a partitioned
  checkpoint (ADR-0006 assumes one writer).
* The suite covers the lite path. The streaming path (Kafka, Spark) and the lake are not benchmarked; their tests
  cover correctness under failure, not speed.
* fastjsonschema is one more dependency. It is justified by the measured bottleneck, and jsonschema remains the
  reference for every rejection.

## Alternatives considered
* **Synthetic micro-benchmarks only** (validate N events in a loop). Rejected as the main method: they hide the
  database, the transaction and the checkpoint, which are what a reader needs to judge the pipeline. The profile is
  kept as the explanation, not as the result.
* **Validation in parallel processes.** Possible, but the checkpoint and "first arrival wins" deduplication assume
  one writer; compiling the schemas removed most of the cost without changing those semantics.
* **Dropping jsonschema for fastjsonschema entirely.** Rejected: fastjsonschema does not implement 2020-12, and its
  error messages are less precise. Keeping jsonschema as the authority on rejects costs nothing on valid records,
  which are almost all records.
