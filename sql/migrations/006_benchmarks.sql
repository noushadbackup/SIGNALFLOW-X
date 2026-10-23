-- Phase 6: recorded benchmark runs. Written by the benchmark suite (src/signalflow_bench), read by the Benchmark Lab
-- page. Each row is one workload run on one machine; every number in `results` was measured during that run.
CREATE TABLE IF NOT EXISTS ops.benchmark_runs (
    run_id       text        PRIMARY KEY,
    workload     text        NOT NULL,                 -- S, M, L, ... (see signalflow_bench.WORKLOADS)
    config       jsonb       NOT NULL,                 -- generator config and experiment parameters
    environment  jsonb       NOT NULL,                 -- CPU count, memory, versions, git commit
    results      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    status       text        NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
    error        text,
    started_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at  timestamptz
);
