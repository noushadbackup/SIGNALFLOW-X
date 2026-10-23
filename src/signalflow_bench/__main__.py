"""python -m signalflow_bench run --workload S | list"""

from __future__ import annotations

import argparse
import json
import os
import sys

os.environ.setdefault("SIGNALFLOW_LOG_LEVEL", "WARNING")  # per-batch INFO lines would bury the progress output

import psycopg  # noqa: E402

from .runner import DEFAULT_PARAMS, EXPERIMENTS, list_runs, run  # noqa: E402
from .workloads import WORKLOADS  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="signalflow-bench")
    sub = parser.add_subparsers(dest="command", required=True)
    p_run = sub.add_parser("run", help="run the experiments on one workload")
    p_run.add_argument("--workload", choices=sorted(WORKLOADS), default="S")
    p_run.add_argument("--only", help="comma-separated experiments: " + ",".join(n for n, _ in EXPERIMENTS))
    p_run.add_argument("--param", action="append", default=[], metavar="KEY=JSON",
                       help="override a parameter, e.g. chunks=[1000,5000] (defaults: "
                            + json.dumps(DEFAULT_PARAMS) + ")")
    p_run.add_argument("--keep-data", action="store_true", help="keep the generated workload under data/bench")
    p_run.add_argument("--no-record", action="store_true", help="do not write to ops.benchmark_runs")
    p_list = sub.add_parser("list", help="recorded runs")
    p_list.add_argument("--limit", type=int, default=20)
    args = parser.parse_args(argv)

    if args.command == "list":
        with psycopg.connect(autocommit=True) as conn:
            for r in list_runs(conn, args.limit):
                print(json.dumps(r, default=str))
        return 0
    params = {}
    for item in args.param:
        key, _, value = item.partition("=")
        if key not in DEFAULT_PARAMS:
            parser.error(f"unknown parameter {key!r}")
        params[key] = json.loads(value)
    only = set(args.only.split(",")) if args.only else None
    result = run(args.workload, only=only, params=params, keep_data=args.keep_data, record=not args.no_record)
    print(json.dumps(result, indent=2, default=str))
    return 0 if result["status"] == "COMPLETED" else 1


if __name__ == "__main__":
    sys.exit(main())
