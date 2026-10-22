"""`signalflow` command line. Database settings come from PG* environment variables."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

from .db import connect, migrate
from .generator import GeneratorConfig, generate


def _cmd_generate(args: argparse.Namespace) -> dict[str, Any]:
    cfg = GeneratorConfig(seed=args.seed, start_date=date.fromisoformat(args.start), days=args.days,
                          subscribers=args.subscribers, dealers=args.dealers, warehouses=args.warehouses)
    return generate(cfg, Path(args.out), overwrite=args.overwrite)


def _cmd_migrate(_: argparse.Namespace) -> dict[str, Any]:
    with connect() as conn:
        return {"applied": migrate(conn)}


def _cmd_run(args: argparse.Namespace) -> dict[str, Any]:
    from .pipeline import ingest_and_process, prepare, run_dbt

    data_dir = Path(args.data)
    with connect() as conn:
        prepare(conn, data_dir)
        summary = ingest_and_process(conn, data_dir, chunk_size=args.chunk_size)
    result: dict[str, Any] = {"files": summary.files, "lines": summary.lines,
                              "bronze_inserted": summary.bronze_inserted, "silver_accepted": summary.accepted}
    if not args.skip_dbt:
        proc = run_dbt("build")
        result["dbt_returncode"] = proc.returncode
        tail = proc.stdout.strip().splitlines()[-3:]
        result["dbt_summary"] = tail
        if proc.returncode != 0:
            sys.stderr.write(proc.stdout)
    return result


def _cmd_status(_: argparse.Namespace) -> dict[str, Any]:
    from .processing.silver import pending_rows

    with connect() as conn:
        return {
            "bronze_rows": conn.execute("SELECT count(*) FROM bronze.raw_events").fetchone()[0],  # type: ignore[index]
            "pending_rows": pending_rows(conn),
            "silver_events": conn.execute("SELECT count(*) FROM silver.events").fetchone()[0],  # type: ignore[index]
            "quarantine_by_reason": dict(conn.execute(
                "SELECT reason_code, count(*) FROM silver.quarantine GROUP BY 1 ORDER BY 1").fetchall()),
            "duplicates": conn.execute("SELECT count(*) FROM ops.duplicate_events").fetchone()[0],  # type: ignore[index]
            "streams": [
                {"stream": s, "max_event_time": str(m), "watermark": str(w)}
                for s, m, w in conn.execute("SELECT stream, max_event_time, watermark FROM ops.stream_state "
                                            "ORDER BY stream")
            ],
        }


def _cmd_stream(args: argparse.Namespace) -> dict[str, Any]:
    from . import CONTRACTS_DIR
    from .contracts import ContractRegistry
    from .streaming import admin
    from .streaming.settings import StreamSettings

    settings = StreamSettings.from_env()
    if args.action == "init":
        return admin.init(settings, Path(args.data))
    if args.action == "status":
        return admin.status(settings)
    if args.action == "replay":
        from .streaming.replay import replay

        stats = replay(Path(args.data), settings, ContractRegistry.load(CONTRACTS_DIR), pace_ms=args.pace_ms)
        return {"files": stats.files, "telecom_messages": stats.telecom_messages, "erp_events": stats.erp_events,
                "unparseable": stats.unparseable, "by_topic": dict(sorted(stats.by_topic.items()))}
    if args.action == "storm":
        from .streaming.replay import duplicate_storm

        return {"messages_resent": duplicate_storm(Path(args.data), settings, ContractRegistry.load(CONTRACTS_DIR),
                                                   copies=args.copies)}
    if args.action == "silver":
        from .streaming import spark_silver

        argv = ["--trigger-seconds", str(args.trigger_seconds)] + (["--available-now"] if args.available_now else [])
        return {"exit_code": spark_silver.main(argv)}
    if args.action == "transform":
        from .pipeline import run_dbt

        proc = run_dbt("build", database=settings.stream_db)
        if proc.returncode != 0:
            sys.stderr.write(proc.stdout)
        return {"dbt_returncode": proc.returncode, "dbt_summary": proc.stdout.strip().splitlines()[-3:]}
    if args.action == "teardown":
        if args.drop_databases and not args.yes:
            raise SystemExit("--drop-databases deletes the streaming and ERP databases; add --yes to confirm")
        return admin.teardown(settings, drop_databases=args.drop_databases)
    raise SystemExit(f"unknown stream action {args.action}")


def _cmd_lake(args: argparse.Namespace) -> dict[str, Any]:
    import os

    from .lakehouse import admin
    from .lakehouse.settings import LakeSettings

    settings = LakeSettings.from_env()
    database = args.database or os.environ.get("PGDATABASE", "signalflow")
    if args.action in ("publish", "maintain"):
        from dataclasses import asdict

        from .lakehouse import publish as pub

        pub.ensure_catalog(settings)
        spark = pub.build_session(settings, args.master)
        spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
        try:
            if args.action == "publish":
                results = pub.publish(spark, settings, database)
                return {"source_db": database, "tables": [asdict(r) for r in results],
                        "rows_written": sum(r.rows_written for r in results)}
            return {"maintained": admin.maintain(spark, settings, retain_last=args.retain_last)}
        finally:
            spark.stop()
    if args.action == "transform":
        proc = admin.run_dbt_lake("build", settings=settings)
        if proc.returncode != 0:
            sys.stderr.write(proc.stdout)
        return {"dbt_returncode": proc.returncode, "dbt_summary": proc.stdout.strip().splitlines()[-3:]}
    if args.action == "transform-postgres":
        from .pipeline import run_dbt

        proc = run_dbt("build", database=database)
        if proc.returncode != 0:
            sys.stderr.write(proc.stdout)
        return {"dbt_returncode": proc.returncode, "dbt_summary": proc.stdout.strip().splitlines()[-3:]}
    if args.action == "compare":
        from .lakehouse.compare import compare_gold, record_check

        diffs = compare_gold(settings, database)
        record_check(database, diffs)
        return {"source_db": database, "identical": all(d.identical for d in diffs),
                "exit_code": 0 if all(d.identical for d in diffs) else 1,
                "tables": {d.table: {"rows_postgres": d.rows_postgres, "rows_lake": d.rows_lake,
                                     "identical": d.identical, "only_in_postgres": d.only_in_postgres,
                                     "only_in_lake": d.only_in_lake} for d in diffs}}
    if args.action == "status":
        return admin.status(settings)
    if args.action == "drop":
        if not args.yes:
            raise SystemExit("drop deletes every table under SIGNALFLOW_LAKE_PREFIX; add --yes to confirm")
        return {"dropped_namespaces": admin.drop_namespaces(settings)}
    raise SystemExit(f"unknown lake action {args.action}")


def _cmd_bi(args: argparse.Namespace) -> dict[str, Any]:
    import os

    from .bi.superset import bootstrap

    return bootstrap(args.url, lake_prefix=os.environ.get("SIGNALFLOW_LAKE_PREFIX", ""))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="signalflow")
    sub = parser.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="generate a deterministic synthetic dataset")
    g.add_argument("--out", required=True)
    g.add_argument("--seed", type=int, default=7)
    g.add_argument("--start", default="2026-10-01", help="first business day (Asia/Dhaka)")
    g.add_argument("--days", type=int, default=3)
    g.add_argument("--subscribers", type=int, default=2000)
    g.add_argument("--dealers", type=int, default=150)
    g.add_argument("--warehouses", type=int, default=4)
    g.add_argument("--overwrite", action="store_true")
    g.set_defaults(func=_cmd_generate)

    m = sub.add_parser("migrate", help="apply database migrations")
    m.set_defaults(func=_cmd_migrate)

    r = sub.add_parser("run", help="ingest + process all arrival files, then dbt build")
    r.add_argument("--data", required=True)
    r.add_argument("--chunk-size", type=int, default=5000)
    r.add_argument("--skip-dbt", action="store_true")
    r.set_defaults(func=_cmd_run)

    s = sub.add_parser("status", help="show pipeline state")
    s.set_defaults(func=_cmd_status)

    st = sub.add_parser("stream", help="streaming profile (Kafka, Debezium, Spark)")
    st.add_argument("action", choices=["init", "replay", "silver", "storm", "transform", "status", "teardown"])
    st.add_argument("--data", default="data/seed7")
    st.add_argument("--pace-ms", type=int, default=0, help="replay: pause between arrival files")
    st.add_argument("--copies", type=int, default=3, help="storm: extra copies of each recharge event")
    st.add_argument("--trigger-seconds", type=float, default=2.0)
    st.add_argument("--available-now", action="store_true", help="silver: drain what is in Kafka, then stop")
    st.add_argument("--drop-databases", action="store_true")
    st.add_argument("--yes", action="store_true")
    st.set_defaults(func=_cmd_stream)

    lk = sub.add_parser("lake", help="lakehouse (Iceberg on S3-compatible storage, Trino, dbt-trino)")
    lk.add_argument("action", choices=["publish", "transform", "transform-postgres", "compare", "status",
                                       "maintain", "drop"])
    lk.add_argument("--database", help="source Postgres database (default PGDATABASE)")
    lk.add_argument("--master", default="local[2]", help="Spark master for publish/maintain")
    lk.add_argument("--retain-last", type=int, default=50, help="maintain: snapshots always kept per table")
    lk.add_argument("--yes", action="store_true")
    lk.set_defaults(func=_cmd_lake)

    bi = sub.add_parser("bi", help="Superset assets (Trino connection, datasets, charts, dashboard)")
    bi.add_argument("action", choices=["bootstrap"])
    bi.add_argument("--url", help="Superset base URL (default SIGNALFLOW_SUPERSET_URL or http://127.0.0.1:58088)")
    bi.set_defaults(func=_cmd_bi)

    args = parser.parse_args(argv)
    result = args.func(args)
    print(json.dumps(result, indent=2, default=str))
    if isinstance(result, dict):
        return int(result.get("dbt_returncode", 0)) or int(result.get("exit_code", 0))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
