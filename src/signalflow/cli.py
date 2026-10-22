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
    from .assurance.hunter import hunt
    from .pipeline import ingest_and_process, prepare, run_dbt

    data_dir = Path(args.data)
    runs = []
    with connect() as conn:
        prepare(conn, data_dir)
        summary = ingest_and_process(conn, data_dir, chunk_size=args.chunk_size,
                                     after_file=None if args.no_hunt else lambda c: runs.append(hunt(c)))
    result: dict[str, Any] = {"files": summary.files, "lines": summary.lines,
                              "bronze_inserted": summary.bronze_inserted, "silver_accepted": summary.accepted}
    if runs:
        result["leak_hunter"] = {"runs": len(runs), "opened": sum(r.opened for r in runs),
                                 "auto_cleared": sum(r.auto_cleared for r in runs),
                                 "status_changes": sum(r.status_changes for r in runs)}
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


def _use_database(database: str | None) -> None:
    import os

    if database:
        os.environ["PGDATABASE"] = database


def _operator_password(env: str, confirm: bool = False) -> str:
    import getpass
    import os

    if os.environ.get(env):
        return os.environ[env]
    if not sys.stdin.isatty():
        raise SystemExit(f"set {env} or run interactively to enter the password")
    password = getpass.getpass("Password: ")
    if confirm and getpass.getpass("Repeat password: ") != password:
        raise SystemExit("passwords differ")
    return password


def _cmd_assurance(args: argparse.Namespace) -> dict[str, Any]:
    from .assurance import cases
    from .assurance.hunter import hunt
    from .security import authenticate

    _use_database(args.database)
    with connect() as conn:
        migrate(conn)
        if args.action == "hunt":
            return hunt(conn).as_dict()
        if args.action == "cases":
            rows = conn.execute(
                """SELECT case_id, rule_id, status, resolution, business_unit, exposure_bdt, summary
                   FROM assurance.cases WHERE (%(status)s::text IS NULL OR status = %(status)s)
                     AND (%(rule)s::text IS NULL OR rule_id = %(rule)s)
                   ORDER BY anchor_time DESC LIMIT %(limit)s""",
                {"status": args.status, "rule": args.rule, "limit": args.limit}).fetchall()
            keys = ("case_id", "rule_id", "status", "resolution", "business_unit", "exposure_bdt", "summary")
            return {"cases": [dict(zip(keys, r, strict=True)) for r in rows]}
        if args.action == "show":
            case = conn.execute("SELECT row_to_json(c) FROM assurance.cases c WHERE case_id = %s",
                                (args.case,)).fetchone()
            if case is None:
                raise SystemExit(f"unknown case {args.case}")
            history = [r[0] for r in conn.execute(
                "SELECT row_to_json(e) FROM assurance.case_events e WHERE case_id = %s ORDER BY case_event_id",
                (args.case,))]
            return {"case": case[0], "history": history}
        if not args.user:
            raise SystemExit("--user is required for case actions")
        op = authenticate(conn, args.user, _operator_password("SIGNALFLOW_OPERATOR_PASSWORD"))
        if op is None:
            raise SystemExit("authentication failed")
        if args.action == "note":
            cases.add_note(conn, op, args.case, args.note or "")
        elif args.action == "assign":
            cases.assign(conn, op, args.case, args.assignee, args.expected_version)
        elif args.action == "resolve":
            cases.resolve(conn, op, args.case, args.resolution or "", args.note or "", args.expected_version)
        elif args.action == "reopen":
            cases.reopen(conn, op, args.case, args.note or "", args.expected_version)
        return {"case_id": args.case, "action": args.action, "by": op.username}


def _cmd_operators(args: argparse.Namespace) -> dict[str, Any]:
    from .security import add_operator, set_disabled

    _use_database(args.database)
    with connect() as conn:
        migrate(conn)
        if args.action == "add":
            op = add_operator(conn, args.username, args.name or args.username, args.role,
                              _operator_password("SIGNALFLOW_NEW_OPERATOR_PASSWORD", confirm=True))
            return {"username": op.username, "role": op.role}
        if args.action in ("disable", "enable"):
            return {"username": args.username,
                    "updated": set_disabled(conn, args.username, args.action == "disable")}
        rows = conn.execute("SELECT username, display_name, role, disabled, created_at FROM ops.operators "
                            "ORDER BY username").fetchall()
        return {"operators": [dict(zip(("username", "display_name", "role", "disabled", "created_at"), r,
                                       strict=True)) for r in rows]}


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
    r.add_argument("--no-hunt", action="store_true", help="do not run the Leak Hunter after each arrival file")
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

    asr = sub.add_parser("assurance", help="Revenue Leak Hunter: run rules, list and act on cases")
    asr.add_argument("action", choices=["hunt", "cases", "show", "note", "assign", "resolve", "reopen"])
    asr.add_argument("--database", help="Postgres database (default PGDATABASE)")
    asr.add_argument("--case")
    asr.add_argument("--status", choices=["PENDING", "SUSPICIOUS", "CONFIRMED_BY_RULE", "RESOLVED"])
    asr.add_argument("--rule")
    asr.add_argument("--limit", type=int, default=50)
    asr.add_argument("--user", help="operator username (password from SIGNALFLOW_OPERATOR_PASSWORD or a prompt)")
    asr.add_argument("--note")
    asr.add_argument("--assignee")
    asr.add_argument("--resolution")
    asr.add_argument("--expected-version", type=int)
    asr.set_defaults(func=_cmd_assurance)

    ops_ = sub.add_parser("operators", help="control tower operators and roles")
    ops_.add_argument("action", choices=["add", "list", "disable", "enable"])
    ops_.add_argument("username", nargs="?")
    ops_.add_argument("--role", choices=["viewer", "analyst", "operator", "admin"], default="viewer")
    ops_.add_argument("--name", help="display name")
    ops_.add_argument("--database", help="Postgres database (default PGDATABASE)")
    ops_.set_defaults(func=_cmd_operators)

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
