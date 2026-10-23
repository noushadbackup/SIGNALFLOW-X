"""Lake lifecycle: status (from Trino), maintenance (Spark procedures), dbt on Trino, teardown of a namespace."""

from __future__ import annotations

import os
import subprocess
from datetime import UTC, datetime, timedelta
from typing import Any

from ..log import get_logger
from ..pipeline import dbt_executable
from .settings import CATALOG, LakeSettings
from .tables import TABLES
from .trino_sql import query

log = get_logger(__name__)


def status(settings: LakeSettings) -> dict[str, Any]:
    out: dict[str, Any] = {"catalog": CATALOG, "warehouse": settings.warehouse, "tables": {}}
    for spec in TABLES:
        ns = settings.namespace(spec.layer)
        try:
            _, rows = query(settings, f"""
                SELECT (SELECT count(*) FROM {CATALOG}.{ns}."{spec.name}"),
                       count(*), max(committed_at), max_by(snapshot_id, committed_at)
                FROM {CATALOG}.{ns}."{spec.name}$snapshots" """)
        except Exception as exc:  # table not published yet
            out["tables"][spec.key] = {"error": type(exc).__name__}
            continue
        n_rows, n_snap, last, snap = rows[0]
        out["tables"][spec.key] = {"rows": n_rows, "snapshots": n_snap, "current_snapshot": snap,
                                   "last_commit": str(last)}
    _, gold = query(settings, f"""SELECT count(*) FROM {CATALOG}.information_schema.tables
                                  WHERE table_schema = '{settings.namespace("gold")}'""")
    out["gold_relations"] = gold[0][0]
    return out


def maintain(spark: Any, settings: LakeSettings, retain_last: int = 50,
             older_than_days: int = 7) -> list[dict[str, Any]]:
    """Compact small files (each publish run adds files) and expire old snapshots, keeping at least
    ``retain_last`` so the Time Machine can still travel back. Orphan-file removal is deliberately not automated:
    run it only when no writer is active (see docs/adr/0009)."""
    cutoff = (datetime.now(UTC) - timedelta(days=older_than_days)).strftime("%Y-%m-%d %H:%M:%S")
    results = []
    for spec in TABLES:
        ident = f"{settings.namespace(spec.layer)}.{spec.name}"
        if not spark.catalog.tableExists(f"{CATALOG}.{ident}"):
            continue
        rw = spark.sql(f"CALL {CATALOG}.system.rewrite_data_files(table => '{ident}', "
                       f"options => map('min-input-files', '2'))").collect()[0]
        ex = spark.sql(f"CALL {CATALOG}.system.expire_snapshots(table => '{ident}', "
                       f"older_than => TIMESTAMP '{cutoff}', retain_last => {retain_last})").collect()[0]
        res = {"table": spec.key, "rewritten_files": rw.rewritten_data_files_count,
               "added_files": rw.added_data_files_count, "expired_data_files": ex.deleted_data_files_count}
        log.info("table maintained", extra=res)
        results.append(res)
    return results


def run_dbt_lake(command: str = "build", extra: list[str] | None = None,
                 settings: LakeSettings | None = None) -> subprocess.CompletedProcess[str]:
    """dbt on Trino over the lake. Separate target path so lite and lake runs never share compiled state."""
    from ..pipeline import DBT_DIR

    settings = settings or LakeSettings.from_env()
    args = [dbt_executable(), "--no-use-colors", command, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR),
            "--target", "lake", "--target-path", str(DBT_DIR / "target" / f"lake{settings.namespace_prefix}")]
    if settings.namespace_prefix:
        args += ["--vars", f"{{lake_prefix: '{settings.namespace_prefix}'}}"]
    args += extra or []
    env = dict(os.environ, DBT_SEND_ANONYMOUS_USAGE_STATS="false",
               SIGNALFLOW_TRINO_HOST=settings.trino_host, SIGNALFLOW_TRINO_PORT=str(settings.trino_port))
    log.info("dbt (lake) start", extra={"dbt_args": args[1:]})
    proc = subprocess.run(args, capture_output=True, text=True, env=env, cwd=DBT_DIR)
    log.info("dbt (lake) finished", extra={"returncode": proc.returncode})
    return proc


def drop_namespaces(settings: LakeSettings, allow_main_lake: bool = False) -> list[str]:
    """Drop every table, view and namespace under this prefix. The unprefixed lake is refused unless the caller
    asks for it explicitly, so a test that lost its prefix cannot wipe it."""
    if not settings.namespace_prefix and not allow_main_lake:
        raise ValueError("refusing to drop the unprefixed lake; set SIGNALFLOW_LAKE_PREFIX to a test prefix, or pass "
                         "--include-main-lake")
    dropped: list[str] = []
    for layer in sorted({t.layer for t in TABLES} | {"gold"}):
        ns = settings.namespace(layer)
        try:
            _, views = query(settings, f"SELECT table_name FROM {CATALOG}.information_schema.views "
                                       f"WHERE table_schema = '{ns}'")
            for (v,) in views:
                query(settings, f'DROP VIEW IF EXISTS {CATALOG}.{ns}."{v}"')
            _, tables = query(settings, f"SELECT table_name FROM {CATALOG}.information_schema.tables "
                                        f"WHERE table_schema = '{ns}' AND table_type = 'BASE TABLE'")
            for (t,) in tables:
                query(settings, f'DROP TABLE IF EXISTS {CATALOG}.{ns}."{t}"')
            query(settings, f"DROP SCHEMA IF EXISTS {CATALOG}.{ns}")
            dropped.append(ns)
        except Exception as exc:
            log.warning("namespace drop failed", extra={"namespace": ns, "error": str(exc)})
    return dropped
