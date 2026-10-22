"""Spark job: publish committed Postgres Bronze/Silver/ops/ref state to Iceberg tables (ADR-0009).

Run it as often as you like (Airflow runs it before every dbt build); each run publishes only what was committed
since the previous one, and a run that crashes part-way leaves every table either fully published or untouched.

    python -m signalflow.lakehouse.publish --database signalflow_stream
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from dataclasses import asdict, dataclass
from typing import Any

import psycopg

from ..log import get_logger
from .settings import CATALOG, LakeSettings
from .tables import CONTENT_PROPERTY, HWM_PROPERTY, SOURCE_PROPERTY, TABLES, TableSpec

log = get_logger(__name__)
ROWS_PER_JDBC_PARTITION = 50_000
TABLE_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
}


@dataclass
class TableResult:
    table: str
    mode: str
    rows_written: int
    hwm_before: int | None
    hwm_after: int | None
    snapshot_id: int | None
    skipped: str | None
    seconds: float


def build_session(settings: LakeSettings, master: str = "local[2]", app: str = "signalflow-lake") -> Any:
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    # AWS SDK v2 (2.30+) sends aws-chunked uploads with trailing CRC checksums by default. S3-compatible stores that
    # do not implement that framing (SeaweedFS 3.97 here) store the chunk headers inside the object and every file
    # reads back corrupt. Checksums only when an operation requires them restores plain uploads.
    os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "WHEN_REQUIRED")
    os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "WHEN_REQUIRED")
    jars = sorted(settings.jars_dir.glob("*.jar"))
    if not jars:
        raise FileNotFoundError(f"no lake jars in {settings.jars_dir}; run scripts/fetch_artifacts.sh")
    c = f"spark.sql.catalog.{CATALOG}"
    builder = (SparkSession.builder.appName(app).master(master)
               .config("spark.jars", ",".join(str(j) for j in jars))
               .config("spark.sql.extensions",
                       "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.ui.enabled", os.environ.get("SPARK_UI_ENABLED", "false"))
               .config("spark.ui.showConsoleProgress", "false")
               .config("spark.driver.bindAddress", os.environ.get("SPARK_DRIVER_BIND", "127.0.0.1"))
               .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "127.0.0.1"))
               .config(c, "org.apache.iceberg.spark.SparkCatalog")
               .config(f"{c}.type", "jdbc")
               .config(f"{c}.uri", settings.jdbc_url(settings.catalog_db))
               .config(f"{c}.jdbc.user", os.environ["PGUSER"])
               .config(f"{c}.jdbc.password", os.environ["PGPASSWORD"])
               .config(f"{c}.jdbc.schema-version", "V1")   # adds view support; what Trino's JDBC catalog reads
               .config(f"{c}.warehouse", settings.warehouse)
               .config(f"{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
               .config(f"{c}.s3.endpoint", settings.s3_endpoint)
               .config(f"{c}.s3.path-style-access", "true")
               .config(f"{c}.s3.access-key-id", os.environ["SIGNALFLOW_S3_ACCESS_KEY"])
               .config(f"{c}.s3.secret-access-key", os.environ["SIGNALFLOW_S3_SECRET_KEY"])
               .config(f"{c}.client.region", settings.s3_region)
               .config("spark.redaction.regex", "(?i)secret|password|token|access-key"))
    return builder.getOrCreate()


def ensure_catalog(settings: LakeSettings) -> bool:
    """Create the catalog database if needed. The Iceberg JDBC catalog creates its own tables on first use."""
    with psycopg.connect(dbname="postgres", autocommit=True) as conn:
        if conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (settings.catalog_db,)).fetchone():
            return False
        conn.execute(psycopg.sql.SQL("CREATE DATABASE {}").format(psycopg.sql.Identifier(settings.catalog_db)))
        return True


def ensure_namespaces(spark: Any, settings: LakeSettings) -> None:
    for layer in sorted({t.layer for t in TABLES} | {"gold"}):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{settings.namespace(layer)}")


def _identifier(settings: LakeSettings, spec: TableSpec) -> str:
    return f"{CATALOG}.{settings.namespace(spec.layer)}.{spec.name}"


def last_summary_value(spark: Any, ident: str, key: str) -> str | None:
    """Value of ``key`` in the newest snapshot summary that has it (maintenance snapshots do not carry it)."""
    rows = spark.sql(f"SELECT summary FROM {ident}.snapshots ORDER BY committed_at DESC").collect()
    for r in rows:
        if r.summary and key in r.summary:
            return str(r.summary[key])
    return None


def published_hwm(spark: Any, ident: str, column: str) -> int:
    """High-water mark of an append table = max of the mark column in the table itself. Every row up to it was
    published in the same commit as the row holding it, so the data is its own checkpoint; it survives compaction
    and snapshot expiry, which a value kept only in old snapshot summaries would not."""
    row = spark.sql(f"SELECT max({column}) AS m FROM {ident}").collect()[0]
    return int(row.m) if row.m is not None else 0


def current_snapshot_id(spark: Any, ident: str) -> int | None:
    rows = spark.sql(f"SELECT snapshot_id FROM {ident}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    return int(rows[0].snapshot_id) if rows else None


def _jdbc(spark: Any, settings: LakeSettings, database: str, query: str, num_partitions: int = 1,
          column: str | None = None, lower: int = 0, upper: int = 0) -> Any:
    reader = (spark.read.format("jdbc")
              .option("url", settings.jdbc_url(database))
              .option("user", os.environ["PGUSER"])
              .option("password", os.environ["PGPASSWORD"])
              .option("driver", "org.postgresql.Driver")
              .option("fetchsize", "10000")
              .option("dbtable", f"({query}) src"))
    if column and num_partitions > 1:
        reader = (reader.option("partitionColumn", column).option("lowerBound", str(lower))
                  .option("upperBound", str(upper)).option("numPartitions", str(num_partitions)))
    return reader.load()


def _create_if_missing(spark: Any, ident: str, spec: TableSpec, df: Any) -> bool:
    if spark.catalog.tableExists(ident):
        return False
    from pyspark.sql import functions as F

    writer = df.limit(0).writeTo(ident).using("iceberg")
    for k, v in TABLE_PROPERTIES.items():
        writer = writer.tableProperty(k, v)
    if spec.partition_days:
        writer = writer.partitionedBy(F.partitioning.days(spec.partition_days))
    writer.create()
    if spec.sort_by:
        spark.sql(f"ALTER TABLE {ident} WRITE ORDERED BY {', '.join(spec.sort_by)}")
    log.info("iceberg table created", extra={"table": ident})
    return True


class SourceMismatch(RuntimeError):
    pass


def publish_table(spark: Any, settings: LakeSettings, database: str, spec: TableSpec,
                  conn: psycopg.Connection) -> TableResult:
    t0 = time.perf_counter()
    ident = _identifier(settings, spec)
    if spark.catalog.tableExists(ident):
        # Identity values from two databases overlap, so mixing sources would silently corrupt the table.
        previous = last_summary_value(spark, ident, SOURCE_PROPERTY)
        if previous is not None and previous != database:
            raise SourceMismatch(f"{ident} was published from database {previous!r}, not {database!r}; use another "
                                 "SIGNALFLOW_LAKE_PREFIX or drop the lake namespace first")
    if spec.mode == "append":
        assert spec.hwm
        inner = f"SELECT * FROM ({spec.select}) s"
        exists = spark.catalog.tableExists(ident)
        before = published_hwm(spark, ident, spec.hwm) if exists else 0
        row = conn.execute(f"SELECT max({spec.hwm}), count(*) FILTER (WHERE {spec.hwm} > %s) FROM ({spec.select}) s",
                           (before,)).fetchone()
        upper, pending = (row[0], row[1]) if row else (None, 0)
        if not exists:
            _create_if_missing(spark, ident, spec, _jdbc(spark, settings, database, inner + " WHERE false"))
        if upper is None or upper <= before or pending == 0:
            return TableResult(spec.key, spec.mode, 0, before, before, current_snapshot_id(spark, ident),
                               "no new committed rows", round(time.perf_counter() - t0, 2))
        parts = max(1, min(8, math.ceil(pending / ROWS_PER_JDBC_PARTITION)))
        df = _jdbc(spark, settings, database, f"{inner} WHERE s.{spec.hwm} > {before} AND s.{spec.hwm} <= {upper}",
                   parts, spec.hwm, before + 1, upper + 1)
        (df.writeTo(ident)
         .option(f"snapshot-property.{HWM_PROPERTY}", str(upper))
         .option(f"snapshot-property.{SOURCE_PROPERTY}", database)
         .append())
        return TableResult(spec.key, spec.mode, int(pending), before, int(upper), current_snapshot_id(spark, ident),
                           None, round(time.perf_counter() - t0, 2))

    # replace: whole table in one snapshot, only when the content changed
    order = ", ".join(spec.sort_by) or "1"
    row = conn.execute(f"SELECT md5(coalesce(string_agg(s::text, E'\\n' ORDER BY {order}), '')), count(*) "
                       f"FROM ({spec.select}) s").fetchone()
    digest, rows = (str(row[0]), int(row[1])) if row else ("", 0)
    df = _jdbc(spark, settings, database, spec.select)
    created = _create_if_missing(spark, ident, spec, df)
    if not created and last_summary_value(spark, ident, CONTENT_PROPERTY) == digest:
        return TableResult(spec.key, spec.mode, 0, None, None, current_snapshot_id(spark, ident), "unchanged",
                           round(time.perf_counter() - t0, 2))
    from pyspark.sql import functions as F

    (df.writeTo(ident)
     .option(f"snapshot-property.{CONTENT_PROPERTY}", digest)
     .option(f"snapshot-property.{SOURCE_PROPERTY}", database)
     .overwrite(F.lit(True)))
    return TableResult(spec.key, spec.mode, rows, None, None, current_snapshot_id(spark, ident), None,
                       round(time.perf_counter() - t0, 2))


def _fault_exit_after(table_key: str) -> None:
    """Test hook: die right after ``table_key`` committed, leaving later tables unpublished (crash between tables).
    Active only with SIGNALFLOW_ALLOW_FAULT_INJECTION=1, so a stray variable cannot break a real run."""
    if (os.environ.get("SIGNALFLOW_ALLOW_FAULT_INJECTION") == "1"
            and os.environ.get("SIGNALFLOW_FAULT_EXIT_AFTER_TABLE") == table_key):
        log.warning("fault injection: exiting after table", extra={"table": table_key})
        os._exit(70)


def publish(spark: Any, settings: LakeSettings, database: str, only: list[str] | None = None) -> list[TableResult]:
    ensure_namespaces(spark, settings)
    results: list[TableResult] = []
    with psycopg.connect(dbname=database, autocommit=True, application_name="signalflow-lake") as conn:
        for spec in TABLES:
            if only and spec.key not in only:
                continue
            res = publish_table(spark, settings, database, spec, conn)
            log.info("table published", extra=asdict(res))
            results.append(res)
            _fault_exit_after(spec.key)
            conn.execute(
                """INSERT INTO ops.lake_publish_runs (table_name, mode, rows_written, hwm_before, hwm_after,
                       snapshot_id, skipped, seconds) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (res.table, res.mode, res.rows_written, res.hwm_before, res.hwm_after, res.snapshot_id, res.skipped,
                 res.seconds))
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="signalflow-lake-publish")
    parser.add_argument("--database", default=os.environ.get("PGDATABASE", "signalflow"))
    parser.add_argument("--master", default=os.environ.get("SPARK_MASTER", "local[2]"))
    parser.add_argument("--table", action="append", help="layer.name; repeat to publish a subset")
    args = parser.parse_args(argv)
    settings = LakeSettings.from_env()
    ensure_catalog(settings)
    spark = build_session(settings, args.master)
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    try:
        results = publish(spark, settings, args.database, args.table)
    finally:
        spark.stop()
    print(json.dumps([asdict(r) for r in results], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
