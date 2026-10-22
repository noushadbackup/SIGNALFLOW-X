"""Spark Structured Streaming job: Kafka → (validate on executors) → Postgres Bronze + Silver.

Two queries share one SparkSession:
* ``silver_events``: every event topic (``<prefix>.(telecom|mfg).*``). Validation, the CPU-heavy part, runs in
  ``mapInPandas`` on the executors; the validated batch is then committed by ``sink.commit_batch`` in one Postgres
  transaction keyed by the batch id.
* ``master_data``: Debezium table-CDC topics → ``bronze.master_data_changes``.

Delivery semantics: Kafka → Spark is at-least-once (a batch can be re-executed after a crash); the batch-id marker
and Bronze's unique source position make the Postgres result effectively-once. Proven by tests that SIGKILL the job
and that force an exit between our commit and Spark's (tests/stream/).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from .. import CONTRACTS_DIR
from ..contracts import ContractRegistry
from ..log import get_logger
from .settings import StreamSettings
from .sink import KafkaRecord, commit_batch, commit_master_changes
from .topics import events_pattern

log = get_logger(__name__)
EVENTS_QUERY = "silver_events"
MASTER_QUERY = "master_data"

VALIDATED_SCHEMA = (
    "topic string, partition int, offset long, kafka_ts_ms long, arrival_header string, value string, "
    "event_id string, event_type string, schema_version string, event_time string, reason string, details string, "
    "contract_id string"
)
_REGISTRY: ContractRegistry | None = None


def _registry() -> ContractRegistry:
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = ContractRegistry.load(CONTRACTS_DIR)
    return _REGISTRY


def validate_partition(frames: Iterator[Any]) -> Iterator[Any]:
    """Executor side: contract-validate every message of a partition (pandas batches in, pandas batches out)."""
    import pandas as pd

    from ..contracts import parse_utc
    from ..processing.silver import validate_message

    registry = _registry()
    for pdf in frames:
        out = []
        for row in pdf.itertuples(index=False):
            arrival = None
            if isinstance(row.arrival_header, str):
                try:
                    arrival = parse_utc(row.arrival_header)
                except ValueError:
                    arrival = None
            if arrival is None and row.kafka_ts_ms is not None and not pd.isna(row.kafka_ts_ms):
                arrival = datetime.fromtimestamp(int(row.kafka_ts_ms) / 1000, tz=UTC)
            value = row.value if isinstance(row.value, str) else None
            event, reason, details, contract_id = validate_message(registry, value, arrival)

            def text(key: str, ev: Any = event) -> str | None:
                v = ev.get(key) if isinstance(ev, dict) else None
                return v if isinstance(v, str) else None

            out.append((row.topic, int(row.partition), int(row.offset), row.kafka_ts_ms, row.arrival_header, value,
                        text("event_id"), text("event_type"), text("schema_version"), text("event_time"), reason,
                        json.dumps(details), contract_id))
        yield pd.DataFrame(out, columns=[c.split()[0] for c in VALIDATED_SCHEMA.split(", ")])


class _Connections:
    """One autocommit connection per driver, reopened after a failure."""

    def __init__(self, database: str):
        self.database = database
        self._conn: Any = None

    def get(self) -> Any:
        import psycopg

        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(dbname=self.database, autocommit=True, application_name="signalflow-spark")
        return self._conn

    def reset(self) -> None:
        if self._conn is not None:
            self._conn.close()
        self._conn = None


def build_session(settings: StreamSettings, master: str) -> Any:
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    jars = sorted(settings.jars_dir.glob("*.jar"))
    if not jars:
        raise FileNotFoundError(f"no Spark jars in {settings.jars_dir}; run scripts/fetch_artifacts.sh")
    return (SparkSession.builder.appName("signalflow-silver").master(master)
            .config("spark.jars", ",".join(str(j) for j in jars))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.bindAddress", os.environ.get("SPARK_DRIVER_BIND", "127.0.0.1"))
            .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "127.0.0.1"))
            .config("spark.ui.enabled", os.environ.get("SPARK_UI_ENABLED", "false"))
            .config("spark.sql.streaming.minBatchesToRetain", "20")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())


def start(settings: StreamSettings, master: str = "local[2]", trigger_seconds: float = 2.0,
          available_now: bool = False) -> tuple[Any, list[Any]]:
    from pyspark.sql import functions as F

    spark = build_session(settings, master)
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    checkpoints = settings.state_dir / settings.topic_prefix / "checkpoints"
    db = _Connections(settings.stream_db)

    def kafka(pattern: str) -> Any:
        return (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", settings.bootstrap)
                .option("subscribePattern", pattern)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "true")
                .option("includeHeaders", "true")
                .load())

    def events_batch(batch_df: Any, batch_id: int) -> None:
        # One task per Kafka partition (49 topic-partitions) costs more in task overhead than it gains here;
        # coalescing to the available cores keeps validation parallel without the per-task cost.
        parallelism = batch_df.sparkSession.sparkContext.defaultParallelism
        t0 = time.perf_counter()
        rows = batch_df.coalesce(parallelism).mapInPandas(validate_partition, VALIDATED_SCHEMA).collect()
        log.info("batch validated on executors", extra={"batch": batch_id, "rows": len(rows),
                                                       "validate_ms": round((time.perf_counter() - t0) * 1000)})
        records = [KafkaRecord(
            r.topic, r.partition, r.offset,
            datetime.fromtimestamp(r.kafka_ts_ms / 1000, tz=UTC) if r.kafka_ts_ms is not None else None,
            r.arrival_header, r.value, r.event_id, r.event_type, r.schema_version, r.event_time, r.reason,
            r.details, r.contract_id) for r in rows]
        try:
            commit_batch(db.get(), EVENTS_QUERY, batch_id, records)
        except Exception:
            db.reset()
            raise  # fail the query; the supervisor restarts the job and Spark re-executes this batch

    def master_batch(batch_df: Any, _batch_id: int) -> None:
        rows = [{
            "topic": r.topic, "position": f"{r.partition}:{r.offset}", "key": r.key, "value": r.value,
            "op": r.op, "source_ts": datetime.fromtimestamp(r.ts_ms / 1000, tz=UTC) if r.ts_ms else None,
        } for r in batch_df.collect()]
        try:
            commit_master_changes(db.get(), rows)
        except Exception:
            db.reset()
            raise

    header = F.expr("cast(filter(headers, h -> h.key = 'sf_arrival_time')[0].value as string)")
    events = kafka(events_pattern(settings.topic_prefix)).select(
        "topic", "partition", "offset", F.unix_millis("timestamp").alias("kafka_ts_ms"),
        header.alias("arrival_header"), F.col("value").cast("string").alias("value"))
    master = kafka(rf"{re.escape(settings.topic_prefix)}\.erpcdc\..+").select(
        "topic", "partition", "offset", F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.get_json_object(F.col("value").cast("string"), "$.op").alias("op"),
        F.get_json_object(F.col("value").cast("string"), "$.source.ts_ms").cast("long").alias("ts_ms"))

    def writer(df: Any, name: str, fn: Any) -> Any:
        w = df.writeStream.queryName(name).option("checkpointLocation", str(checkpoints / name)).foreachBatch(fn)
        w = w.trigger(availableNow=True) if available_now else w.trigger(processingTime=f"{trigger_seconds} seconds")
        return w.start()

    return spark, [writer(events, EVENTS_QUERY, events_batch), writer(master, MASTER_QUERY, master_batch)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="signalflow-stream-silver")
    parser.add_argument("--master", default=os.environ.get("SPARK_MASTER", "local[2]"))
    parser.add_argument("--trigger-seconds", type=float, default=2.0)
    parser.add_argument("--available-now", action="store_true",
                        help="process everything currently in Kafka, then stop (catch-up / tests)")
    args = parser.parse_args(argv)
    settings = StreamSettings.from_env()
    spark, queries = start(settings, args.master, args.trigger_seconds, args.available_now)
    log.info("streaming queries started", extra={"queries": [q.name for q in queries],
                                                 "available_now": args.available_now})
    try:
        if args.available_now:
            for q in queries:
                q.awaitTermination()
        else:
            spark.streams.awaitAnyTermination()  # a failed query ends the job; the supervisor restarts it
    except Exception as exc:  # StreamingQueryException carries the foreachBatch error
        log.error("streaming query failed", extra={"error": str(exc)[:4000]})
        return 1
    finally:
        for q in queries:
            if q.isActive:
                q.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
