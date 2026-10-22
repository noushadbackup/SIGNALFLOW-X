"""Lifecycle of the streaming profile: init (databases, ERP, topics, connectors), status, teardown."""

from __future__ import annotations

import shutil
import time
from pathlib import Path
from typing import Any

from psycopg import sql

from .. import CONTRACTS_DIR
from ..contracts import ContractRegistry
from ..db import load_reference, migrate
from ..erp import connect_db, ensure_database, init_erp, publications
from ..log import get_logger
from .connect import ConnectClient, master_data_connector, outbox_connector
from .settings import StreamSettings
from .topics import MASTER_DATA_TABLES, all_event_topics, create_topics, delete_topics, master_data_topic

log = get_logger(__name__)


def init(settings: StreamSettings, data_dir: Path) -> dict[str, Any]:
    registry = ContractRegistry.load(CONTRACTS_DIR)
    created_db = ensure_database(settings.stream_db)
    with connect_db(settings.stream_db) as conn:
        applied = migrate(conn)
        load_reference(conn, data_dir / "reference")
    init_erp(settings, data_dir / "reference")
    topics = all_event_topics(settings.topic_prefix, registry)
    created = create_topics(settings.bootstrap, topics, settings.partitions)
    created += create_topics(settings.bootstrap,
                             [master_data_topic(settings.topic_prefix, t) for t in MASTER_DATA_TABLES], 1)
    client = ConnectClient(settings.connect_url)
    connectors = {}
    for name, config in (outbox_connector(settings), master_data_connector(settings)):
        client.put_config(name, config)
        connectors[name] = client.wait_running(name)["connector"]["state"]
    summary = {"stream_db_created": created_db, "migrations_applied": applied, "topics_created": len(created),
               "event_topics": len(topics), "connectors": connectors}
    log.info("stream profile initialised", extra=summary)
    return summary


def status(settings: StreamSettings) -> dict[str, Any]:
    client = ConnectClient(settings.connect_url)
    out: dict[str, Any] = {"connectors": {}}
    for name, _ in (outbox_connector(settings), master_data_connector(settings)):
        st = client.status(name)
        out["connectors"][name] = None if st is None else {
            "connector": st["connector"]["state"], "tasks": [t["state"] for t in st["tasks"]]}
    with connect_db(settings.erp_db) as erp:
        out["replication_slots"] = [
            {"slot": s, "active": a, "lag_bytes": int(lag) if lag is not None else None}
            for s, a, lag in erp.execute(
                """SELECT slot_name, active, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
                   FROM pg_replication_slots WHERE slot_name = ANY(%s) ORDER BY 1""", (list(publications(settings)),))]
    with connect_db(settings.stream_db) as conn:
        def one(q: str) -> Any:
            row = conn.execute(q).fetchone()
            return row[0] if row else None
        out["bronze_rows"] = one("SELECT count(*) FROM bronze.raw_events")
        out["silver_events"] = one("SELECT count(*) FROM silver.events")
        out["quarantined"] = one("SELECT count(*) FROM silver.quarantine")
        out["duplicates"] = one("SELECT count(*) FROM ops.duplicate_events")
        out["batches_committed"] = one("SELECT count(*) FROM ops.stream_commits")
        out["replays_skipped"] = one("SELECT count(*) FROM ops.stream_replays")
        out["master_data_changes"] = one("SELECT count(*) FROM bronze.master_data_changes")
    return out


def teardown(settings: StreamSettings, drop_databases: bool = False) -> dict[str, Any]:
    """Remove connectors, replication slots, publications, topics and Spark checkpoints for this topic prefix.
    A replication slot left behind pins WAL forever, so slots are dropped explicitly after the connectors stop."""
    client = ConnectClient(settings.connect_url)
    deleted = [name for name, _ in (outbox_connector(settings), master_data_connector(settings)) if client.delete(name)]
    dropped_slots: list[str] = []
    try:
        with connect_db(settings.erp_db) as erp:
            for slot in publications(settings):
                for _ in range(30):
                    row = erp.execute("SELECT active FROM pg_replication_slots WHERE slot_name = %s",
                                      (slot,)).fetchone()
                    if row is None:
                        break
                    if not row[0]:
                        erp.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
                        dropped_slots.append(slot)
                        break
                    time.sleep(1)
                erp.execute(sql.SQL("DROP PUBLICATION IF EXISTS {}").format(sql.Identifier(slot)))
    except Exception as exc:  # the ERP database may already be gone
        log.warning("erp cleanup skipped", extra={"error": str(exc)[:500]})
    topics = delete_topics(settings.bootstrap, settings.topic_prefix)
    topics += delete_topics(settings.bootstrap, f"__debezium-heartbeat.{settings.topic_prefix}_erp")
    shutil.rmtree(settings.state_dir / settings.topic_prefix, ignore_errors=True)
    if drop_databases:
        import psycopg

        with psycopg.connect(autocommit=True) as admin:
            for db in (settings.stream_db, settings.erp_db):
                admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(db)))
    return {"connectors_deleted": deleted, "slots_dropped": dropped_slots, "topics_deleted": len(topics),
            "databases_dropped": drop_databases}
