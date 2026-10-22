"""Simulated manufacturer ERP. Each manufacturing event becomes a business-row upsert plus an outbox row, committed
together; Debezium publishes the outbox to Kafka (ADR-0003). Master data lives in plain tables captured by CDC."""

from __future__ import annotations

import csv
import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from . import REPO_ROOT
from .contracts import ContractRegistry
from .log import get_logger
from .streaming.settings import StreamSettings
from .streaming.topics import Router

ERP_DDL = REPO_ROOT / "sql" / "erp" / "001_erp.sql"
MASTER_TABLES = ("dealers", "products", "warehouses")
log = get_logger(__name__)


def ensure_database(name: str) -> bool:
    """Create database ``name`` if missing (uses the PG* connection's default database). Returns True if created."""
    with psycopg.connect(autocommit=True) as admin:
        if admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone():
            return False
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        return True


def connect_db(name: str) -> psycopg.Connection:
    return psycopg.connect(dbname=name, autocommit=True, application_name="signalflow")


def init_erp(settings: StreamSettings, reference_dir: Path) -> None:
    """Idempotent: database, schema, master data, CDC role, publications."""
    password = os.environ.get("SIGNALFLOW_CDC_PASSWORD")
    if not password:
        raise RuntimeError("SIGNALFLOW_CDC_PASSWORD must be set (see .env.example)")
    ensure_database(settings.erp_db)
    with connect_db(settings.erp_db) as conn, conn.transaction():
        conn.execute(ERP_DDL.read_text())
        conn.execute("SELECT pg_advisory_xact_lock(hashtext('signalflow.erp.init'))")
        if conn.execute("SELECT count(*) FROM erp.dealers").fetchone() == (0,):
            for table in MASTER_TABLES:
                with (reference_dir / f"{table}.csv").open(newline="") as fh:
                    rows = list(csv.reader(fh))
                cols = sql.SQL(", ").join(sql.Identifier(c) for c in rows[0])
                with conn.cursor().copy(sql.SQL("COPY erp.{} ({}) FROM STDIN").format(sql.Identifier(table), cols)) \
                        as cp:
                    for r in rows[1:]:
                        cp.write_row(r)
        role = sql.Identifier(settings.cdc_user)
        exists = conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (settings.cdc_user,)).fetchone()
        verb = sql.SQL("ALTER") if exists else sql.SQL("CREATE")
        # Least privilege for Debezium: LOGIN + REPLICATION, read-only on the captured tables.
        conn.execute(sql.SQL("{} ROLE {} WITH LOGIN REPLICATION PASSWORD {}").format(verb, role, sql.Literal(password)))
        conn.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(settings.erp_db), role))
        conn.execute(sql.SQL("GRANT USAGE ON SCHEMA erp TO {}").format(role))
        conn.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA erp TO {}").format(role))
        conn.execute(sql.SQL("GRANT INSERT, UPDATE ON erp.cdc_heartbeat TO {}").format(role))
        for pub, tables in publications(settings).items():
            if not conn.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (pub,)).fetchone():
                conn.execute(sql.SQL("CREATE PUBLICATION {} FOR TABLE {}").format(
                    sql.Identifier(pub), sql.SQL(", ").join(sql.Identifier("erp", t) for t in tables)))
    log.info("erp initialised", extra={"database": settings.erp_db})


def publications(settings: StreamSettings) -> dict[str, tuple[str, ...]]:
    return {f"sfx_outbox_{settings.slot_suffix}": ("outbox", "cdc_heartbeat"),
            f"sfx_master_{settings.slot_suffix}": MASTER_TABLES}


class ErpApp:
    """Writes manufacturing events the way an ERP would: business row + outbox row in one transaction."""

    def __init__(self, conn: psycopg.Connection, registry: ContractRegistry, router: Router):
        self.conn = conn
        self.registry = registry
        self.router = router

    def record(self, event_text: str, event: Any, arrival_time: str) -> str:
        """Persist one event; returns the outbox ``aggregatetype`` (the topic suffix it will be routed to)."""
        topic_suffix = self.router.route(event, "mfg").split(".", 1)[1]
        event_id = event.get("event_id") if isinstance(event, dict) else None
        with self.conn.transaction():
            if topic_suffix != "mfg.unrouted.v1" and isinstance(event, dict):
                self._upsert_document(event)
            self.conn.execute(
                """INSERT INTO erp.outbox (id, aggregatetype, aggregateid, type, payload, arrival_time)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (uuid.uuid4(), topic_suffix, str(event_id) if event_id else "unknown",
                 str(event.get("event_type")) if isinstance(event, dict) else "unknown", event_text, arrival_time))
        return topic_suffix

    def _upsert_document(self, event: dict[str, Any]) -> None:
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        contract = self.registry.get(str(event.get("event_type")), str(event.get("schema_version")))
        key = contract.business_key if contract else "event_id"
        doc_id = payload.get(key, event.get(key)) if isinstance(payload, dict) else event.get(key)
        if doc_id is None or not isinstance(event.get("event_id"), str):
            return  # a malformed event still goes to the outbox (Silver will quarantine it); no business row
        try:
            event_uuid = uuid.UUID(event["event_id"])
        except ValueError:
            return
        self.conn.execute(
            """INSERT INTO erp.documents (doc_type, doc_id, last_event_id, status, body)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (doc_type, doc_id) DO UPDATE SET last_event_id = EXCLUDED.last_event_id,
                   status = EXCLUDED.status, body = EXCLUDED.body, version = erp.documents.version + 1,
                   updated_at = now()""",
            (str(event["event_type"]).split(".", 1)[1], str(doc_id), event_uuid,
             payload.get("status") if isinstance(payload, dict) else None, json.dumps(payload)))

    def update_dealer_credit_days(self, dealer_id: str, credit_days: int) -> None:
        with self.conn.transaction():
            cur = self.conn.execute("UPDATE erp.dealers SET credit_days = %s, updated_at = now() WHERE dealer_id = %s",
                                    (credit_days, dealer_id))
            if cur.rowcount != 1:
                raise KeyError(dealer_id)


def parse_arrival(arrival_time: str) -> datetime:
    return datetime.fromisoformat(arrival_time.replace("Z", "+00:00"))
