"""Database connection and migrations. Connection settings come only from libpq environment variables
(PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE), so no credentials live in code or config files."""

from __future__ import annotations

import csv
from pathlib import Path

import psycopg

from . import REPO_ROOT
from .log import get_logger

MIGRATIONS_DIR = REPO_ROOT / "sql" / "migrations"
log = get_logger(__name__)

REFERENCE_TABLES = ["regions", "packages", "products", "factories", "warehouses", "dealers"]  # FK order


def connect() -> psycopg.Connection:
    """Autocommit connection: every unit of work opens its own explicit ``conn.transaction()``, which is then a
    real transaction rather than a savepoint inside one long implicit transaction."""
    return psycopg.connect(autocommit=True, application_name="signalflow")


def require_autocommit(conn: psycopg.Connection) -> None:
    if not conn.autocommit:
        raise ValueError("connection must be in autocommit mode so each batch commits independently")


def migrate(conn: psycopg.Connection) -> list[str]:
    applied: list[str] = []
    with conn.transaction():
        conn.execute("CREATE SCHEMA IF NOT EXISTS ops")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ops.schema_migrations "
            "(version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
        )
        conn.execute("SELECT pg_advisory_xact_lock(hashtext('signalflow.migrate'))")
        done = {r[0] for r in conn.execute("SELECT version FROM ops.schema_migrations")}
        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            if path.stem in done:
                continue
            conn.execute(path.read_text())
            conn.execute("INSERT INTO ops.schema_migrations (version) VALUES (%s)", (path.stem,))
            applied.append(path.stem)
    log.info("migrations applied", extra={"applied": applied})
    return applied


def load_reference(conn: psycopg.Connection, reference_dir: Path) -> dict[str, int]:
    """Replace reference snapshots atomically (master data is small; full replace keeps it simple and exact)."""
    counts: dict[str, int] = {}
    with conn.transaction():
        conn.execute("TRUNCATE " + ", ".join(f"ref.{t}" for t in reversed(REFERENCE_TABLES)))
        for table in REFERENCE_TABLES:
            path = reference_dir / f"{table}.csv"
            with path.open(newline="") as fh:
                header = next(csv.reader(fh))
            cols = ", ".join(header)
            with conn.cursor().copy(f"COPY ref.{table} ({cols}) FROM STDIN WITH (FORMAT csv, HEADER true)") as cp:
                cp.write(path.read_bytes())
            row = conn.execute(f"SELECT count(*) FROM ref.{table}").fetchone()
            counts[table] = int(row[0]) if row else 0
    log.info("reference data loaded", extra={"counts": counts})
    return counts
