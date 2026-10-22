"""Small Trino helpers (catalog `lake`). Used by the CLI, the differential test and the control tower."""

from __future__ import annotations

from typing import Any

from .settings import CATALOG, LakeSettings


def connect(settings: LakeSettings) -> Any:
    import trino

    return trino.dbapi.connect(  # type: ignore[no-untyped-call,unused-ignore]  # untyped only when the lake extra is installed
        host=settings.trino_host, port=settings.trino_port, user="signalflow", catalog=CATALOG, http_scheme="http")


def query(settings: LakeSettings, sql: str) -> tuple[list[str], list[tuple[Any, ...]]]:
    conn = connect(settings)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = [tuple(r) for r in cur.fetchall()]
        cols = [d[0] for d in cur.description or []]
        return cols, rows
    finally:
        conn.close()


def snapshots(settings: LakeSettings, layer: str, table: str) -> list[dict[str, Any]]:
    """Snapshot history of one Iceberg table, oldest first, with the SignalFlow summary properties."""
    ns = settings.namespace(layer)
    cols, rows = query(settings, f"""
        SELECT snapshot_id, parent_id, committed_at, operation,
               element_at(summary, 'added-records') AS added_records,
               element_at(summary, 'total-records') AS total_records,
               element_at(summary, 'signalflow.hwm') AS hwm,
               element_at(summary, 'signalflow.source-db') AS source_db
        FROM {CATALOG}.{ns}."{table}$snapshots" ORDER BY committed_at""")
    return [dict(zip(cols, r, strict=True)) for r in rows]
