"""Keep the Iceberg copies of Silver in step with a promoted (or undone) replay, through Trino.

The lake publisher appends by high-water mark, so it never sees rows a replay replaced, removed or inserted below
the mark. After a promotion, ``sync`` makes the lake rows for exactly the touched keys equal to Postgres: one
DELETE and one INSERT per table, i.e. two Iceberg snapshots, whose ids are recorded on the replay.

``undo_lake`` uses Iceberg itself when it can: if a table's current snapshot is still the one the sync produced,
it rolls the table back to the snapshot before the sync (``rollback_to_snapshot``). If anything was committed to
the table since (a later publish), rolling back would discard that, so it re-syncs the keys from Postgres instead.
Either way the result is verified row by row against Postgres.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from ..lakehouse import trino_sql
from ..lakehouse.settings import CATALOG, LakeSettings
from ..lakehouse.tables import by_key
from .replay import ReplayError, get

# replay target table -> key column (the same name in Postgres and in the lake)
TARGETS = {"silver.events": "event_id", "silver.quarantine": "bronze_id", "ops.duplicate_events": "bronze_id",
           "ops.late_events": "event_id"}
CHUNK = 200


def _ident(settings: LakeSettings, target: str) -> str:
    layer, table = target.split(".")
    return f'{CATALOG}.{settings.namespace(layer)}."{table}"'


def _snapshot(settings: LakeSettings, target: str) -> int | None:
    """The table's current snapshot: what the ``main`` branch points at. Not the newest entry of ``$snapshots`` or
    ``$history``: a rollback makes an older snapshot current without creating or logging a new one."""
    layer, table = target.split(".")
    _, rows = trino_sql.query(settings, f'SELECT snapshot_id FROM {CATALOG}.{settings.namespace(layer)}."{table}$refs" '
                                        "WHERE name = 'main'")
    return int(rows[0][0]) if rows else None


def _literal(value: Any, trino_type: str) -> str:
    if value is None:
        return "NULL"
    if trino_type.startswith("timestamp"):
        ts = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        return f"TIMESTAMP '{ts.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S.%f')} UTC'"
    if trino_type in ("bigint", "integer", "smallint"):
        return str(int(value))
    if trino_type.startswith("decimal"):
        return f"DECIMAL '{Decimal(str(value))}'"
    if trino_type in ("double", "real"):
        return repr(float(value))
    if trino_type == "boolean":
        return "true" if value else "false"
    if trino_type == "date":
        return f"DATE '{value}'"
    return "'" + str(value).replace("'", "''") + "'"


def _key_list(keys: list[str], key_type: str) -> str:
    return ", ".join(_literal(k, key_type) for k in keys)


def _columns(settings: LakeSettings, target: str) -> list[tuple[str, str]]:
    _, rows = trino_sql.query(settings, f"DESCRIBE {_ident(settings, target)}")
    return [(r[0], r[1]) for r in rows]


def _pg_rows(conn: psycopg.Connection, target: str, keys: list[str]) -> list[tuple[Any, ...]]:
    spec = by_key(target)
    key = TARGETS[target]
    return [tuple(r) for r in conn.execute(
        f"SELECT * FROM ({spec.select}) s WHERE s.{key}::text = ANY(%s)", (keys,)).fetchall()]


def _lake_rows(settings: LakeSettings, target: str, keys: list[str], cols: list[tuple[str, str]]
               ) -> list[tuple[Any, ...]]:
    key = TARGETS[target]
    key_type = dict(cols)[key]
    out: list[tuple[Any, ...]] = []
    for i in range(0, len(keys), CHUNK):
        _, rows = trino_sql.query(settings, f"SELECT {', '.join(c for c, _ in cols)} FROM {_ident(settings, target)} "
                                            f"WHERE {key} IN ({_key_list(keys[i:i + CHUNK], key_type)})")
        out.extend(rows)
    return out


def _canon(row: tuple[Any, ...]) -> tuple[str, ...]:
    def one(v: Any) -> str:
        if isinstance(v, datetime):
            return v.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")
        if isinstance(v, Decimal):
            return str(v.normalize())
        return "" if v is None else str(v)
    return tuple(one(v) for v in row)


def touched_keys(conn: psycopg.Connection, replay_id: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {t: [] for t in TARGETS}
    for target, key in conn.execute("SELECT target, key FROM ops.replay_archive WHERE replay_id = %s ORDER BY 1, 2",
                                    (replay_id,)):
        out[target].append(key)
    return out


def _resync(settings: LakeSettings, conn: psycopg.Connection, target: str, keys: list[str]) -> dict[str, Any]:
    cols = _columns(settings, target)
    key = TARGETS[target]
    key_type = dict(cols)[key]
    before = _snapshot(settings, target)
    for i in range(0, len(keys), CHUNK):
        trino_sql.query(settings, f"DELETE FROM {_ident(settings, target)} "
                                  f"WHERE {key} IN ({_key_list(keys[i:i + CHUNK], key_type)})")
    rows = _pg_rows(conn, target, keys)
    for i in range(0, len(rows), CHUNK):
        values = ", ".join("(" + ", ".join(_literal(v, t) for v, (_, t) in zip(r, cols, strict=True)) + ")"
                           for r in rows[i:i + CHUNK])
        trino_sql.query(settings, f"INSERT INTO {_ident(settings, target)} ({', '.join(c for c, _ in cols)}) "
                                  f"VALUES {values}")
    return {"before": before, "after": _snapshot(settings, target), "keys": len(keys), "rows_written": len(rows)}


def verify(settings: LakeSettings, conn: psycopg.Connection, replay_id: str) -> dict[str, bool]:
    """Per table: do the lake rows for the touched keys equal the Postgres rows?"""
    out = {}
    for target, keys in touched_keys(conn, replay_id).items():
        if not keys:
            continue
        cols = _columns(settings, target)
        out[target] = sorted(map(_canon, _lake_rows(settings, target, keys, cols))) == \
            sorted(map(_canon, _pg_rows(conn, target, keys)))
    return out


def sync(settings: LakeSettings, conn: psycopg.Connection, replay_id: str) -> dict[str, Any]:
    """After ``promote``: make the lake rows for every touched key equal Postgres, and record the snapshots."""
    run = get(conn, replay_id)
    if run["status"] != "PROMOTED":
        raise ReplayError(f"{replay_id} is {run['status']}; sync the lake after a promotion")
    tables = {t: _resync(settings, conn, t, keys) for t, keys in touched_keys(conn, replay_id).items() if keys}
    result = {"promote": tables, "verified_after_promote": verify(settings, conn, replay_id)}
    conn.execute("UPDATE ops.replay_runs SET lake = lake || %s WHERE replay_id = %s", (Jsonb(result), replay_id))
    return result


def undo_lake(settings: LakeSettings, conn: psycopg.Connection, replay_id: str) -> dict[str, Any]:
    """After ``undo``: roll each table back to its pre-sync snapshot if nothing was committed since, else re-sync."""
    run = get(conn, replay_id)
    if run["status"] != "UNDONE":
        raise ReplayError(f"{replay_id} is {run['status']}; undo the lake after undoing the replay")
    recorded = (run["lake"] or {}).get("promote", {})
    if not recorded:
        raise ReplayError(f"{replay_id} was never synced to the lake")
    keys = touched_keys(conn, replay_id)
    actions: dict[str, Any] = {}
    for target, snap in recorded.items():
        current = _snapshot(settings, target)
        if current == snap["after"] and snap["before"] is not None:
            trino_sql.query(settings, f"ALTER TABLE {_ident(settings, target)} "
                                      f"EXECUTE rollback_to_snapshot({int(snap['before'])})")
            actions[target] = {"method": "rollback_to_snapshot", "to": snap["before"], "from": current}
        else:
            actions[target] = {"method": "resync", "reason": "the table has newer snapshots than the sync",
                               **_resync(settings, conn, target, keys[target])}
    result = {"undo": actions, "verified_after_undo": verify(settings, conn, replay_id)}
    conn.execute("UPDATE ops.replay_runs SET lake = lake || %s WHERE replay_id = %s", (Jsonb(result), replay_id))
    return result


def as_of(settings: LakeSettings, target: str, snapshot_id: int, where: str = "") -> list[tuple[Any, ...]]:
    """Iceberg time travel: the table as it was at ``snapshot_id`` (optionally filtered)."""
    _, rows = trino_sql.query(settings, f"SELECT * FROM {_ident(settings, target)} FOR VERSION AS OF {int(snapshot_id)}"
                                        + (f" WHERE {where}" if where else ""))
    return rows
