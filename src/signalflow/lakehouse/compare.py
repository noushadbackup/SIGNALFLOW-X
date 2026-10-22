"""Differential check: Gold built by dbt on Postgres versus Gold built by the same dbt project on Trino/Iceberg.

Every Gold model is compared row by row as a multiset after normalising engine-specific representations (UUID vs
text, numeric scale, time zone of returned timestamps). Totals of every numeric column are reported too, because
they are what a reviewer reads first.
"""

from __future__ import annotations

import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import psycopg

from .settings import CATALOG, LakeSettings
from .trino_sql import query


def normalise(value: Any) -> Any:
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, (int, Decimal, float)):
        d = Decimal(str(value))
        return d.normalize() if d != 0 else Decimal(0)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("naive timestamp in Gold; every timestamp should carry a time zone")
        return value.astimezone(UTC).isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    return str(value)


@dataclass
class TableDiff:
    table: str
    rows_postgres: int
    rows_lake: int
    only_in_postgres: list[tuple[Any, ...]] = field(default_factory=list)
    only_in_lake: list[tuple[Any, ...]] = field(default_factory=list)
    totals_postgres: dict[str, str] = field(default_factory=dict)
    totals_lake: dict[str, str] = field(default_factory=dict)

    @property
    def identical(self) -> bool:
        return not self.only_in_postgres and not self.only_in_lake and self.rows_postgres == self.rows_lake


def _totals(cols: list[str], rows: list[tuple[Any, ...]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for i, c in enumerate(cols):
        vals = [r[i] for r in rows if isinstance(r[i], Decimal)]
        if vals and len(vals) == sum(1 for r in rows if r[i] is not None):
            out[c] = str(sum(vals, Decimal(0)).normalize())
    return out


def gold_tables(pg: psycopg.Connection) -> list[str]:
    return [r[0] for r in pg.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'gold' ORDER BY 1").fetchall()]


def compare_gold(settings: LakeSettings, database: str, sample: int = 5) -> list[TableDiff]:
    diffs: list[TableDiff] = []
    with psycopg.connect(dbname=database, autocommit=True) as pg:
        for table in gold_tables(pg):
            cols = [r[0] for r in pg.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = 'gold' AND table_name = %s "
                "ORDER BY ordinal_position", (table,)).fetchall()]
            col_list = ", ".join(f'"{c}"' for c in cols)
            pg_rows = [tuple(normalise(v) for v in r)
                       for r in pg.execute(f'SELECT {col_list} FROM gold."{table}"').fetchall()]
            _, lake_raw = query(settings, f'SELECT {col_list} FROM {CATALOG}.{settings.namespace("gold")}."{table}"')
            lake_rows = [tuple(normalise(v) for v in r) for r in lake_raw]
            a, b = Counter(pg_rows), Counter(lake_rows)
            diffs.append(TableDiff(
                table, len(pg_rows), len(lake_rows),
                sorted((a - b).elements(), key=repr)[:sample], sorted((b - a).elements(), key=repr)[:sample],
                _totals(cols, pg_rows), _totals(cols, lake_rows)))
    return diffs


def record_check(database: str, diffs: list[TableDiff]) -> int:
    """Store one check (all models) in ops.lake_differential_checks for the control tower; returns the check id."""
    with psycopg.connect(dbname=database, autocommit=True) as pg, pg.transaction():
        check_id = int(pg.execute("SELECT nextval('ops.lake_differential_check_seq')").fetchone()[0])  # type: ignore[index]
        with pg.cursor() as cur:
            cur.executemany(
                "INSERT INTO ops.lake_differential_checks (check_id, model, rows_postgres, rows_lake, identical) "
                "VALUES (%s, %s, %s, %s, %s)",
                [(check_id, d.table, d.rows_postgres, d.rows_lake, d.identical) for d in diffs])
    return check_id
