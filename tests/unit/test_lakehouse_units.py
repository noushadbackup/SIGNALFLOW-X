"""Lakehouse pieces that need no infrastructure: table specs, value normalisation for the differential test."""

from __future__ import annotations

import re
import uuid
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from signalflow.lakehouse.compare import normalise
from signalflow.lakehouse.tables import TABLES, by_key


def selected_columns(select: str) -> list[str]:
    body = re.search(r"SELECT(.*?)FROM", select, re.S | re.I).group(1)  # type: ignore[union-attr]
    cols = []
    for part in re.split(r",(?![^()]*\))", body):
        part = part.strip()
        alias = re.search(r"\bAS\s+(\w+)$", part, re.I)
        cols.append(alias.group(1) if alias else part.split(".")[-1])
    return cols


def test_every_append_table_selects_its_high_water_mark_column() -> None:
    for spec in TABLES:
        cols = selected_columns(spec.select)
        assert len(cols) == len(set(cols)), spec.key
        if spec.mode == "append":
            assert spec.hwm in cols, spec.key
        else:
            assert spec.hwm is None, spec.key
        for c in spec.sort_by:
            assert c in cols, (spec.key, c)
        if spec.partition_days:
            assert spec.partition_days in cols


def test_lake_types_avoid_postgres_only_types() -> None:
    # jsonb and uuid have no Iceberg/Spark equivalent: they must be published as text.
    events = by_key("silver.events").select
    assert "payload::text AS payload" in events and "event_id::text AS event_id" in events
    assert "allowed_lateness_seconds" in by_key("ops.stream_state").select


def test_table_keys_are_unique() -> None:
    assert len({t.key for t in TABLES}) == len(TABLES)
    with pytest.raises(KeyError):
        by_key("silver.nope")


def test_normalise_makes_engine_representations_comparable() -> None:
    u = uuid.uuid4()
    assert normalise(u) == normalise(str(u))
    assert normalise(Decimal("1.50")) == normalise(Decimal("1.500000000000")) == normalise(1.5)
    assert normalise(Decimal("0E-12")) == normalise(0)
    dhaka = timezone(timedelta(hours=6))
    assert normalise(datetime(2026, 10, 1, 6, 0, tzinfo=dhaka)) == normalise(datetime(2026, 10, 1, 0, 0, tzinfo=UTC))
    assert normalise(date(2026, 10, 1)) == "2026-10-01"
    assert normalise(True) is True and normalise(None) is None
    with pytest.raises(ValueError):
        normalise(datetime(2026, 10, 1))  # naive timestamps in Gold are a bug, not something to paper over
