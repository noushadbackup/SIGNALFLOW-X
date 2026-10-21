from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
import pytest


def require_postgres() -> None:
    if os.environ.get("PGHOST"):
        return
    if os.environ.get("CI"):
        pytest.fail("PGHOST must be set in CI; integration tests may not be skipped there")
    pytest.skip("integration tests need PostgreSQL: set PG* variables (see .env.example)")


@contextmanager
def temp_database() -> Iterator[str]:
    """Create an empty database and point PGDATABASE (read by psycopg and dbt) at it; drop it afterwards."""
    require_postgres()
    name = f"sfx_test_{uuid.uuid4().hex[:10]}"
    previous = os.environ.get("PGDATABASE")
    with psycopg.connect(autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{name}"')
    os.environ["PGDATABASE"] = name
    try:
        yield name
    finally:
        if previous is None:
            os.environ.pop("PGDATABASE", None)
        else:
            os.environ["PGDATABASE"] = previous
        with psycopg.connect(autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
