"""The Silver processor as its own OS process, so Incident D can kill it with SIGKILL mid-work.

Runs the same ``process`` loop as the lite pipeline against PGDATABASE until Bronze is drained.
"""

from __future__ import annotations

import argparse

import psycopg

from .. import CONTRACTS_DIR
from ..contracts import ContractRegistry
from ..processing.silver import process


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="signalflow-chaos-worker")
    parser.add_argument("--chunk-size", type=int, default=250)
    args = parser.parse_args(argv)
    with psycopg.connect(autocommit=True, application_name="signalflow-chaos-worker") as conn:
        process(conn, ContractRegistry.load(CONTRACTS_DIR), chunk_size=args.chunk_size)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
