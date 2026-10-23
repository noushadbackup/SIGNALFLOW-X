"""The Chaos Lab's disposable environment: a lab database built from a deterministic synthetic dataset.

Every incident run rebuilds the lab database from scratch, optionally holding back some records (they arrive later,
as part of the incident). Nothing outside the lab database is modified, and the lab database name is checked so a
misconfiguration cannot point the lab at the demo, streaming, ERP, catalog, Airflow or Superset databases.

A *reference* is what a clean, uninterrupted run of the same dataset produces: the Silver fingerprint (event id and
payload hash of every accepted event) and the Gold figures the incidents touch. Recovery is verified against it.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from .. import REPO_ROOT
from ..assurance.hunter import hunt
from ..generator import GeneratorConfig, generate
from ..ingest.bronze_loader import arrival_files
from ..log import get_logger
from ..pipeline import ingest_and_process, prepare, run_dbt

log = get_logger(__name__)

# The Gold models incidents verify, plus everything upstream of them.
DBT_SELECT = ["+mart_telecom_revenue_daily", "+mart_inventory_position"]
PROTECTED_ENV = ("PGDATABASE", "SIGNALFLOW_STREAM_DB", "SIGNALFLOW_ERP_DB", "SIGNALFLOW_CATALOG_DB",
                 "SIGNALFLOW_LAKE_SOURCE_DB")
PROTECTED_NAMES = {"postgres", "template0", "template1", "signalflow", "signalflow_stream", "signalflow_erp",
                   "signalflow_catalog", "signalflow_airflow", "signalflow_superset"}


@dataclass(frozen=True)
class LabDataset:
    seed: int = 7
    days: int = 3
    subscribers: int = 1000
    dealers: int = 120

    @property
    def key(self) -> str:
        return f"seed{self.seed}-{self.days}d-{self.subscribers}s-{self.dealers}d"

    @property
    def directory(self) -> Path:
        return Path(os.environ.get("SIGNALFLOW_LAB_DATA", REPO_ROOT / "data" / "lab")) / self.key

    def config(self) -> GeneratorConfig:
        return GeneratorConfig(seed=self.seed, days=self.days, subscribers=self.subscribers, dealers=self.dealers)

    def ensure(self) -> Path:
        """Generate the dataset once; the generator is deterministic, so a cached copy is identical."""
        if not (self.directory / "arrivals").is_dir():
            log.info("generating lab dataset", extra={"dataset": self.key})
            generate(self.config(), self.directory, overwrite=True)
        return self.directory


def lab_database_name(name: str | None = None) -> str:
    name = name or os.environ.get("SIGNALFLOW_LAB_DB", "signalflow_lab")
    protected = PROTECTED_NAMES | {os.environ[e] for e in PROTECTED_ENV if os.environ.get(e)}
    if name in protected or "lab" not in name or not name.replace("_", "").isalnum():
        raise ValueError(f"refusing to use {name!r} as the Chaos Lab database: it must contain 'lab' and must not "
                         "be the demo, streaming, ERP, catalog, Airflow or Superset database")
    return name


def recreate_database(name: str) -> None:
    lab_database_name(name)
    with psycopg.connect(dbname="postgres", autocommit=True) as admin:
        admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))


def drop_database(name: str) -> None:
    lab_database_name(name)
    with psycopg.connect(dbname="postgres", autocommit=True) as admin:
        admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))


def lab_connect(name: str) -> psycopg.Connection:
    return psycopg.connect(dbname=lab_database_name(name), autocommit=True, application_name="signalflow-chaos")


@dataclass
class LabBuild:
    database: str
    work_dir: Path
    files_processed: int
    held_back_files: list[Path]
    withheld_lines: list[str]
    seconds_build: float = 0.0


def build(name: str, dataset: LabDataset, work_dir: Path, withhold: Callable[[dict[str, Any]], bool] | None = None,
          process_files: int | None = None, dbt: bool = True) -> LabBuild:
    """Recreate the lab database and run the lite pipeline over the dataset.

    ``withhold(event)`` keeps matching records out of the arrival files (they are returned for the incident to send
    later). ``process_files`` stops after that many arrival files; the rest are returned untouched."""
    import time

    t0 = time.perf_counter()
    source = dataset.ensure()
    if work_dir.exists():
        shutil.rmtree(work_dir)
    (work_dir / "arrivals").mkdir(parents=True)
    files = arrival_files(source / "arrivals")
    withheld: list[str] = []
    staged: list[Path] = []
    for path in files:
        target = work_dir / "arrivals" / path.name
        if withhold is None:
            shutil.copyfile(path, target)
        else:
            kept: list[str] = []
            for line in path.read_text().splitlines():
                try:
                    event = json.loads(line).get("event")
                except ValueError:
                    event = None
                (withheld if isinstance(event, dict) and withhold(event) else kept).append(line)
            target.write_text("".join(f"{line}\n" for line in kept))
        staged.append(target)
    shutil.copytree(source / "reference", work_dir / "reference")
    run_now = staged if process_files is None else staged[:process_files]
    recreate_database(name)
    with lab_connect(name) as conn:
        prepare(conn, work_dir)
        ingest_and_process(conn, work_dir, chunk_size=20_000, files=run_now)
        hunt(conn)  # once at the end: case state does not depend on hunting cadence (Phase 4 test)
    if dbt:
        dbt_lab(name)
    result = LabBuild(name, work_dir, len(run_now), staged[len(run_now):], withheld,
                      round(time.perf_counter() - t0, 2))
    log.info("lab built", extra={"database": name, "files": len(run_now), "withheld": len(withheld),
                                 "held_back_files": len(result.held_back_files), "seconds": result.seconds_build})
    return result


def dbt_lab(name: str) -> None:
    proc = run_dbt("run", ["--select", *DBT_SELECT], database=name)
    if proc.returncode != 0:
        raise RuntimeError("dbt run on the lab database failed:\n" + proc.stdout[-3000:])


SILVER_FINGERPRINT = """SELECT count(*), md5(coalesce(string_agg(event_id::text || ':' || event_sha256, ','
                                                               ORDER BY event_id), ''))
                        FROM silver.events WHERE NOT (event_id = ANY(%s::uuid[]))"""


def silver_fingerprint(conn: psycopg.Connection, exclude: list[str] | None = None) -> dict[str, Any]:
    n, digest = conn.execute(SILVER_FINGERPRINT, (exclude or [],)).fetchone()  # type: ignore[misc]
    return {"events": int(n), "md5": str(digest)}


def gold_figures(conn: psycopg.Connection) -> dict[str, Any]:
    """The Gold numbers incidents must leave correct: recharge revenue per day and channel, stock per position."""
    revenue = {f"{d}|{ch}": {"gross_bdt": str(g), "successful": int(s)} for d, ch, g, s in conn.execute(
        """SELECT business_date, channel, sum(gross_recharge_bdt), sum(successful_recharges)
           FROM gold.mart_telecom_revenue_daily GROUP BY 1, 2 ORDER BY 1, 2""")}
    stock = {f"{w}|{s}": int(q) for w, s, q in conn.execute(
        "SELECT warehouse_id, sku, on_hand_qty FROM gold.mart_inventory_position ORDER BY 1, 2")}
    total = conn.execute("SELECT coalesce(sum(gross_recharge_bdt), 0) FROM gold.mart_telecom_revenue_daily"
                         ).fetchone()
    return {"revenue": revenue, "stock": stock, "gross_recharge_bdt": str(total[0]) if total else "0"}


def open_cases(conn: psycopg.Connection) -> dict[str, str]:
    return {c: s for c, s in conn.execute(
        "SELECT case_id, status FROM assurance.cases WHERE status <> 'RESOLVED' ORDER BY case_id")}


def metrics(conn: psycopg.Connection) -> dict[str, Any]:
    return {"silver": silver_fingerprint(conn), "gold": gold_figures(conn), "open_cases": open_cases(conn)}


def ensure_reference(control: psycopg.Connection, dataset: LabDataset, lab_name: str) -> dict[str, Any]:
    """Reference metrics for ``dataset``, built once in a separate lab database and cached in the control DB."""
    row = control.execute("SELECT metrics FROM ops.chaos_references WHERE dataset_key = %s", (dataset.key,)
                          ).fetchone()
    if row:
        return dict(row[0])
    ref_db = lab_database_name(f"{lab_name}_ref")
    work = dataset.directory.parent / "work" / f"{dataset.key}-reference"
    build(ref_db, dataset, work)
    with lab_connect(ref_db) as conn:
        ref = metrics(conn)
    drop_database(ref_db)
    shutil.rmtree(work, ignore_errors=True)
    control.execute(
        """INSERT INTO ops.chaos_references (dataset_key, dataset, metrics) VALUES (%s, %s, %s)
           ON CONFLICT (dataset_key) DO UPDATE SET metrics = EXCLUDED.metrics, created_at = clock_timestamp()""",
        (dataset.key, Jsonb(asdict(dataset)), Jsonb(ref)))
    return ref


def digest(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()[:16]
