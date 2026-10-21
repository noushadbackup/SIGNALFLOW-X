"""Deterministic synthetic enterprise generator.

Output layout (``out_dir``)::

    arrivals/<YYYYMMDDTHH>.jsonl   transport files, one per UTC arrival hour, lines in arrival order
    reference/*.csv                master data snapshots
    truth/manifest.json            expected pipeline results (tests only)
    truth/anomalies.jsonl          injected scenarios ledger (tests and evaluation only)
    truth/valid_event_ids.txt      ids that must end up in silver

Pipeline code must never read ``truth/``.
"""

from __future__ import annotations

import csv
import hashlib
import json
import random
import shutil
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .config import Disruption, GeneratorConfig
from .core import Emitter, dhaka_midnight_ms, iso_ms
from .manufacturing import ManufacturingSimulator
from .reference import FACTORIES, PACKAGES, PRODUCTS, REGIONS, build_dealers, build_warehouses
from .telecom import TelecomSimulator
from .transport import apply_transport_effects

__all__ = ["Disruption", "GeneratorConfig", "generate"]


def generate(cfg: GeneratorConfig, out_dir: Path, overwrite: bool = False) -> dict[str, Any]:
    if out_dir.exists() and any(out_dir.iterdir()):
        if not overwrite:
            raise FileExistsError(f"{out_dir} is not empty; pass overwrite=True to replace it")
        shutil.rmtree(out_dir)
    rng = random.Random(cfg.seed)
    start_ms = dhaka_midnight_ms(cfg.start_date)
    end_ms = start_ms + cfg.days * 24 * 3600 * 1000
    days = [cfg.start_date + timedelta(days=i) for i in range(cfg.days)]
    em = Emitter(cfg.seed, random.Random(rng.random()), end_ms)

    warehouses = build_warehouses(cfg.warehouses)
    dealers = build_dealers(cfg.dealers, random.Random(rng.random()))
    tel = TelecomSimulator(cfg, em, random.Random(rng.random()))
    mfg = ManufacturingSimulator(cfg, em, random.Random(rng.random()), warehouses, dealers)

    tel.initial_snapshot(start_ms)
    mfg.opening_balances(cfg.start_date)
    for i, day in enumerate(days):
        tel.simulate_day(i, day)
        mfg.simulate_day(i, day)
    tel.settlements(days)
    mfg.inject_end_of_run_anomalies()
    apply_transport_effects(cfg, em, random.Random(rng.random()), start_ms)

    _write_reference(out_dir / "reference", warehouses, dealers)
    files, lines = _write_arrivals(out_dir / "arrivals", em)
    manifest = _write_truth(out_dir / "truth", cfg, em, tel, files, lines, start_ms, end_ms)
    return {"files": len(files), "lines": lines, "unique_valid_events": manifest["unique_valid_events"],
            "anomalies": len(em.ledger)}


def _write_reference(path: Path, warehouses: list[Any], dealers: list[Any]) -> None:
    path.mkdir(parents=True)
    tables: dict[str, tuple[list[str], list[tuple[Any, ...]]]] = {
        "regions": (["region_code", "region_name", "demand_weight"], [(r[0], r[1], r[3]) for r in REGIONS]),
        "packages": (["package_code", "package_name", "category", "price_bdt", "validity_days", "data_mb",
                      "voice_minutes"], [tuple(p) for p in PACKAGES]),
        "products": (["sku", "product_name", "category", "dealer_price_bdt"], [p[:4] for p in PRODUCTS]),
        "factories": (["factory_id", "factory_name", "region_code"], [f[:3] for f in FACTORIES]),
        "warehouses": (["warehouse_id", "warehouse_name", "region_code", "is_central"],
                       [(w.warehouse_id, w.name, w.region, str(w.is_central).lower()) for w in warehouses]),
        "dealers": (["dealer_id", "dealer_name", "region_code", "tier", "credit_days"],
                    [(d.dealer_id, d.name, d.region, d.tier, d.credit_days) for d in dealers]),
    }
    for name, (header, rows) in tables.items():
        with (path / f"{name}.csv").open("w", newline="") as fh:
            writer = csv.writer(fh, lineterminator="\n")
            writer.writerow(header)
            writer.writerows(rows)


def _line(rec: Any) -> str:
    if rec.raw_override is not None:
        return str(rec.raw_override)
    event = {k: v for k, v in rec.event.items() if not k.startswith("_")}
    return json.dumps({"arrival_time": iso_ms(rec.arrival_ms), "event": event}, separators=(",", ":"))


def _write_arrivals(path: Path, em: Emitter) -> tuple[list[str], int]:
    path.mkdir(parents=True)
    by_file: dict[str, list[str]] = defaultdict(list)
    for rec in sorted(em.records, key=lambda r: (r.arrival_ms, r.seq)):
        hour = datetime.fromtimestamp(rec.arrival_ms / 1000, tz=UTC).strftime("%Y%m%dT%H")
        by_file[hour].append(_line(rec))
    for name, lines in by_file.items():
        (path / f"{name}.jsonl").write_text("\n".join(lines) + "\n")
    return sorted(by_file), sum(len(v) for v in by_file.values())


def _write_truth(path: Path, cfg: GeneratorConfig, em: Emitter, tel: TelecomSimulator, files: list[str],
                 lines: int, start_ms: int, end_ms: int) -> dict[str, Any]:
    path.mkdir(parents=True)
    valid = [r for r in em.records if r.expected == "VALID"]
    valid_ids = sorted({str(r.event["event_id"]) for r in valid if r.event is not None})
    assert len(valid_ids) == len(valid), "generator produced two valid records with one event_id"
    on_hand: Counter[tuple[str, str]] = Counter()
    for r in valid:
        assert r.event is not None
        if r.event["event_type"] == "mfg.inventory_movement":
            p = r.event["payload"]
            on_hand[(p["warehouse_id"], p["sku"])] += p["quantity_delta"]
    expected = Counter(r.expected for r in em.records if r.expected != "VALID")
    manifest: dict[str, Any] = {
        "generator_config": cfg.to_json(),
        "stream_start": iso_ms(start_ms),
        "stream_end": iso_ms(end_ms),
        "arrival_files": len(files),
        "lines_total": lines,
        "unique_valid_events": len(valid_ids),
        "valid_events_by_type": dict(sorted(Counter(str(r.event["event_type"]) for r in valid if r.event).items())),
        "technical_duplicates": expected.pop("DUPLICATE", 0),
        "expected_quarantine_by_reason": dict(sorted(expected.items())),
        "recharge_revenue": [
            {"business_date": d, "channel": ch, "region": rg, "success_count": c, "amount_poisha": p}
            for (d, ch, rg), (c, p) in sorted(tel.revenue_truth.items())
        ],
        "inventory_on_hand": [
            {"warehouse_id": wh, "sku": sku, "quantity": q} for (wh, sku), q in sorted(on_hand.items())
        ],
        "anomalies_by_type": dict(sorted(Counter(a.anomaly_type for a in em.ledger).items())),
    }
    (path / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (path / "valid_event_ids.txt").write_text("\n".join(valid_ids) + "\n")
    with (path / "anomalies.jsonl").open("w") as fh:
        for a in em.ledger:
            fh.write(json.dumps(asdict(a), separators=(",", ":")) + "\n")
    return manifest


def directory_digest(path: Path) -> str:
    """SHA-256 over all files (relative path + content); used to prove determinism."""
    h = hashlib.sha256()
    for f in sorted(p for p in path.rglob("*") if p.is_file()):
        h.update(str(f.relative_to(path)).encode())
        h.update(f.read_bytes())
    return h.hexdigest()
