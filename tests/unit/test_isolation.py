"""Ground truth must never leak into pipeline logic (requirement R7 / DATA_CONTRACTS §6)."""

from __future__ import annotations

import re
from pathlib import Path

from signalflow import REPO_ROOT

PIPELINE_CODE = [REPO_ROOT / "src" / "signalflow", REPO_ROOT / "dbt" / "models", REPO_ROOT / "dbt" / "macros",
                 REPO_ROOT / "sql"]
FORBIDDEN = re.compile(r"truth|anomalies\.jsonl|valid_event_ids|manifest\.json", re.IGNORECASE)
# Files that read dbt's own manifest (dbt/target/lineage/manifest.json, a dbt parse artifact). Only the word
# "manifest.json" is excused there; every other forbidden word is still checked.
DBT_MANIFEST_READERS = {"src/signalflow/lineage/graph.py"}


def test_pipeline_code_never_references_ground_truth() -> None:
    offenders: list[str] = []
    for root in PIPELINE_CODE:
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix not in {".py", ".sql", ".yml"}:
                continue
            if "generator" in path.relative_to(REPO_ROOT).parts:
                continue  # the generator is the only writer of truth
            excused = str(path.relative_to(REPO_ROOT)) in DBT_MANIFEST_READERS
            for n, line in enumerate(path.read_text().splitlines(), 1):
                hits = {m.group(0).lower() for m in FORBIDDEN.finditer(line)}
                if hits - ({"manifest.json"} if excused else set()):
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{n}: {line.strip()}")
    assert offenders == []


def test_isolation_scan_covers_real_files() -> None:
    files = [p for root in PIPELINE_CODE for p in root.rglob("*.py")] + list(Path(REPO_ROOT / "dbt").rglob("*.sql"))
    assert len(files) > 30
