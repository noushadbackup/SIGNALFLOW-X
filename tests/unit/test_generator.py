from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

import pytest

from signalflow import CONTRACTS_DIR
from signalflow.contracts import ContractRegistry
from signalflow.generator import GeneratorConfig, directory_digest, generate

SMALL = GeneratorConfig(seed=11, days=3, subscribers=400, dealers=60)
# Bangladeshi mobile numbers: 01[3-9] followed by 8 digits, optionally with +880 / 880 prefix.
MSISDN = re.compile(r"(?<!\d)(?:\+?880)?01[3-9]\d{8}(?!\d)")


@pytest.fixture(scope="module")
def dataset(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out = tmp_path_factory.mktemp("gen") / "data"
    generate(SMALL, out)
    return out


def _lines(path: Path) -> list[str]:
    return [line for f in sorted((path / "arrivals").glob("*.jsonl")) for line in f.read_text().splitlines()]


def test_same_seed_is_byte_identical(dataset: Path, tmp_path: Path) -> None:
    generate(SMALL, tmp_path / "again")
    assert directory_digest(dataset) == directory_digest(tmp_path / "again")


def test_different_seed_differs(dataset: Path, tmp_path: Path) -> None:
    generate(GeneratorConfig(seed=12, days=3, subscribers=400, dealers=60), tmp_path / "other")
    assert directory_digest(dataset) != directory_digest(tmp_path / "other")


def test_refuses_to_overwrite(dataset: Path) -> None:
    with pytest.raises(FileExistsError):
        generate(SMALL, dataset)


def test_no_phone_numbers_anywhere(dataset: Path) -> None:
    for f in dataset.rglob("*"):
        if f.is_file():
            assert not MSISDN.search(f.read_text()), f"phone-number-like string in {f}"


def test_manifest_counts_match_files(dataset: Path) -> None:
    manifest = json.loads((dataset / "truth" / "manifest.json").read_text())
    lines = _lines(dataset)
    assert len(lines) == manifest["lines_total"]
    quarantine = sum(manifest["expected_quarantine_by_reason"].values())
    assert len(lines) == manifest["unique_valid_events"] + manifest["technical_duplicates"] + quarantine


def test_valid_events_pass_contracts_and_invalid_fail_as_expected(dataset: Path) -> None:
    registry = ContractRegistry.load(CONTRACTS_DIR)
    manifest = json.loads((dataset / "truth" / "manifest.json").read_text())
    valid_ids = set((dataset / "truth" / "valid_event_ids.txt").read_text().split())
    seen: Counter[str] = Counter()
    reasons: Counter[str] = Counter()
    for line in _lines(dataset):
        try:
            event = json.loads(line)["event"]
        except (ValueError, KeyError):
            reasons["UNPARSEABLE"] += 1
            continue
        result = registry.validate(event)
        if event["event_id"] in valid_ids:
            assert result.ok, (event["event_type"], result.details)
            seen[event["event_id"]] += 1
        else:
            assert not result.ok
            reasons[str(result.reason_code)] += 1
    assert set(seen) == valid_ids
    # Conflicts reuse a valid event_id with a different payload; they validate fine and are caught by the processor.
    expected = dict(manifest["expected_quarantine_by_reason"])
    conflicts = expected.pop("EVENT_ID_CONFLICT", 0)
    assert dict(reasons) == expected
    assert sum(seen.values()) == len(valid_ids) + manifest["technical_duplicates"] + conflicts


def test_every_scenario_is_present_in_small_runs(dataset: Path) -> None:
    manifest = json.loads((dataset / "truth" / "manifest.json").read_text())
    for kind in ("MISSING_BILLING", "DUPLICATE_PAYMENT_CHARGE", "ACTIVATION_WITHOUT_CAPTURE", "SETTLEMENT_MISMATCH",
                 "SHIPMENT_WITHOUT_INVOICE", "DEALER_SETTLEMENT_MISMATCH", "DOUBLE_REFUND", "UNAUTHORIZED_ADJUSTMENT",
                 "TECH_DUPLICATE", "SCHEMA_BREAK", "EVENT_ID_CONFLICT", "LATE_WITHIN_WATERMARK",
                 "LATE_BEYOND_WATERMARK"):
        assert manifest["anomalies_by_type"].get(kind, 0) >= 1, kind


def test_ledger_references_real_events(dataset: Path) -> None:
    lines = "\n".join(_lines(dataset))
    for raw in (dataset / "truth" / "anomalies.jsonl").read_text().splitlines():
        anomaly = json.loads(raw)
        for key in ("event_id", "recharge_id", "invoice_id", "return_id", "shipment_id", "source_ref"):
            if key in anomaly["refs"]:
                assert anomaly["refs"][key] in lines, anomaly


def test_arrival_files_are_in_arrival_order(dataset: Path) -> None:
    previous = ""
    for line in _lines(dataset):
        match = re.match(r'\{"arrival_time":\s*"([^"]+)"', line)
        assert match, line[:80]
        assert match.group(1) >= previous
        previous = match.group(1)


def test_money_has_at_most_two_decimals(dataset: Path) -> None:
    for line in _lines(dataset):
        for value in re.findall(r'_bdt":(-?\d+\.\d+)', line):
            assert len(value.split(".")[1]) <= 2, value
