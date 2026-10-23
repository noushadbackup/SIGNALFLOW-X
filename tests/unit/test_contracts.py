from __future__ import annotations

import copy
import json
from typing import Any

import pytest

from signalflow import CONTRACTS_DIR
from signalflow.contracts import (
    CONTRACT_VIOLATION,
    ENVELOPE_VIOLATION,
    INVALID_TIMESTAMP,
    UNKNOWN_EVENT_TYPE,
    UNSUPPORTED_SCHEMA_VERSION,
    VALIDATORS,
    ContractRegistry,
    compatibility_problems,
)

# One valid payload per registered contract. Adding a contract without a sample fails test_every_contract_has_sample.
SAMPLES: dict[str, dict[str, Any]] = {
    "telecom.subscriber_lifecycle": {
        "subscriber_id": "SUB-0000001", "action": "ACTIVATED", "segment": "MASS",
        "home_region": "DHAKA", "plan_type": "PREPAID",
    },
    "telecom.recharge": {
        "recharge_id": "RCH-0123456789AB", "subscriber_id": "SUB-0000001", "amount_bdt": 50.0,
        "channel": "MOBILE_WALLET", "payment_txn_ref": "PAY-0123456789AB", "status": "SUCCESS",
        "failure_reason": None, "region": "DHAKA",
    },
    "telecom.billing_charge": {
        "charge_id": "CHG-0123456789AB", "recharge_id": "RCH-0123456789AB", "subscriber_id": "SUB-0000001",
        "amount_bdt": 50.0, "charge_type": "RECHARGE_CREDIT",
    },
    "telecom.payment_capture": {
        "capture_id": "CAP-0123456789AB", "payment_txn_ref": "PAY-0123456789AB", "subscriber_id": "SUB-0000001",
        "amount_bdt": 99.0, "status": "CAPTURED", "purpose": "PACKAGE",
    },
    "telecom.package_activation": {
        "activation_id": "ACT-0123456789AB", "subscriber_id": "SUB-0000001", "package_code": "DATA_1GB_7D",
        "price_bdt": 99.0, "payment_txn_ref": "PAY-0123456789AB", "status": "ACTIVATED",
    },
    "telecom.usage_record": {
        "usage_id": "USG-0123456789AB", "subscriber_id": "SUB-0000001", "usage_type": "DATA", "quantity": 120.5,
        "unit": "MB", "serving_region": "SYLHET", "cell_area_code": "SYL-004", "rated_amount_bdt": 0,
    },
    "telecom.channel_settlement": {
        "settlement_id": "STL-BANK_CARD-20261001", "channel": "BANK_CARD", "business_date": "2026-10-01",
        "transaction_count": 10, "settled_amount_bdt": 1500.0,
    },
    "mfg.production_batch": {
        "batch_id": "BAT-0123456789AB", "factory_id": "FAC-01", "sku": "RF-230L", "quantity": 40,
        "destination_warehouse_id": "WH-DHK-01",
    },
    "mfg.inventory_movement": {
        "movement_id": "MOV-0123456789AB", "warehouse_id": "WH-DHK-01", "sku": "RF-230L", "quantity_delta": -3,
        "movement_type": "SHIPMENT_OUT", "source_ref": "SHP-0123456789AB",
    },
    "mfg.stock_count_approval": {
        "approval_id": "SCA-0123456789AB", "warehouse_id": "WH-DHK-01", "sku": "RF-230L", "counted_quantity": 10,
        "system_quantity": 11, "approved_by_role": "INVENTORY_CONTROLLER",
    },
    "mfg.dealer_order": {
        "order_id": "ORD-0123456789AB", "dealer_id": "DLR-0001", "sku": "RF-230L", "quantity": 2,
        "unit_price_bdt": 54000.0,
    },
    "mfg.order_status": {"order_id": "ORD-0123456789AB", "status": "BACKORDERED", "reason": "INSUFFICIENT_STOCK"},
    "mfg.shipment": {
        "shipment_id": "SHP-0123456789AB", "order_id": "ORD-0123456789AB", "warehouse_id": "WH-DHK-01",
        "sku": "RF-230L", "quantity": 2, "status": "DISPATCHED",
    },
    "mfg.invoice": {
        "invoice_id": "INV-0123456789AB", "shipment_id": "SHP-0123456789AB", "order_id": "ORD-0123456789AB",
        "dealer_id": "DLR-0001", "amount_bdt": 108000.0, "due_date": "2026-10-31",
    },
    "mfg.dealer_settlement": {
        "settlement_id": "DST-0123456789AB", "invoice_id": "INV-0123456789AB", "dealer_id": "DLR-0001",
        "amount_bdt": 108000.0,
    },
    "mfg.product_return": {
        "return_id": "RET-0123456789AB", "invoice_id": "INV-0123456789AB", "dealer_id": "DLR-0001", "sku": "RF-230L",
        "quantity": 1, "refund_amount_bdt": 54000.0, "warehouse_id": "WH-DHK-01",
    },
    "mfg.refund": {
        "refund_id": "RFD-0123456789AB", "return_id": "RET-0123456789AB", "dealer_id": "DLR-0001",
        "amount_bdt": 54000.0,
    },
    "mfg.supply_disruption": {
        "disruption_id": "DSR-0123456789AB", "factory_id": "FAC-01", "reason": "POWER_OUTAGE",
        "expected_duration_hours": 12, "severity": "HIGH",
    },
}


@pytest.fixture(scope="module", params=VALIDATORS)
def registry(request: pytest.FixtureRequest) -> ContractRegistry:
    """Every contract test runs against both validators."""
    return ContractRegistry.load(CONTRACTS_DIR, validator=request.param)


def envelope(event_type: str, payload: dict[str, Any], version: str = "1.0") -> dict[str, Any]:
    return {
        "event_id": "4f9b0f4e-1c1d-5d6a-9a5b-3c2d1e0f9a8b",
        "event_type": event_type,
        "schema_version": version,
        "source_system": "tel-ocs" if event_type.startswith("telecom") else "mfg-erp",
        "event_time": "2026-10-01T04:00:00Z",
        "produced_at": "2026-10-01T04:00:02.125Z",
        "payload": payload,
    }


def test_every_contract_has_sample(registry: ContractRegistry) -> None:
    assert registry.event_types() == set(SAMPLES)


@pytest.mark.parametrize("event_type", sorted(SAMPLES))
def test_sample_is_valid(registry: ContractRegistry, event_type: str) -> None:
    result = registry.validate(envelope(event_type, SAMPLES[event_type]))
    assert result.ok, result.details


@pytest.mark.parametrize("event_type", sorted(SAMPLES))
def test_unknown_payload_field_rejected(registry: ContractRegistry, event_type: str) -> None:
    payload = dict(SAMPLES[event_type], unexpected_field=1)
    result = registry.validate(envelope(event_type, payload))
    assert result.reason_code == CONTRACT_VIOLATION


@pytest.mark.parametrize("event_type", sorted(SAMPLES))
def test_missing_required_field_rejected(registry: ContractRegistry, event_type: str) -> None:
    payload = dict(SAMPLES[event_type])
    payload.pop(next(iter(payload)))
    assert registry.validate(envelope(event_type, payload)).reason_code == CONTRACT_VIOLATION


def test_amount_as_string_rejected(registry: ContractRegistry) -> None:
    payload = dict(SAMPLES["telecom.recharge"], amount_bdt="50 BDT")
    result = registry.validate(envelope("telecom.recharge", payload))
    assert result.reason_code == CONTRACT_VIOLATION
    assert any("amount_bdt" in d for d in result.details)


def test_negative_and_sub_poisha_amounts_rejected(registry: ContractRegistry) -> None:
    for bad in (-5.0, 0, 10.001):
        payload = dict(SAMPLES["telecom.recharge"], amount_bdt=bad)
        assert registry.validate(envelope("telecom.recharge", payload)).reason_code == CONTRACT_VIOLATION, bad


def test_recharge_status_and_failure_reason_must_agree(registry: ContractRegistry) -> None:
    success_with_reason = dict(SAMPLES["telecom.recharge"], failure_reason="GATEWAY_TIMEOUT")
    failed_without_reason = dict(SAMPLES["telecom.recharge"], status="FAILED")
    failed_with_reason = dict(SAMPLES["telecom.recharge"], status="FAILED", failure_reason="GATEWAY_TIMEOUT")
    assert not registry.validate(envelope("telecom.recharge", success_with_reason)).ok
    assert not registry.validate(envelope("telecom.recharge", failed_without_reason)).ok
    assert registry.validate(envelope("telecom.recharge", failed_with_reason)).ok


def test_usage_unit_must_match_type(registry: ContractRegistry) -> None:
    payload = dict(SAMPLES["telecom.usage_record"], unit="SECOND")
    assert not registry.validate(envelope("telecom.usage_record", payload)).ok


def test_zero_inventory_delta_rejected(registry: ContractRegistry) -> None:
    payload = dict(SAMPLES["mfg.inventory_movement"], quantity_delta=0)
    assert not registry.validate(envelope("mfg.inventory_movement", payload)).ok


def test_minor_version_accepts_optional_field(registry: ContractRegistry) -> None:
    payload = dict(SAMPLES["telecom.recharge"], promo_code="PUJA26")
    assert registry.validate(envelope("telecom.recharge", payload, "1.1")).ok
    assert registry.validate(envelope("telecom.recharge", SAMPLES["telecom.recharge"], "1.1")).ok
    assert registry.validate(envelope("telecom.recharge", payload, "1.0")).reason_code == CONTRACT_VIOLATION


def test_unregistered_major_version_rejected(registry: ContractRegistry) -> None:
    result = registry.validate(envelope("telecom.recharge", SAMPLES["telecom.recharge"], "2.0"))
    assert result.reason_code == UNSUPPORTED_SCHEMA_VERSION


def test_unknown_event_type(registry: ContractRegistry) -> None:
    assert registry.validate(envelope("telecom.unknown_thing", {})).reason_code == UNKNOWN_EVENT_TYPE


@pytest.mark.parametrize(
    "mutation",
    [
        lambda e: e.pop("event_id"),
        lambda e: e.update(event_id="not-a-uuid"),
        lambda e: e.update(event_time="2026-10-01 04:00:00"),
        lambda e: e.update(source_system="unknown-system"),
        lambda e: e.update(extra=True),
        lambda e: e.update(payload="string"),
    ],
)
def test_envelope_violations(registry: ContractRegistry, mutation: Any) -> None:
    event = envelope("telecom.recharge", SAMPLES["telecom.recharge"])
    mutation(event)
    assert registry.validate(event).reason_code == ENVELOPE_VIOLATION


def test_non_object_event(registry: ContractRegistry) -> None:
    assert registry.validate(["a"]).reason_code == ENVELOPE_VIOLATION


def test_event_time_after_produced_at_rejected(registry: ContractRegistry) -> None:
    event = envelope("telecom.recharge", SAMPLES["telecom.recharge"])
    event["event_time"] = "2026-10-01T05:00:00Z"
    assert registry.validate(event).reason_code == INVALID_TIMESTAMP


def test_all_minor_versions_are_backward_compatible(registry: ContractRegistry) -> None:
    by_type: dict[tuple[str, str], list[tuple[int, Any]]] = {}
    for (event_type, version), contract in registry.contracts.items():
        major, minor = version.split(".")
        by_type.setdefault((event_type, major), []).append((int(minor), contract.schema))
    for (event_type, major), versions in by_type.items():
        versions.sort(key=lambda v: v[0])
        for (_, older), (minor, newer) in zip(versions, versions[1:], strict=False):
            assert compatibility_problems(older, newer) == [], f"{event_type} {major}.{minor}"


def test_compatibility_checker_detects_breaking_change(registry: ContractRegistry) -> None:
    old = registry.get("telecom.recharge", "1.0")
    assert old is not None
    new = copy.deepcopy(old.schema)
    new["properties"]["amount_bdt"] = {"type": "string"}
    new["required"].append("promo_code")
    problems = compatibility_problems(old.schema, new)
    assert any("amount_bdt" in p for p in problems)
    assert any("required" in p for p in problems)


def test_contract_files_are_canonical_json() -> None:
    for path in CONTRACTS_DIR.rglob("*.json"):
        doc = json.loads(path.read_text())
        assert path.read_text() == json.dumps(doc, indent=2) + "\n", path


# Keywords whose meaning is identical in draft-07 (the compiled validator) and 2020-12 (the reference). A contract
# using anything else must extend this list only after checking the two drafts agree on it.
SAFE_KEYWORDS = {"$schema", "$id", "title", "description", "type", "properties", "required", "additionalProperties",
                 "enum", "const", "pattern", "minimum", "maximum", "exclusiveMinimum", "minLength", "maxLength",
                 "if", "then", "allOf", "not", "x-business-key", "x-decimal-places"}


def _keywords(node: Any) -> set[str]:
    if isinstance(node, list):
        return set().union(*(_keywords(n) for n in node)) if node else set()
    if not isinstance(node, dict):
        return set()
    found = set(node)
    for key, value in node.items():
        found |= set().union(*(_keywords(v) for v in value.values())) if key == "properties" else _keywords(value)
    return found


def test_contracts_use_only_keywords_both_validators_agree_on() -> None:
    for path in CONTRACTS_DIR.rglob("*.json"):
        assert _keywords(json.loads(path.read_text())) <= SAFE_KEYWORDS, path


def _variants(event: dict[str, Any]) -> list[Any]:
    """Each payload field removed, nulled, or replaced by values that differ between lax and strict validators,
    plus envelope damage."""
    odd: list[Any] = [None, "", "x", 0, 1, 1.0, -1, 0.001, 1e12, True, False, [], {}]
    out: list[Any] = [event]
    for name in list(event["payload"]):
        gone = copy.deepcopy(event)
        del gone["payload"][name]
        out.append(gone)
        for value in odd:
            changed = copy.deepcopy(event)
            changed["payload"][name] = value
            out.append(changed)
    for name in ("event_id", "event_type", "schema_version", "event_time", "payload"):
        for value in (None, 1, "bad", {}):
            changed = copy.deepcopy(event)
            changed[name] = value
            out.append(changed)
    extra = copy.deepcopy(event)
    extra["payload"]["unexpected"] = 1
    out.append(extra)
    return out


def test_compiled_and_reference_validators_decide_identically() -> None:
    compiled = ContractRegistry.load(CONTRACTS_DIR, validator="compiled")
    reference = ContractRegistry.load(CONTRACTS_DIR, validator="reference")
    checked = 0
    for event_type, payload in SAMPLES.items():
        versions = sorted(v for et, v in reference.contracts if et == event_type)
        for version in versions:
            for event in _variants(envelope(event_type, payload, version)):
                assert compiled.validate(event) == reference.validate(event), (event_type, version, event)
                checked += 1
    assert checked > 1500


def test_validator_must_be_known() -> None:
    with pytest.raises(ValueError):
        ContractRegistry.load(CONTRACTS_DIR, validator="fast")
