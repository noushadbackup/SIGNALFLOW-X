"""Versioned data contracts (JSON Schema 2020-12) and the validation policy applied before Silver.

Contract files live in ``contracts/<domain>/<name>/<major>.<minor>.json``; ``contracts/envelope.json`` describes the
envelope shared by every event. The same files are meant to be reused by the Spark path in later phases, which is
why business rules are expressed in JSON Schema wherever the language allows it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

# Reason codes are part of the operator-facing contract: dashboards and alerts group by them.
ENVELOPE_VIOLATION = "ENVELOPE_VIOLATION"
UNKNOWN_EVENT_TYPE = "UNKNOWN_EVENT_TYPE"
UNSUPPORTED_SCHEMA_VERSION = "UNSUPPORTED_SCHEMA_VERSION"
CONTRACT_VIOLATION = "CONTRACT_VIOLATION"
INVALID_TIMESTAMP = "INVALID_TIMESTAMP"

# A producer clock may run slightly behind the business clock; anything beyond this is treated as corrupt.
MAX_PRODUCER_CLOCK_SKEW = timedelta(minutes=5)


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    reason_code: str | None = None
    details: list[str] = field(default_factory=list)
    contract_id: str | None = None


@dataclass(frozen=True)
class Contract:
    event_type: str
    version: str
    schema: dict[str, Any]
    validator: Draft202012Validator
    decimal_fields: tuple[str, ...]

    @property
    def contract_id(self) -> str:
        return f"{self.event_type}/{self.version}"

    @property
    def business_key(self) -> str:
        return str(self.schema["x-business-key"])


def parse_utc(value: str) -> datetime:
    """Parse the envelope timestamp format (RFC 3339, UTC 'Z' suffix only)."""
    if not value.endswith("Z"):
        raise ValueError(f"timestamp must be UTC with 'Z' suffix: {value!r}")
    return datetime.fromisoformat(value[:-1] + "+00:00")


def _decimal_places_ok(value: Any, places: int) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return True  # type errors are reported by the schema itself
    return Decimal(repr(value)).as_tuple().exponent >= -places  # type: ignore[operator]


class ContractRegistry:
    def __init__(self, envelope: dict[str, Any], contracts: dict[tuple[str, str], Contract]):
        self._envelope = Draft202012Validator(envelope)
        self._contracts = contracts

    @classmethod
    def load(cls, root: Path) -> ContractRegistry:
        envelope = json.loads((root / "envelope.json").read_text())
        Draft202012Validator.check_schema(envelope)
        contracts: dict[tuple[str, str], Contract] = {}
        for path in sorted(root.glob("*/*/*.json")):
            schema = json.loads(path.read_text())
            Draft202012Validator.check_schema(schema)
            event_type = f"{path.parent.parent.name}.{path.parent.name}"
            version = path.stem
            decimals = tuple(
                name for name, prop in schema["properties"].items() if "x-decimal-places" in prop
            )
            contracts[(event_type, version)] = Contract(
                event_type, version, schema, Draft202012Validator(schema), decimals
            )
        return cls(envelope, contracts)

    @property
    def contracts(self) -> dict[tuple[str, str], Contract]:
        return dict(self._contracts)

    def event_types(self) -> set[str]:
        return {et for et, _ in self._contracts}

    def get(self, event_type: str, version: str) -> Contract | None:
        return self._contracts.get((event_type, version))

    def validate(self, event: Any) -> ValidationResult:
        if not isinstance(event, dict):
            return ValidationResult(False, ENVELOPE_VIOLATION, ["event is not a JSON object"])
        env_errors = sorted(self._envelope.iter_errors(event), key=lambda e: list(e.path))
        if env_errors:
            return ValidationResult(False, ENVELOPE_VIOLATION, [_fmt(e) for e in env_errors][:10])

        try:
            event_time = parse_utc(event["event_time"])
            produced_at = parse_utc(event["produced_at"])
        except ValueError as exc:
            return ValidationResult(False, INVALID_TIMESTAMP, [str(exc)])
        if event_time - produced_at > MAX_PRODUCER_CLOCK_SKEW:
            return ValidationResult(
                False, INVALID_TIMESTAMP, ["event_time is later than produced_at beyond allowed clock skew"]
            )

        event_type, version = event["event_type"], event["schema_version"]
        if event_type not in self.event_types():
            return ValidationResult(False, UNKNOWN_EVENT_TYPE, [f"no contract for {event_type}"])
        contract = self.get(event_type, version)
        if contract is None:
            known = sorted(v for et, v in self._contracts if et == event_type)
            return ValidationResult(
                False, UNSUPPORTED_SCHEMA_VERSION, [f"{event_type} v{version} not registered; known: {known}"]
            )

        payload = event["payload"]
        errors = [_fmt(e) for e in sorted(contract.validator.iter_errors(payload), key=lambda e: list(e.path))]
        errors += [
            f"payload.{name}: more than 2 decimal places"
            for name in contract.decimal_fields
            if name in payload and not _decimal_places_ok(payload[name], 2)
        ]
        if errors:
            return ValidationResult(False, CONTRACT_VIOLATION, errors[:10], contract.contract_id)
        return ValidationResult(True, contract_id=contract.contract_id)


def _fmt(error: Any) -> str:
    path = ".".join(str(p) for p in error.absolute_path) or "<root>"
    return f"{path}: {error.message}"[:300]


def compatibility_problems(old: dict[str, Any], new: dict[str, Any]) -> list[str]:
    """Minor-version policy: a reader of either version must accept events of the other.

    That holds when the required set is unchanged, no property is removed or retyped, and new properties are
    optional. Anything else needs a new major version (and a new topic/table in the streaming path).
    """
    problems: list[str] = []
    if set(old["required"]) != set(new["required"]):
        problems.append(f"required set changed: {sorted(set(old['required']) ^ set(new['required']))}")
    for name, spec in old["properties"].items():
        if name not in new["properties"]:
            problems.append(f"property removed: {name}")
        elif new["properties"][name] != spec:
            problems.append(f"property definition changed: {name}")
    if old.get("allOf") != new.get("allOf"):
        problems.append("conditional business rules changed")
    if old.get("additionalProperties") != new.get("additionalProperties"):
        problems.append("additionalProperties policy changed")
    return problems
