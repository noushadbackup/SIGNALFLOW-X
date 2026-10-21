"""Deterministic event emission, identifiers, time helpers and the ground-truth ledger."""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

DHAKA_OFFSET_MS = 6 * 3600 * 1000  # Asia/Dhaka is UTC+6 with no DST
HOUR_MS = 3600 * 1000
DAY_MS = 24 * HOUR_MS
_ID_MULT = 0x9E3779B97F4A7C15 % (1 << 48) | 1  # odd => multiplication mod 2^48 is a bijection
_NAMESPACE = uuid.UUID("6f1c2a8e-3b7d-4e0a-9c55-5a1f3e9b2d10")


def dhaka_midnight_ms(day: date) -> int:
    """UTC epoch ms of 00:00 Asia/Dhaka on ``day``."""
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp() * 1000) - DHAKA_OFFSET_MS


def iso_ms(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{ms % 1000:03d}Z"


def business_date(ms: int) -> date:
    return (datetime.fromtimestamp((ms + DHAKA_OFFSET_MS) / 1000, tz=UTC)).date()


def poisha_to_bdt(poisha: int) -> float:
    return poisha / 100


@dataclass
class Record:
    """One line of transport output: an event plus the time it arrives at the platform."""

    arrival_ms: int
    event: dict[str, Any] | None
    seq: int
    raw_override: str | None = None  # for deliberately unparseable lines
    expected: str = "VALID"  # VALID, DUPLICATE, or the quarantine reason code the pipeline should assign

    @property
    def event_time_ms(self) -> int:
        assert self.event is not None
        return int(self.event["_event_ms"])


@dataclass
class Anomaly:
    anomaly_id: str
    anomaly_type: str
    domain: str
    refs: dict[str, Any]
    expected_effect: str


@dataclass
class Emitter:
    seed: int
    rng: random.Random
    end_ms: int
    records: list[Record] = field(default_factory=list)
    ledger: list[Anomaly] = field(default_factory=list)
    _event_counter: int = 0
    _id_counters: dict[str, int] = field(default_factory=dict)
    _seq: int = 0
    guarantee_scenarios: bool = True
    _opportunities: dict[str, int] = field(default_factory=dict)
    _injected: set[str] = field(default_factory=set)

    def business_id(self, prefix: str) -> str:
        n = self._id_counters.get(prefix, 0) + 1
        self._id_counters[prefix] = n
        scrambled = (n * _ID_MULT + self.seed * 7919 + hash_prefix(prefix)) % (1 << 48)
        return f"{prefix}-{scrambled:012X}"

    def event_id(self) -> str:
        self._event_counter += 1
        return str(uuid.uuid5(_NAMESPACE, f"{self.seed}:{self._event_counter}"))

    def emit(
        self,
        event_type: str,
        source_system: str,
        event_ms: int,
        payload: dict[str, Any],
        version: str = "1.0",
        arrival_ms: int | None = None,
    ) -> dict[str, Any] | None:
        """Create an event; events in the simulated future (>= end) are not emitted and None is returned."""
        if event_ms >= self.end_ms:
            return None
        produced_ms = event_ms + self.rng.randint(50, 2000)
        event = {
            "event_id": self.event_id(),
            "event_type": event_type,
            "schema_version": version,
            "source_system": source_system,
            "event_time": iso_ms(event_ms),
            "produced_at": iso_ms(produced_ms),
            "payload": payload,
            "_event_ms": event_ms,
        }
        arrival = arrival_ms if arrival_ms is not None else produced_ms + self.rng.randint(200, 90_000)
        self.add_record(arrival, event)
        return event

    def add_record(self, arrival_ms: int, event: dict[str, Any] | None, raw: str | None = None,
                   expected: str = "VALID") -> Record:
        self._seq += 1
        rec = Record(arrival_ms, event, self._seq, raw, expected)
        self.records.append(rec)
        return rec

    def roll(self, rng: random.Random, kind: str, prob: float) -> bool:
        """Bernoulli draw for a business anomaly. With guarantee_scenarios, the first opportunity is forced if the
        scenario has not occurred yet, so small demo runs still contain every scenario. Always consumes one draw."""
        hit = rng.random() < prob
        n = self._opportunities.get(kind, 0) + 1
        self._opportunities[kind] = n
        if not hit and self.guarantee_scenarios and n == 1 and kind not in self._injected:
            hit = True
        if hit:
            self._injected.add(kind)
        return hit

    def anomaly(self, anomaly_type: str, domain: str, refs: dict[str, Any], expected_effect: str) -> None:
        self.ledger.append(Anomaly(f"ANM-{len(self.ledger) + 1:05d}", anomaly_type, domain, refs, expected_effect))


def hash_prefix(prefix: str) -> int:
    value = 0
    for ch in prefix:
        value = (value * 131 + ord(ch)) % (1 << 32)
    return value


def at_local_time(day_start_ms: int, rng: random.Random, hour_weights: list[float]) -> int:
    hour = rng.choices(range(24), hour_weights)[0]
    return day_start_ms + hour * HOUR_MS + rng.randrange(HOUR_MS)


def between(rng: random.Random, start_ms: int, low: timedelta, high: timedelta) -> int:
    return start_ms + rng.randint(int(low.total_seconds() * 1000), int(high.total_seconds() * 1000))
