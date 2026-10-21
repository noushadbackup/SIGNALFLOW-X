"""Transport-level effects the platform must handle: late arrival, redelivery, schema breaks, id conflicts, garbage.

These are applied after the business simulation so they never change business truth, only how it arrives.
"""

from __future__ import annotations

import copy
import random
from datetime import timedelta

from .config import GeneratorConfig
from .core import HOUR_MS, Emitter, Record, between, iso_ms

LATE_WITHIN = (timedelta(hours=1), timedelta(hours=4))
# Beyond-watermark delays are well past the 6 h allowed lateness, with headroom for quiet periods in a stream
# (manufacturing is mostly idle at night, so its stream clock can lag wall time by several hours).
LATE_BEYOND = {"telecom": (timedelta(hours=10), timedelta(hours=30)), "mfg": (timedelta(hours=24), timedelta(hours=48))}


def _stream(rec: Record) -> str:
    assert rec.event is not None
    return "telecom" if rec.event["event_type"].startswith("telecom.") else "mfg"


def apply_transport_effects(cfg: GeneratorConfig, em: Emitter, rng: random.Random, stream_start_ms: int) -> None:
    live = [r for r in em.records if r.event is not None and r.arrival_ms >= stream_start_ms]

    for rec in live:
        assert rec.event is not None
        r = rng.random()
        if r < cfg.late_beyond_prob:
            low, high = LATE_BEYOND[_stream(rec)]
            kind = "LATE_BEYOND_WATERMARK"
        elif r < cfg.late_beyond_prob + cfg.late_within_prob:
            low, high = LATE_WITHIN
            kind = "LATE_WITHIN_WATERMARK"
        else:
            continue
        rec.arrival_ms = between(rng, rec.event_time_ms, low, high)
        em.anomaly(kind, _stream(rec), {"event_id": rec.event["event_id"], "event_type": rec.event["event_type"],
                                        "delay_seconds": (rec.arrival_ms - rec.event_time_ms) // 1000},
                   "accepted into silver and classified by lateness")

    duplicated: set[int] = set()
    for rec in live:
        assert rec.event is not None
        if rng.random() < cfg.duplicate_prob:
            duplicated.add(rec.seq)
            em.add_record(between(rng, rec.arrival_ms, timedelta(seconds=1), timedelta(minutes=15)), rec.event,
                          expected="DUPLICATE")
            em.anomaly("TECH_DUPLICATE", _stream(rec), {"event_id": rec.event["event_id"]},
                       "redelivered record is logged as duplicate and not double counted")

    recharges = [r for r in live if r.event and r.event["event_type"] == "telecom.recharge"]
    for i, rec in enumerate(rng.sample(recharges, min(cfg.schema_break_count, len(recharges)))):
        broken, reason, note = _break(copy.deepcopy(rec.event), i, em)  # type: ignore[arg-type]
        em.add_record(between(rng, rec.arrival_ms, timedelta(seconds=1), timedelta(seconds=30)), broken,
                      expected=reason)
        em.anomaly("SCHEMA_BREAK", "telecom", {"event_id": broken["event_id"], "mutation": note,
                                               "expected_reason": reason}, "quarantined; trusted data unaffected")

    usage = [r for r in live if r.event and r.event["event_type"] == "telecom.usage_record" and r.seq not in duplicated]
    for rec in rng.sample(usage, min(cfg.event_id_conflict_count, len(usage))):
        assert rec.event is not None
        conflicting = copy.deepcopy(rec.event)
        conflicting["payload"]["quantity"] = conflicting["payload"]["quantity"] + 1
        em.add_record(rec.arrival_ms + HOUR_MS, conflicting, expected="EVENT_ID_CONFLICT")
        em.anomaly("EVENT_ID_CONFLICT", "telecom", {"event_id": rec.event["event_id"]},
                   "second version with same event_id is quarantined; first accepted version stays")

    for _ in range(cfg.unparseable_line_count):
        arrival = rng.randint(stream_start_ms, em.end_ms - 1)
        raw = '{"arrival_time": "' + iso_ms(arrival) + '", "event": {"event_id": "0e1f'  # truncated write
        em.add_record(arrival, None, raw=raw, expected="UNPARSEABLE")
        em.anomaly("UNPARSEABLE_RECORD", "telecom", {"arrival_time": iso_ms(arrival)},
                   "kept verbatim in bronze, quarantined as UNPARSEABLE")


def _break(event: dict[str, object], i: int, em: Emitter) -> tuple[dict[str, object], str, str]:
    event["event_id"] = em.event_id()
    payload = event["payload"]
    assert isinstance(payload, dict)
    match i % 5:
        case 0:
            payload.pop("amount_bdt")
            return event, "CONTRACT_VIOLATION", "required field amount_bdt removed"
        case 1:
            payload["amount_bdt"] = f"{payload['amount_bdt']} BDT"
            return event, "CONTRACT_VIOLATION", "amount_bdt sent as string"
        case 2:
            event["schema_version"] = "2.0"
            payload["amount_poisha"] = int(round(float(payload.pop("amount_bdt")) * 100))
            return event, "UNSUPPORTED_SCHEMA_VERSION", "producer switched to unregistered v2.0 (amount in poisha)"
        case 3:
            payload["status"] = "PARTIAL"
            return event, "CONTRACT_VIOLATION", "status outside enum"
        case _:
            event["event_time"] = str(event["event_time"]).rstrip("Z")
            return event, "ENVELOPE_VIOLATION", "event_time without UTC designator"
