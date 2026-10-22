"""Streaming pieces that need no infrastructure: routing, source-record parsing, connector definitions, sink
helpers."""

from __future__ import annotations

import json
import re
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from signalflow import CONTRACTS_DIR
from signalflow.contracts import ContractRegistry
from signalflow.processing.silver import UNPARSEABLE, validate_message
from signalflow.streaming.connect import master_data_connector, outbox_connector
from signalflow.streaming.replay import read_line
from signalflow.streaming.settings import StreamSettings
from signalflow.streaming.sink import KafkaRecord, _offsets
from signalflow.streaming.topics import Router, all_event_topics, events_pattern

REGISTRY = ContractRegistry.load(CONTRACTS_DIR)
SETTINGS = replace(StreamSettings.from_env(), topic_prefix="sfx")


def test_routing_by_type_and_major_version() -> None:
    r = Router("sfx", REGISTRY)
    assert r.route({"event_type": "telecom.recharge", "schema_version": "1.1"}, "telecom") == "sfx.telecom.recharge.v1"
    assert r.route({"event_type": "mfg.invoice", "schema_version": "1.0"}, "telecom") == "sfx.mfg.invoice.v1"
    # Unregistered major version or type: the domain's unrouted topic, so Silver can quarantine it.
    assert r.route({"event_type": "telecom.recharge", "schema_version": "2.0"}, "telecom") == \
        "sfx.telecom.unrouted.v1"
    assert r.route({"event_type": "mfg.unknown", "schema_version": "1.0"}, "telecom") == "sfx.mfg.unrouted.v1"
    assert r.route(None, "telecom") == "sfx.telecom.unrouted.v1"
    assert r.route({"schema_version": "1.0"}, "mfg") == "sfx.mfg.unrouted.v1"


def test_every_routable_topic_matches_the_spark_pattern_and_master_data_does_not() -> None:
    pattern = re.compile(events_pattern("sfx"))
    topics = all_event_topics("sfx", REGISTRY)
    assert len(topics) == len({(et, v.split(".")[0]) for et, v in REGISTRY.contracts}) + 2
    assert all(pattern.fullmatch(t) for t in topics)
    assert not pattern.fullmatch("sfx.erpcdc.dealers")
    assert not pattern.fullmatch("other.telecom.recharge.v1")


def test_read_line_splits_transport_wrapper_and_keeps_damaged_lines_verbatim() -> None:
    event = {"event_id": "e1", "event_type": "mfg.invoice", "schema_version": "1.0"}
    rec = read_line(json.dumps({"arrival_time": "2026-10-01T00:00:00.000Z", "event": event}))
    assert rec.arrival_time == "2026-10-01T00:00:00.000Z" and json.loads(rec.value) == event
    assert rec.domain == "mfg" and rec.key == "e1"
    broken = '{"arrival_time": "2026-10-01T00:00:00.000Z", "event": {"event_id": "0e1f'
    rec = read_line(broken)
    assert rec.event is None and rec.value == broken and rec.domain == "telecom" and rec.key is None


def test_validate_message_matches_file_semantics() -> None:
    assert validate_message(REGISTRY, "{not json", None)[1] == UNPARSEABLE
    assert validate_message(REGISTRY, None, None)[1] == UNPARSEABLE
    assert validate_message(REGISTRY, "[]", None)[1] == "ENVELOPE_VIOLATION"


def test_connectors_never_carry_secrets_and_route_with_the_prefix() -> None:
    for name, config in (outbox_connector(SETTINGS), master_data_connector(SETTINGS)):
        assert name.startswith("sfx-")
        assert config["database.password"] == "${env:CDC_PASSWORD}"
        assert config["publication.autocreate.mode"] == "disabled"
        assert config["database.user"] == SETTINGS.cdc_user
    _, outbox = outbox_connector(SETTINGS)
    assert outbox["transforms.outbox.route.topic.replacement"] == "sfx.${routedByValue}"
    assert outbox["snapshot.mode"] == "no_data"
    assert outbox["slot.name"] != master_data_connector(SETTINGS)[1]["slot.name"]


@pytest.mark.parametrize(("header", "expected"), [
    ("2026-10-01T06:00:00.000Z", datetime(2026, 10, 1, 6, tzinfo=UTC)),
    (None, datetime(2026, 10, 22, tzinfo=UTC)),
    ("garbage", datetime(2026, 10, 22, tzinfo=UTC)),
])
def test_arrival_time_prefers_the_source_header(header: str | None, expected: datetime) -> None:
    rec = KafkaRecord("t", 0, 1, datetime(2026, 10, 22, tzinfo=UTC), header, "{}", None, None, None, None, None,
                      "[]", None)
    assert rec.arrival_time == expected


def test_offsets_summary() -> None:
    recs = [KafkaRecord("a", p, o, None, None, None, None, None, None, None, None, "[]", None)
            for p, o in [(0, 5), (0, 3), (1, 9)]]
    assert _offsets(recs) == {"a": {"0": [3, 5], "1": [9, 9]}}
