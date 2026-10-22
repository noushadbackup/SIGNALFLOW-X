"""Source simulator for the streaming profile. Replays the generator's arrival files in order: telecom events go to
Kafka through an idempotent producer (the charging/mediation systems), manufacturing events go through the ERP
(business row + outbox row per transaction), from where Debezium publishes them."""

from __future__ import annotations

import json
import time
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..contracts import ContractRegistry
from ..erp import ErpApp, connect_db
from ..ingest.bronze_loader import arrival_files
from ..log import get_logger
from .settings import StreamSettings
from .topics import Router

ARRIVAL_HEADER = "sf_arrival_time"
log = get_logger(__name__)


@dataclass
class ReplayStats:
    files: int = 0
    telecom_messages: int = 0
    erp_events: int = 0
    unparseable: int = 0
    by_topic: Counter[str] = field(default_factory=Counter)


@dataclass(frozen=True)
class SourceRecord:
    arrival_time: str | None
    event: Any               # parsed event object, or None if the line could not be parsed
    value: str               # bytes the source system emits (the event JSON, or the damaged line verbatim)

    @property
    def domain(self) -> str:
        et = self.event.get("event_type") if isinstance(self.event, dict) else None
        return "mfg" if isinstance(et, str) and et.startswith("mfg.") else "telecom"

    @property
    def key(self) -> str | None:
        eid = self.event.get("event_id") if isinstance(self.event, dict) else None
        return eid if isinstance(eid, str) else None


def read_line(line: str) -> SourceRecord:
    """Split the file transport wrapper ``{"arrival_time", "event"}`` from the event the source emitted.
    A damaged line (the generator writes one truncated record) is forwarded verbatim; it is attributed to telecom,
    the feed it came from, and lands on the telecom ``unrouted`` topic."""
    try:
        wrapper = json.loads(line)
        event = wrapper["event"]
        arrival = wrapper.get("arrival_time")
    except (ValueError, KeyError, TypeError):
        return SourceRecord(None, None, line)
    value = json.dumps(event, separators=(",", ":"), ensure_ascii=False)
    return SourceRecord(arrival if isinstance(arrival, str) else None, event, value)


class TelecomProducer:
    def __init__(self, settings: StreamSettings, router: Router):
        from confluent_kafka import Producer

        self.router = router
        self.errors: list[str] = []
        # Idempotent producer: acks=all + broker-side sequence numbers, so a retried send is not duplicated and
        # per-partition order is kept (max 5 in flight). It does not dedupe re-sends by the application itself.
        self._producer = Producer({
            "bootstrap.servers": settings.bootstrap, "enable.idempotence": True, "acks": "all",
            "linger.ms": 5, "compression.type": "lz4", "client.id": "signalflow-telecom-producer",
        })

    def _on_delivery(self, err: Any, _msg: Any) -> None:
        if err is not None:
            self.errors.append(str(err))

    def send(self, rec: SourceRecord) -> str:
        topic = self.router.route(rec.event, "telecom")
        headers = [(ARRIVAL_HEADER, rec.arrival_time.encode())] if rec.arrival_time else None
        kwargs: dict[str, Any] = {"key": rec.key, "value": rec.value.encode(), "headers": headers,
                                  "on_delivery": self._on_delivery}
        # The simulated arrival time travels in a header, NOT as the Kafka timestamp: replaying weeks-old data with
        # its original timestamps makes the broker's time-based retention delete it within minutes (seen in testing
        # as Spark's "data may have been lost" failure).
        while True:
            try:
                self._producer.produce(topic, **kwargs)
                break
            except BufferError:  # local queue full: let deliveries drain, then retry
                self._producer.poll(0.5)
        self._producer.poll(0)
        return topic

    def flush(self) -> None:
        remaining = self._producer.flush(60)
        if remaining or self.errors:
            raise RuntimeError(f"telecom producer: {remaining} undelivered, errors: {self.errors[:5]}")


def replay(data_dir: Path, settings: StreamSettings, registry: ContractRegistry, pace_ms: int = 0,
           files: Iterable[Path] | None = None) -> ReplayStats:
    router = Router(settings.topic_prefix, registry)
    producer = TelecomProducer(settings, router)
    stats = ReplayStats()
    with connect_db(settings.erp_db) as erp_conn:
        erp = ErpApp(erp_conn, registry, router)
        for path in files if files is not None else arrival_files(data_dir / "arrivals"):
            for line in path.read_text().splitlines():
                rec = read_line(line)
                if rec.event is None:
                    stats.unparseable += 1
                if rec.domain == "mfg":
                    stats.by_topic[settings.topic_prefix + "." + erp.record(rec.value, rec.event,
                                                                            rec.arrival_time or "")] += 1
                    stats.erp_events += 1
                else:
                    stats.by_topic[producer.send(rec)] += 1
                    stats.telecom_messages += 1
            producer.flush()  # a file is one arrival interval: everything in it is delivered before the next
            stats.files += 1
            if pace_ms:
                time.sleep(pace_ms / 1000)
    log.info("replay finished", extra={"files": stats.files, "telecom": stats.telecom_messages,
                                       "erp": stats.erp_events})
    return stats


def duplicate_storm(data_dir: Path, settings: StreamSettings, registry: ContractRegistry, copies: int = 3,
                    event_type: str = "telecom.recharge") -> int:
    """Chaos: re-send every ``event_type`` record ``copies`` more times (a producer retry loop gone wrong).
    Returns the number of messages sent."""
    router = Router(settings.topic_prefix, registry)
    producer = TelecomProducer(settings, router)
    sent = 0
    for path in arrival_files(data_dir / "arrivals"):
        for line in path.read_text().splitlines():
            rec = read_line(line)
            if isinstance(rec.event, dict) and rec.event.get("event_type") == event_type:
                for _ in range(copies):
                    producer.send(rec)
                    sent += 1
    producer.flush()
    return sent
