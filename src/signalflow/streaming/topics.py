"""Topic naming and routing. One topic per event type and major contract version, plus one ``unrouted`` topic per
domain for records whose type or version is not registered (or that cannot be parsed at all). Routing only reads
the envelope's ``event_type`` and ``schema_version``; validating the record is Silver's job, not the producer's."""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from ..contracts import ContractRegistry

DOMAINS = ("telecom", "mfg")
MASTER_DATA_TABLES = ("dealers", "products", "warehouses")


def event_topic(prefix: str, event_type: str, schema_version: str) -> str:
    major = schema_version.split(".", 1)[0]
    return f"{prefix}.{event_type}.v{major}"


def unrouted_topic(prefix: str, domain: str) -> str:
    return f"{prefix}.{domain}.unrouted.v1"


def master_data_topic(prefix: str, table: str) -> str:
    return f"{prefix}.erpcdc.{table}"


def events_pattern(prefix: str) -> str:
    """Regex for Spark's ``subscribePattern``: every event topic of both domains (master data excluded)."""
    return rf"{re.escape(prefix)}\.(telecom|mfg)\..+"


def registered_majors(registry: ContractRegistry) -> set[tuple[str, str]]:
    return {(et, version.split(".", 1)[0]) for et, version in registry.contracts}


class Router:
    def __init__(self, prefix: str, registry: ContractRegistry):
        self.prefix = prefix
        self._majors = registered_majors(registry)

    def route(self, event: Any, default_domain: str) -> str:
        """Topic for a parsed record; ``event`` is None (or not a dict) when the record could not be parsed."""
        if isinstance(event, dict):
            et, version = event.get("event_type"), event.get("schema_version")
            if isinstance(et, str) and isinstance(version, str) and (et, version.split(".", 1)[0]) in self._majors:
                return event_topic(self.prefix, et, version)
            if isinstance(et, str) and et.split(".", 1)[0] in DOMAINS:
                return unrouted_topic(self.prefix, et.split(".", 1)[0])
        return unrouted_topic(self.prefix, default_domain)


def all_event_topics(prefix: str, registry: ContractRegistry) -> list[str]:
    topics = {f"{prefix}.{et}.v{major}" for et, major in registered_majors(registry)}
    topics |= {unrouted_topic(prefix, d) for d in DOMAINS}
    return sorted(topics)


def create_topics(bootstrap: str, topics: Iterable[str], partitions: int, replication: int = 1) -> list[str]:
    """Create missing topics; return the ones created. Existing topics are left untouched."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=15).topics)
    wanted = [t for t in topics if t not in existing]
    if not wanted:
        return []
    futures = admin.create_topics([NewTopic(t, num_partitions=partitions, replication_factor=replication,
                                            config={"cleanup.policy": "delete", "retention.ms": str(7 * 86_400_000)})
                                   for t in wanted])
    for f in futures.values():
        f.result(timeout=30)
    return wanted


def delete_topics(bootstrap: str, prefix: str) -> list[str]:
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap})
    doomed = [t for t in admin.list_topics(timeout=15).topics if t == prefix or t.startswith(prefix + ".")]
    if doomed:
        for f in admin.delete_topics(doomed).values():
            f.result(timeout=30)
    return doomed
