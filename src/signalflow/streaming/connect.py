"""Kafka Connect REST client and the two Debezium connector definitions (outbox events, master-data CDC)."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any

from ..erp import publications
from .settings import StreamSettings
from .topics import MASTER_DATA_TABLES


def _request(method: str, url: str, body: dict[str, Any] | None = None) -> Any:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 (URL comes from local configuration)
        raw = resp.read()
    return json.loads(raw) if raw else None


def outbox_connector(s: StreamSettings) -> tuple[str, dict[str, str]]:
    name = f"{s.topic_prefix}-erp-outbox"
    pub = f"sfx_outbox_{s.slot_suffix}"
    assert pub in publications(s)
    return name, {
        **_common(s),
        "topic.prefix": f"{s.topic_prefix}_erp",
        "slot.name": pub,
        "publication.name": pub,
        "table.include.list": "erp.outbox",
        "snapshot.mode": "no_data",  # registered before the ERP writes anything; every outbox insert is streamed
        "tombstones.on.delete": "false",
        **_heartbeat(1),
        "predicates": "isHeartbeat",
        "predicates.isHeartbeat.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
        "predicates.isHeartbeat.pattern": "__debezium-heartbeat.*",
        "transforms": "outbox",
        "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
        "transforms.outbox.predicate": "isHeartbeat",
        "transforms.outbox.negate": "true",
        "transforms.outbox.table.field.event.id": "id",
        "transforms.outbox.table.field.event.key": "aggregateid",
        "transforms.outbox.table.field.event.payload": "payload",
        "transforms.outbox.table.fields.additional.placement":
            "arrival_time:header:sf_arrival_time,type:header:sf_event_type",
        "transforms.outbox.route.by.field": "aggregatetype",
        "transforms.outbox.route.topic.replacement": f"{s.topic_prefix}.${{routedByValue}}",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    }


def master_data_connector(s: StreamSettings) -> tuple[str, dict[str, str]]:
    name = f"{s.topic_prefix}-erp-master-data"
    pub = f"sfx_master_{s.slot_suffix}"
    return name, {
        **_common(s),
        "topic.prefix": f"{s.topic_prefix}_erpmd",
        "slot.name": pub,
        "publication.name": pub,
        "table.include.list": ",".join(f"erp.{t}" for t in MASTER_DATA_TABLES),
        "snapshot.mode": "initial",  # current master data first (op = r), then every change
        "decimal.handling.mode": "string",
        **_heartbeat(2),
        "transforms": "route",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": r".*\.erp\.(.*)",
        "transforms.route.replacement": f"{s.topic_prefix}.erpcdc.$1",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    }


def _heartbeat(row: int) -> dict[str, str]:
    """Each connector writes its own row of erp.cdc_heartbeat, a table in its publication but not in its include list.
    WAL is shared by every database in the cluster: without a change it can confirm, an idle slot pins all of it
    (the master-data slot once held 6.5 GB on the demo machine). The heartbeat gives it one every few seconds."""
    return {
        "heartbeat.interval.ms": "5000",
        "heartbeat.action.query":
            f"INSERT INTO erp.cdc_heartbeat (id, beat_at) VALUES ({row}, now()) "
            "ON CONFLICT (id) DO UPDATE SET beat_at = EXCLUDED.beat_at",
        "topic.creation.default.replication.factor": "1",  # only the heartbeat topic is created this way
        "topic.creation.default.partitions": "1",
    }


def _common(s: StreamSettings) -> dict[str, str]:
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": s.connect_pg_host,
        "database.port": str(s.connect_pg_port),
        "database.user": s.cdc_user,
        "database.password": "${env:CDC_PASSWORD}",  # resolved inside the worker (EnvVarConfigProvider)
        "database.dbname": s.erp_db,
        "plugin.name": "pgoutput",
        "publication.autocreate.mode": "disabled",  # publications are created by `stream init` as the owner
        "slot.drop.on.stop": "false",
    }


class ConnectClient:
    def __init__(self, url: str):
        self.url = url

    def put_config(self, name: str, config: dict[str, str]) -> None:
        """Create or update (PUT is idempotent)."""
        _request("PUT", f"{self.url}/connectors/{name}/config", config)

    def status(self, name: str) -> dict[str, Any] | None:
        try:
            return _request("GET", f"{self.url}/connectors/{name}/status")  # type: ignore[no-any-return]
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            raise

    def delete(self, name: str, reset_offsets: bool = True) -> bool:
        """Delete a connector. With ``reset_offsets`` its stored source offsets are removed first (stop → delete
        offsets → delete, KIP-875). Without that, a connector re-created under the same name resumes from an LSN
        whose replication slot no longer exists and its task fails ("Last recorded offset is no longer available")."""
        if self.status(name) is None:
            return False
        if reset_offsets:
            _request("PUT", f"{self.url}/connectors/{name}/stop")
            self._wait_state(name, "STOPPED")
            _request("DELETE", f"{self.url}/connectors/{name}/offsets")
        _request("DELETE", f"{self.url}/connectors/{name}")
        return True

    def _wait_state(self, name: str, state: str, timeout_s: float = 60) -> None:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            st = self.status(name)
            if st and st["connector"]["state"] == state:
                return
            time.sleep(0.5)
        raise TimeoutError(f"connector {name} did not reach {state}")

    def wait_running(self, name: str, timeout_s: float = 90) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_s
        last: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            last = self.status(name)
            if last and last["connector"]["state"] == "RUNNING" and last["tasks"] and \
                    all(t["state"] == "RUNNING" for t in last["tasks"]):
                # A task can report RUNNING and fail a moment later (e.g. on a stale offset): confirm it stays up.
                time.sleep(3)
                again = self.status(name)
                if again and all(t["state"] == "RUNNING" for t in again["tasks"]):
                    return again
                last = again
            if last and any(t["state"] == "FAILED" for t in last.get("tasks", [])):
                raise RuntimeError(f"connector {name} failed: {last['tasks'][0].get('trace', '')[:2000]}")
            time.sleep(1)
        raise TimeoutError(f"connector {name} not running after {timeout_s}s: {last}")
