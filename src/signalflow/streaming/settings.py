"""Streaming configuration, read only from environment variables (see .env.example). Credentials stay in libpq
variables (PG*) or in the Kafka Connect worker's environment; nothing secret is held here."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .. import REPO_ROOT


@dataclass(frozen=True)
class StreamSettings:
    bootstrap: str
    connect_url: str
    topic_prefix: str
    stream_db: str
    erp_db: str
    cdc_user: str
    connect_pg_host: str  # how the Connect container reaches Postgres (compose service name)
    connect_pg_port: int
    partitions: int
    state_dir: Path
    jars_dir: Path

    @classmethod
    def from_env(cls) -> StreamSettings:
        env = os.environ.get
        return cls(
            bootstrap=env("SIGNALFLOW_KAFKA_BOOTSTRAP", "127.0.0.1:59092"),
            connect_url=env("SIGNALFLOW_CONNECT_URL", "http://127.0.0.1:58083").rstrip("/"),
            topic_prefix=env("SIGNALFLOW_TOPIC_PREFIX", "signalflow"),
            stream_db=env("SIGNALFLOW_STREAM_DB", "signalflow_stream"),
            erp_db=env("SIGNALFLOW_ERP_DB", "signalflow_erp"),
            cdc_user=env("SIGNALFLOW_CDC_USER", "signalflow_cdc"),
            connect_pg_host=env("SIGNALFLOW_CONNECT_PGHOST", "postgres"),
            connect_pg_port=int(env("SIGNALFLOW_CONNECT_PGPORT", "5432")),
            partitions=int(env("SIGNALFLOW_TOPIC_PARTITIONS", "3")),
            state_dir=Path(env("SIGNALFLOW_STATE_DIR", str(REPO_ROOT / ".cache" / "state"))),
            jars_dir=Path(env("SIGNALFLOW_SPARK_JARS", str(REPO_ROOT / ".cache" / "artifacts" / "jars"))),
        )

    @property
    def slot_suffix(self) -> str:
        """Replication slot and publication names must be valid identifiers and unique per prefix."""
        return "".join(c if c.isalnum() else "_" for c in self.topic_prefix.lower())
