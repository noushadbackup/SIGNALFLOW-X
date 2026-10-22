"""Lakehouse configuration, read only from environment variables (see .env.example). Secrets (PGPASSWORD, S3 keys)
are passed to Spark in memory and redacted from its logs; they are never written to files."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .. import REPO_ROOT

CATALOG = "lake"  # Spark catalog name == Iceberg JDBC catalog_name == Trino catalog name


@dataclass(frozen=True)
class LakeSettings:
    catalog_db: str
    bucket: str
    s3_endpoint: str      # as seen from where Spark runs (the host, or a container on the compose network)
    s3_region: str
    pg_jdbc_host: str     # how Spark reaches Postgres
    pg_port: int
    namespace_prefix: str  # isolates test lakes inside one catalog: "<prefix>bronze", "<prefix>silver", ...
    trino_host: str
    trino_port: int
    jars_dir: Path

    @classmethod
    def from_env(cls) -> LakeSettings:
        env = os.environ.get
        return cls(
            catalog_db=env("SIGNALFLOW_CATALOG_DB", "signalflow_catalog"),
            bucket=env("SIGNALFLOW_LAKE_BUCKET", "signalflow-lake"),
            s3_endpoint=env("SIGNALFLOW_S3_ENDPOINT", "http://127.0.0.1:58333"),
            s3_region=env("SIGNALFLOW_S3_REGION", "us-east-1"),
            pg_jdbc_host=env("SIGNALFLOW_JDBC_PGHOST", env("PGHOST", "127.0.0.1")),
            pg_port=int(env("PGPORT", "5432")),
            namespace_prefix=env("SIGNALFLOW_LAKE_PREFIX", ""),
            trino_host=env("SIGNALFLOW_TRINO_HOST", "127.0.0.1"),
            trino_port=int(env("SIGNALFLOW_TRINO_PORT", env("TRINO_PORT", "58080"))),
            jars_dir=Path(env("SIGNALFLOW_LAKE_JARS", str(REPO_ROOT / ".cache" / "artifacts" / "lake-jars"))),
        )

    @property
    def warehouse(self) -> str:
        return f"s3://{self.bucket}/warehouse"

    def namespace(self, layer: str) -> str:
        return f"{self.namespace_prefix}{layer}"

    def jdbc_url(self, database: str) -> str:
        return f"jdbc:postgresql://{self.pg_jdbc_host}:{self.pg_port}/{database}"
