"""Environment-driven settings for the adapters.

Secrets (MinIO/Postgres credentials) come from environment variables or
Kubernetes secrets, never from the repo (design doc section 8). Defaults here are
the local docker-compose values only.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class KafkaSettings:
    bootstrap_servers: str = "redpanda:9092"

    @classmethod
    def from_env(cls) -> KafkaSettings:
        return cls(bootstrap_servers=os.environ.get("ML_KAFKA_BOOTSTRAP", "redpanda:9092"))


@dataclass(frozen=True, slots=True)
class ObjectStoreSettings:
    endpoint: str = "http://minio:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    region: str = "us-east-1"
    bucket: str = "lakehouse"

    @classmethod
    def from_env(cls) -> ObjectStoreSettings:
        return cls(
            endpoint=os.environ.get("ML_S3_ENDPOINT", "http://minio:9000"),
            access_key=os.environ.get("ML_S3_ACCESS_KEY", "minioadmin"),
            secret_key=os.environ.get("ML_S3_SECRET_KEY", "minioadmin"),
            region=os.environ.get("ML_S3_REGION", "us-east-1"),
            bucket=os.environ.get("ML_S3_BUCKET", "lakehouse"),
        )


@dataclass(frozen=True, slots=True)
class CatalogSettings:
    uri: str = "http://iceberg-rest:8181"
    warehouse: str = "s3://lakehouse/warehouse"

    @classmethod
    def from_env(cls) -> CatalogSettings:
        return cls(
            uri=os.environ.get("ML_CATALOG_URI", "http://iceberg-rest:8181"),
            warehouse=os.environ.get("ML_CATALOG_WAREHOUSE", "s3://lakehouse/warehouse"),
        )
