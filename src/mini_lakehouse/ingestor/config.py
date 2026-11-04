"""Ingestor configuration, loaded from environment variables in ``__main__``."""

from __future__ import annotations

import os
from dataclasses import dataclass

from mini_lakehouse.domain.partitioning import DEFAULT_PLAYER_BUCKETS


@dataclass(frozen=True, slots=True)
class IngestorConfig:
    """Tunables for the consume/commit loop (design doc section 5)."""

    topic: str = "telemetry.events.v1"
    group_id: str = "lakehouse-ingestor"
    table: str = "bronze.events"
    max_batch_records: int = 10_000
    max_batch_seconds: float = 10.0
    player_buckets: int = DEFAULT_PLAYER_BUCKETS
    commit_max_attempts: int = 5
    commit_backoff_base_s: float = 0.1
    commit_backoff_max_s: float = 5.0

    @classmethod
    def from_env(cls) -> IngestorConfig:
        return cls(
            topic=os.environ.get("ML_TOPIC", "telemetry.events.v1"),
            group_id=os.environ.get("ML_GROUP_ID", "lakehouse-ingestor"),
            table=os.environ.get("ML_TABLE", "bronze.events"),
            max_batch_records=int(os.environ.get("ML_MAX_BATCH_RECORDS", "10000")),
            max_batch_seconds=float(os.environ.get("ML_MAX_BATCH_SECONDS", "10")),
            player_buckets=int(os.environ.get("ML_PLAYER_BUCKETS", str(DEFAULT_PLAYER_BUCKETS))),
            commit_max_attempts=int(os.environ.get("ML_COMMIT_MAX_ATTEMPTS", "5")),
        )
