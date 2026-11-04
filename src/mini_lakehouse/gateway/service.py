"""Gateway service: validate, then produce to Kafka or route to the DLQ.

Keyed by ``player_id`` so a player's events share a partition and stay ordered
(design doc section 5). The producer is a small Protocol so the service is tested
with a fake and runs unchanged against confluent-kafka.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from .validation import SchemaRegistry


@runtime_checkable
class Producer(Protocol):
    def produce(
        self,
        topic: str,
        key: str | None,
        value: bytes,
        headers: dict[str, str] | None = None,
    ) -> None: ...
    def flush(self, timeout_s: float = 10.0) -> None: ...


@dataclass
class GatewayStats:
    accepted: int = 0
    rejected: int = 0


class Gateway:
    def __init__(
        self,
        registry: SchemaRegistry,
        producer: Producer,
        *,
        topic: str = "telemetry.events.v1",
        dlq_topic: str = "telemetry.events.dlq",
    ) -> None:
        self._registry = registry
        self._producer = producer
        self._topic = topic
        self._dlq_topic = dlq_topic
        self.stats = GatewayStats()

    def submit(self, event: dict[str, object]) -> bool:
        """Validate and produce one event. Returns True if it reached the main topic."""
        outcome = self._registry.validate(event)
        payload = json.dumps(event, separators=(",", ":")).encode("utf-8")
        key = event.get("player_id")
        key_str = str(key) if key is not None else None

        if not outcome.ok:
            self._producer.produce(
                self._dlq_topic,
                key=str(event.get("event_id", "")) or None,
                value=payload,
                headers={"reason": outcome.reason, "detail": outcome.detail},
            )
            self.stats.rejected += 1
            return False

        self._producer.produce(self._topic, key=key_str, value=payload)
        self.stats.accepted += 1
        return True

    def flush(self, timeout_s: float = 10.0) -> None:
        self._producer.flush(timeout_s)
