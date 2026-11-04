"""Synthetic game-telemetry generator (design doc F1).

Emits ``session_start``, ``match_end``, and ``purchase`` events with:

* realistic **player skew** -- a small set of heavy players produce most events
  (a simple power-law pick), so partitions and buckets are unevenly loaded the
  way real telemetry is;
* **late events** -- a configurable fraction arrive with an ``event_time`` in the
  recent past (up to ``max_lateness``), exercising late-data partitioning (F9).

The generator is pure: given a seeded RNG and a "now" it returns event dicts, so
the producer and tests share one source of truth for shape.
"""

from __future__ import annotations

import random
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass(frozen=True, slots=True)
class GeneratorConfig:
    num_players: int = 5_000
    skew: float = 1.3            # higher = heavier tail (power-law exponent)
    late_fraction: float = 0.05  # 5% of events are late
    max_lateness_s: int = 3_600  # up to 1h late (<= 24h per F9)
    schema_version: int = 1


class TelemetryGenerator:
    def __init__(self, config: GeneratorConfig | None = None, *, seed: int | None = None) -> None:
        self._cfg = config or GeneratorConfig()
        self._rng = random.Random(seed)
        # Precompute a skewed player-id weighting (Zipf-like).
        self._players = [f"player-{i}" for i in range(self._cfg.num_players)]
        self._weights = [1.0 / ((i + 1) ** self._cfg.skew) for i in range(self._cfg.num_players)]

    def _player(self) -> str:
        return self._rng.choices(self._players, weights=self._weights, k=1)[0]

    def _payload(self, event_type: str) -> dict[str, object]:
        if event_type == "match_end":
            return {"match_id": uuid.uuid4().hex, "duration_s": self._rng.randint(60, 2400)}
        if event_type == "purchase":
            return {
                "item_id": f"item-{self._rng.randint(1, 200)}",
                "price_cents": self._rng.choice([99, 499, 999, 1999]),
            }
        return {"client_version": f"1.{self._rng.randint(0, 9)}.{self._rng.randint(0, 9)}"}

    def one(self, now: datetime | None = None) -> dict[str, object]:
        now = now or datetime.now(UTC)
        event_type = self._rng.choices(
            ["session_start", "match_end", "purchase"], weights=[0.5, 0.35, 0.15], k=1
        )[0]
        if self._rng.random() < self._cfg.late_fraction:
            event_time = now - timedelta(seconds=self._rng.randint(1, self._cfg.max_lateness_s))
        else:
            event_time = now
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_time": event_time.astimezone(UTC).isoformat(),
            "ingest_time": now.astimezone(UTC).isoformat(),
            "player_id": self._player(),
            "schema_version": self._cfg.schema_version,
            "payload": self._payload(event_type),
        }

    def stream(self, count: int, now: datetime | None = None) -> Iterator[dict[str, object]]:
        for _ in range(count):
            yield self.one(now)
