"""Partition assignment for ``bronze.events``.

Design doc section 6:

* Partition by **event time**, not ingest time: ``day(event_time)`` so queries
  that filter on when things happened prune partitions, and late events land in
  the correct (old) day.
* Bucket on ``player_id`` into 16 buckets to spread writes and speed per-player
  lookups without creating millions of partitions.

This module is pure: given an event it returns the partition key. The ingestor
groups a micro-batch by this key before writing Parquet files.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from .events import EventEnvelope
from .hashing import iceberg_bucket

DEFAULT_PLAYER_BUCKETS = 16


@dataclass(frozen=True, slots=True)
class PartitionKey:
    """The physical partition an event belongs to in ``bronze.events``.

    ``event_day`` is the ``day(event_time)`` value; ``player_bucket`` is the
    ``bucket(N, player_id)`` value. Equality/hash make this usable as a dict key
    when grouping a batch.
    """

    event_day: date
    player_bucket: int

    def path_fragment(self) -> str:
        """Human-readable fragment for logs and staging paths (not the storage layout).

        Iceberg owns the real on-disk layout via hidden partitioning; this is
        only for diagnostics.
        """
        return f"event_day={self.event_day.isoformat()}/player_bucket={self.player_bucket}"


def partition_for(
    event: EventEnvelope, *, player_buckets: int = DEFAULT_PLAYER_BUCKETS
) -> PartitionKey:
    """Return the :class:`PartitionKey` for an event."""
    return PartitionKey(
        event_day=event.event_time.date(),
        player_bucket=iceberg_bucket(event.player_id, player_buckets),
    )


def group_by_partition(
    events: list[EventEnvelope], *, player_buckets: int = DEFAULT_PLAYER_BUCKETS
) -> dict[PartitionKey, list[EventEnvelope]]:
    """Group events by partition so each partition becomes one (or few) Parquet files.

    Order within a partition is preserved, which keeps a player's events ordered
    (they share a bucket because they share a ``player_id``).
    """
    groups: dict[PartitionKey, list[EventEnvelope]] = {}
    for event in events:
        key = partition_for(event, player_buckets=player_buckets)
        groups.setdefault(key, []).append(event)
    return groups
