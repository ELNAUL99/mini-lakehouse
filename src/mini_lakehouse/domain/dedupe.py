"""Idempotent dedupe by ``event_id``.

The ingestor's effectively-once guarantee (design doc section 5) has two halves:

1. **Within a micro-batch**: collapse duplicate ``event_id`` values before
   writing, keeping the first occurrence so order is stable.
2. **Across batches / after a crash**: the restart reads the Kafka offsets stored
   in the latest Iceberg snapshot and re-polls from there. Anything already
   committed is skipped by comparing against the committed offsets, so no row is
   written twice.

This module owns (1) and the offset-comparison helper for (2). Both are pure and
heavily property-tested, because this is *the* correctness claim of the project.
"""

from __future__ import annotations

from collections.abc import Iterable

from .events import EventEnvelope


def dedupe_batch(events: Iterable[EventEnvelope]) -> list[EventEnvelope]:
    """Return ``events`` with duplicate ``event_id`` values removed, first-wins.

    Keeping the first occurrence preserves per-player ordering within the batch.
    """
    seen: set[str] = set()
    out: list[EventEnvelope] = []
    for event in events:
        if event.event_id in seen:
            continue
        seen.add(event.event_id)
        out.append(event)
    return out


def duplicate_ids(events: Iterable[EventEnvelope]) -> set[str]:
    """Return the set of ``event_id`` values that appear more than once."""
    counts: dict[str, int] = {}
    for event in events:
        counts[event.event_id] = counts.get(event.event_id, 0) + 1
    return {eid for eid, n in counts.items() if n > 1}


def skip_already_committed(
    events: list[EventEnvelope],
    offsets: list[tuple[int, int]],
    committed: dict[int, int],
) -> list[EventEnvelope]:
    """Drop events whose Kafka offset is <= the last committed offset for its partition.

    This is the cross-batch half of effectively-once recovery. ``events`` and
    ``offsets`` are parallel lists (``offsets[i]`` is ``(partition, offset)`` of
    ``events[i]``); ``committed`` maps partition -> last committed offset read
    from the latest Iceberg snapshot summary. An event is kept only if its offset
    is strictly greater than the committed high-water mark for its partition.
    """
    if len(events) != len(offsets):
        raise ValueError("events and offsets must be the same length")

    kept: list[EventEnvelope] = []
    for event, (partition, offset) in zip(events, offsets, strict=True):
        high_water = committed.get(partition)
        if high_water is not None and offset <= high_water:
            continue
        kept.append(event)
    return kept


def merge_committed_offsets(
    current: dict[int, int], batch_offsets: list[tuple[int, int]]
) -> dict[int, int]:
    """Fold a batch's ``(partition, offset)`` pairs into the committed high-water map.

    Returns a new dict mapping partition -> max offset seen. Stored in the Iceberg
    snapshot summary as ``kafka.offsets`` so a restart knows exactly where to resume.
    """
    merged = dict(current)
    for partition, offset in batch_offsets:
        existing = merged.get(partition)
        if existing is None or offset > existing:
            merged[partition] = offset
    return merged
