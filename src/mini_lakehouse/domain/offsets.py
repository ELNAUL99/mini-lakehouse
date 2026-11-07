"""Kafka-offset recovery from Iceberg snapshot summaries (pure).

The ingestor stores the committed Kafka offsets in the snapshot summary under
``kafka.offsets`` on every *data* commit. But not every snapshot is a data commit:
compaction (an ``overwrite``) and snapshot expiry create snapshots too, and if
recovery read only ``current_snapshot()`` it could land on one of those and lose
the offsets, replaying -- and duplicating -- already-written events.

The fix is to scan snapshots newest-first and take the offsets from the most
recent snapshot that carries them. This logic is pure and unit-tested; the
adapter just feeds it the summaries from ``table.snapshots()``.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence

OFFSETS_KEY = "kafka.offsets"
BATCH_KEY = "ingestor.batch_id"


def parse_offsets(raw: str | None) -> dict[int, int]:
    """Parse the JSON ``{partition: offset}`` map stored in a snapshot summary."""
    if not raw:
        return {}
    return {int(p): int(o) for p, o in json.loads(raw).items()}


def encode_offsets(offsets: Mapping[int, int]) -> str:
    """Encode a ``{partition: offset}`` map for a snapshot summary."""
    return json.dumps({str(p): o for p, o in offsets.items()})


def pick_committed_offsets(
    summaries_newest_first: Sequence[Mapping[str, str] | None],
) -> dict[int, int]:
    """Return offsets from the most recent snapshot summary that carries them.

    ``summaries_newest_first`` is the snapshot summaries ordered newest to oldest.
    Returns ``{}`` if no snapshot carries ``kafka.offsets`` (fresh table).
    """
    for summary in summaries_newest_first:
        if not summary:
            continue
        raw = summary.get(OFFSETS_KEY)
        if raw:
            return parse_offsets(raw)
    return {}
