"""In-memory test doubles for the ports.

These let the whole ingestor commit protocol be exercised -- including crashes --
with no containers. Offsets are modelled as Kafka models them: a per-partition
log of records, a committed high-water mark (last processed offset, inclusive),
and a read cursor that resumes from ``committed + 1`` after a restart.
"""

from __future__ import annotations

import itertools
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

from mini_lakehouse.domain.events import EventEnvelope
from mini_lakehouse.domain.partitioning import PartitionKey
from mini_lakehouse.ports.interfaces import (
    CommitConflict,
    CommitResult,
    ConsumedRecord,
    EventSource,
    TableWriter,
)


def make_record(
    *,
    event_id: str,
    player_id: str,
    partition: int,
    offset: int,
    event_type: str = "session_start",
    event_time: str | None = None,
    ingest_time: str | None = None,
    schema_version: int = 1,
    payload: dict | None = None,
) -> ConsumedRecord:
    """Build a well-formed :class:`ConsumedRecord` carrying a valid event envelope."""
    now = datetime(2026, 11, 4, 12, 0, 0, tzinfo=UTC).isoformat()
    body = {
        "event_id": event_id,
        "event_type": event_type,
        "event_time": event_time or now,
        "ingest_time": ingest_time or now,
        "player_id": player_id,
        "schema_version": schema_version,
        "payload": payload or {},
    }
    return ConsumedRecord(
        partition=partition,
        offset=offset,
        key=player_id,
        value=body,
        raw=json.dumps(body).encode("utf-8"),
    )


class FakeEventSource(EventSource):
    """A durable Kafka-like log with commit/seek semantics."""

    def __init__(self, partitions: Mapping[int, list[ConsumedRecord]]) -> None:
        self._log: dict[int, list[ConsumedRecord]] = {p: list(r) for p, r in partitions.items()}
        self._committed: dict[int, int] = {}
        # Cursor = next offset index to read per partition (resume from committed+1).
        self._cursor: dict[int, int] = {p: 0 for p in self._log}
        self._paused: set[int] = set()

    def poll(self, max_records: int, timeout_s: float) -> Sequence[ConsumedRecord]:
        out: list[ConsumedRecord] = []
        for partition, records in self._log.items():
            if partition in self._paused:
                continue
            while self._cursor[partition] < len(records) and len(out) < max_records:
                out.append(records[self._cursor[partition]])
                self._cursor[partition] += 1
            if len(out) >= max_records:
                break
        return out

    def commit(self, offsets: Mapping[int, int]) -> None:
        for partition, offset in offsets.items():
            prev = self._committed.get(partition, -1)
            self._committed[partition] = max(prev, offset)

    def committed(self) -> Mapping[int, int]:
        return dict(self._committed)

    def pause(self, partitions: Sequence[int]) -> None:
        self._paused.update(partitions)

    def resume(self, partitions: Sequence[int]) -> None:
        self._paused.difference_update(partitions)

    def close(self) -> None:  # pragma: no cover - nothing to release
        pass

    # -- test controls -------------------------------------------------------
    def simulate_restart(self) -> None:
        """Rejoin the group: seek each partition back to committed + 1."""
        for partition in self._log:
            self._cursor[partition] = self._committed.get(partition, -1) + 1

    def remaining(self) -> int:
        return sum(len(r) - self._cursor[p] for p, r in self._log.items())


class FakeTableWriter(TableWriter):
    """An Iceberg-like table that commits rows + offsets atomically into a snapshot.

    ``fail_commits`` forces :class:`CommitConflict` on the first N appends to
    exercise the optimistic-retry path.
    """

    def __init__(self, *, fail_commits: int = 0) -> None:
        self.rows: list[EventEnvelope] = []
        self._snapshot_offsets: dict[int, int] = {}
        self._snapshot_seq = itertools.count(1)
        self._fail_commits = fail_commits
        self.snapshots: list[dict[str, object]] = []

    def read_committed_offsets(self) -> Mapping[int, int]:
        return dict(self._snapshot_offsets)

    def append_batch(
        self,
        groups: Mapping[PartitionKey, Sequence[EventEnvelope]],
        kafka_offsets: Mapping[int, int],
        batch_id: str,
    ) -> CommitResult:
        if self._fail_commits > 0:
            self._fail_commits -= 1
            raise CommitConflict(f"forced conflict (batch_id={batch_id})")

        written = 0
        for _key, events in groups.items():
            self.rows.extend(events)
            written += len(events)

        # Atomic: table rows and snapshot offsets move together.
        for partition, offset in kafka_offsets.items():
            prev = self._snapshot_offsets.get(partition, -1)
            self._snapshot_offsets[partition] = max(prev, offset)

        snapshot_id = next(self._snapshot_seq)
        self.snapshots.append(
            {
                "snapshot_id": snapshot_id,
                "batch_id": batch_id,
                "kafka.offsets": dict(self._snapshot_offsets),
                "rows_written": written,
            }
        )
        return CommitResult(
            snapshot_id=snapshot_id,
            rows_written=written,
            committed_offsets=dict(self._snapshot_offsets),
            batch_id=batch_id,
        )

    # -- test assertions -----------------------------------------------------
    def distinct_event_ids(self) -> set[str]:
        return {row.event_id for row in self.rows}


class FakeDeadLetterSink:
    """Collects dead-lettered records with their reason codes."""

    def __init__(self) -> None:
        self.items: list[tuple[ConsumedRecord, str, str]] = []

    def send(self, record: ConsumedRecord, reason: str, detail: str = "") -> None:
        self.items.append((record, reason, detail))

    def reasons(self) -> list[str]:
        return [reason for _rec, reason, _detail in self.items]
