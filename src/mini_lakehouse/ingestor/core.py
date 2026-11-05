"""The ingestor's micro-batch consume/commit loop.

This is the core correctness machinery of the project, written against the ports
so it runs identically against in-memory fakes (tests) and Redpanda + Iceberg
(production). One iteration of :meth:`Ingestor.run_once` is:

1. Poll a micro-batch (up to ``max_batch_records`` or ``max_batch_seconds``).
2. Read the committed Kafka offsets from the latest Iceberg snapshot summary.
3. Parse each record; route undecodable/invalid records to the DLQ with a reason.
4. **Skip already-committed** records (offset <= snapshot high-water) -- this is
   the cross-batch half of crash recovery.
5. **Dedupe** the remaining records by ``event_id`` (within-batch, first-wins).
6. Group by partition and **append + commit atomically** to Iceberg, storing the
   batch's end offsets in the snapshot summary. Retry on commit conflict.
7. Only *after* the Iceberg commit succeeds, commit offsets back to Kafka.

If the process dies between (6) and (7), the restart reads the offsets from the
snapshot in step (2) and skips what is already written in step (4). The result is
at-least-once delivery with idempotent output -- effectively-once in the table.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from mini_lakehouse.domain.dedupe import merge_committed_offsets
from mini_lakehouse.domain.events import EventEnvelope, EventParseError, parse_event
from mini_lakehouse.domain.partitioning import PartitionKey, group_by_partition
from mini_lakehouse.ports.interfaces import (
    CommitConflict,
    CommitResult,
    ConsumedRecord,
    DeadLetterSink,
    EventSource,
    TableWriter,
)

from .config import IngestorConfig


@dataclass(frozen=True, slots=True)
class BatchOutcome:
    """What one call to :meth:`Ingestor.run_once` did; drives metrics and tests."""

    polled: int
    written: int
    deduped: int
    dead_lettered: int
    skipped_already_committed: int
    commit_attempts: int
    snapshot_id: int | None
    batch_id: str | None

    @classmethod
    def empty(cls) -> BatchOutcome:
        return cls(0, 0, 0, 0, 0, 0, None, None)


@dataclass
class IngestMetrics:
    """Cumulative counters exposed by the service (Prometheus in the adapter)."""

    batches: int = 0
    events_written: int = 0
    duplicates_dropped: int = 0
    dead_lettered: int = 0
    commit_retries: int = 0


class Ingestor:
    """Owns the consume/commit loop. Pure orchestration over the ports."""

    def __init__(
        self,
        source: EventSource,
        writer: TableWriter,
        dlq: DeadLetterSink,
        config: IngestorConfig | None = None,
        *,
        sleep: Callable[[float], None] = time.sleep,
        new_batch_id: Callable[[], str] = lambda: uuid.uuid4().hex,
    ) -> None:
        self._source = source
        self._writer = writer
        self._dlq = dlq
        self._cfg = config or IngestorConfig()
        self._sleep = sleep
        self._new_batch_id = new_batch_id
        self.metrics = IngestMetrics()

    # -- one micro-batch -----------------------------------------------------
    def run_once(self) -> BatchOutcome:
        records = list(
            self._source.poll(self._cfg.max_batch_records, self._cfg.max_batch_seconds)
        )
        if not records:
            return BatchOutcome.empty()

        committed = dict(self._writer.read_committed_offsets())

        parsed: list[tuple[EventEnvelope, int, int]] = []
        dead = 0
        for rec in records:
            event = self._parse_or_dlq(rec)
            if event is None:
                dead += 1
                continue
            parsed.append((event, rec.partition, rec.offset))

        # (4) cross-batch skip: anything at or below the snapshot high-water is
        # already durably written, so drop it.
        survivors: list[tuple[EventEnvelope, int, int]] = []
        skipped = 0
        for event, partition, offset in parsed:
            high_water = committed.get(partition)
            if high_water is not None and offset <= high_water:
                skipped += 1
                continue
            survivors.append((event, partition, offset))

        # (5) within-batch dedupe by event_id, first-wins (order preserved).
        seen: set[str] = set()
        kept: list[EventEnvelope] = []
        duplicates = 0
        for event, _partition, _offset in survivors:
            if event.event_id in seen:
                duplicates += 1
                continue
            seen.add(event.event_id)
            kept.append(event)

        # The batch's end offsets cover EVERY polled record (written, deduped,
        # and dead-lettered alike): all of them are "handled", so a restart must
        # resume strictly after them.
        all_offsets = [(rec.partition, rec.offset) for rec in records]
        batch_end_offsets = merge_committed_offsets(committed, all_offsets)

        batch_id = self._new_batch_id()
        snapshot_id: int | None = None
        attempts = 0

        if kept:
            groups = group_by_partition(kept, player_buckets=self._cfg.player_buckets)
            result, attempts = self._append_with_retry(
                groups, batch_end_offsets, batch_id, records
            )
            if result is None:
                # Commit conflict exhausted: the whole batch went to the DLQ.
                self.metrics.dead_lettered += len(records)
                self.metrics.commit_retries += attempts
                return BatchOutcome(
                    polled=len(records),
                    written=0,
                    deduped=duplicates,
                    dead_lettered=dead + len(records),
                    skipped_already_committed=skipped,
                    commit_attempts=attempts,
                    snapshot_id=None,
                    batch_id=batch_id,
                )
            snapshot_id = result.snapshot_id
            # (7) Only now is it safe to advance Kafka offsets.
            self._source.commit(dict(result.committed_offsets))
            self.metrics.events_written += result.rows_written
        else:
            # Nothing to write (all duplicates/invalid). Still advance Kafka so we
            # do not re-poll them. A crash before this commit simply re-DLQs the
            # invalid records on restart (DLQ is keyed by event_id, so benign).
            self._source.commit(batch_end_offsets)

        self.metrics.batches += 1
        self.metrics.duplicates_dropped += duplicates
        self.metrics.dead_lettered += dead
        self.metrics.commit_retries += max(attempts - 1, 0)

        return BatchOutcome(
            polled=len(records),
            written=len(kept),
            deduped=duplicates,
            dead_lettered=dead,
            skipped_already_committed=skipped,
            commit_attempts=attempts,
            snapshot_id=snapshot_id,
            batch_id=batch_id,
        )

    def run_forever(self, *, should_stop: Callable[[], bool] = lambda: False) -> None:
        """Drive :meth:`run_once` until ``should_stop`` returns True."""
        while not should_stop():
            self.run_once()

    # -- helpers -------------------------------------------------------------
    def _parse_or_dlq(self, rec: ConsumedRecord) -> EventEnvelope | None:
        if rec.value is None:
            self._dlq.send(rec, "decode_error", "record value was not valid JSON")
            return None
        try:
            return parse_event(rec.value)
        except EventParseError as exc:
            self._dlq.send(rec, exc.reason, exc.detail)
            return None

    def _append_with_retry(
        self,
        groups: Mapping[PartitionKey, Sequence[EventEnvelope]],
        batch_end_offsets: dict[int, int],
        batch_id: str,
        records: Sequence[ConsumedRecord],
    ) -> tuple[CommitResult | None, int]:
        """Append with optimistic-concurrency retry (design doc section 6, step 3).

        Returns ``(CommitResult, attempts)`` on success, or ``(None, attempts)``
        after the whole batch was routed to the DLQ because retries were exhausted.
        """
        cfg = self._cfg
        last_attempt = 0
        for attempt in range(1, cfg.commit_max_attempts + 1):
            last_attempt = attempt
            try:
                result = self._writer.append_batch(groups, batch_end_offsets, batch_id)
                return result, attempt
            except CommitConflict:
                if attempt >= cfg.commit_max_attempts:
                    break
                backoff = min(
                    cfg.commit_backoff_base_s * (2 ** (attempt - 1)),
                    cfg.commit_backoff_max_s,
                )
                self._sleep(backoff)
        # Exhausted: route the batch to the DLQ and alert via the reason code.
        for rec in records:
            self._dlq.send(rec, "commit_conflict_exhausted", f"batch_id={batch_id}")
        return None, last_attempt
