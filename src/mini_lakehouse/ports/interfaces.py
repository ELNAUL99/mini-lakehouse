"""Ports: the typed boundary between the domain and the outside world.

The ingestor depends only on these Protocols, never on Kafka or Iceberg directly.
That is what lets the whole commit protocol be tested with in-memory fakes (see
``tests/chaos``) and still run unchanged against Redpanda + Iceberg in production.

Four ports cover the ingestor's needs:

* :class:`EventSource`    - a Kafka-like consumer (poll, commit, pause/resume).
* :class:`TableWriter`    - an Iceberg table that commits atomically and stores
                            the Kafka offsets in its snapshot summary.
* :class:`DeadLetterSink` - where invalid or unwritable events go, with a reason.
* :class:`Catalog`        - table/namespace provisioning for the Platform API.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from mini_lakehouse.domain.events import EventEnvelope
from mini_lakehouse.domain.partitioning import PartitionKey


@dataclass(frozen=True, slots=True)
class ConsumedRecord:
    """A raw record pulled from the broker, before parsing.

    ``value`` is the decoded JSON object (dict) or ``None`` if it could not be
    decoded; the raw bytes are kept so the DLQ can store exactly what arrived.
    """

    partition: int
    offset: int
    key: str | None
    value: Mapping[str, Any] | None
    raw: bytes


@dataclass(frozen=True, slots=True)
class CommitResult:
    """The outcome of an Iceberg commit."""

    snapshot_id: int
    rows_written: int
    committed_offsets: Mapping[int, int]
    batch_id: str


@runtime_checkable
class EventSource(Protocol):
    """A Kafka-style consumer. Auto-commit is OFF; the ingestor commits explicitly."""

    def poll(self, max_records: int, timeout_s: float) -> Sequence[ConsumedRecord]:
        """Return up to ``max_records`` records, or fewer after ``timeout_s`` seconds."""
        ...

    def commit(self, offsets: Mapping[int, int]) -> None:
        """Commit per-partition offsets (last processed offset per partition)."""
        ...

    def committed(self) -> Mapping[int, int]:
        """Return the consumer group's last committed offsets per partition."""
        ...

    def pause(self, partitions: Sequence[int]) -> None:
        """Pause fetching for ``partitions`` (backpressure)."""
        ...

    def resume(self, partitions: Sequence[int]) -> None:
        """Resume fetching for ``partitions``."""
        ...

    def close(self) -> None:
        """Flush and release the consumer; called on rebalance/shutdown."""
        ...


@runtime_checkable
class TableWriter(Protocol):
    """An Iceberg table writer with offsets-in-snapshot semantics."""

    def read_committed_offsets(self) -> Mapping[int, int]:
        """Read ``kafka.offsets`` from the latest snapshot summary (empty on first run)."""
        ...

    def append_batch(
        self,
        groups: Mapping[PartitionKey, Sequence[EventEnvelope]],
        kafka_offsets: Mapping[int, int],
        batch_id: str,
    ) -> CommitResult:
        """Write the partitioned batch and commit atomically.

        The commit records ``kafka.offsets`` and ``ingestor.batch_id`` in the
        snapshot summary. Raises :class:`CommitConflict` if another writer touched
        the table; the caller refreshes and retries.
        """
        ...


@runtime_checkable
class DeadLetterSink(Protocol):
    """Where rejected events go, with a reason code (design doc F4)."""

    def send(self, record: ConsumedRecord, reason: str, detail: str = "") -> None:
        ...


@runtime_checkable
class Catalog(Protocol):
    """Iceberg catalog operations used by the Platform API to provision datasets."""

    def namespace_exists(self, namespace: str) -> bool: ...
    def create_namespace(self, namespace: str, properties: Mapping[str, str]) -> None: ...
    def table_exists(self, identifier: str) -> bool: ...
    def create_table(self, identifier: str, schema: Any, properties: Mapping[str, str]) -> None: ...


class CommitConflict(Exception):
    """Raised by a :class:`TableWriter` when an optimistic Iceberg commit loses a race."""
