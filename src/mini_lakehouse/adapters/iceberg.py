"""Iceberg table writer (PyIceberg) with offsets-in-snapshot semantics.

Commit protocol (design doc section 6):

1. Buffer a micro-batch, build a PyArrow table.
2. ``append`` + commit atomically through the REST catalog, writing
   ``kafka.offsets`` and ``ingestor.batch_id`` into the snapshot summary.
3. On a commit conflict, PyIceberg raises; we surface
   :class:`CommitConflict` so the ingestor refreshes and retries with backoff.

Import-guarded: PyIceberg/PyArrow are optional ``[adapters]`` dependencies.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence

from mini_lakehouse.domain.events import EventEnvelope
from mini_lakehouse.domain.partitioning import PartitionKey
from mini_lakehouse.ports.interfaces import CommitConflict, CommitResult

from .settings import CatalogSettings, ObjectStoreSettings

try:  # pragma: no cover - requires the optional dependency
    import pyarrow as pa
    from pyiceberg.catalog import load_catalog
    from pyiceberg.exceptions import CommitFailedException
except ImportError:  # pragma: no cover
    pa = None  # type: ignore[assignment]
    load_catalog = None  # type: ignore[assignment]
    CommitFailedException = Exception  # type: ignore[assignment,misc]

_OFFSETS_KEY = "kafka.offsets"
_BATCH_KEY = "ingestor.batch_id"


def build_catalog(catalog: CatalogSettings | None = None, store: ObjectStoreSettings | None = None):
    """Load the Iceberg REST catalog pointed at MinIO/S3 (design doc section 6)."""
    if load_catalog is None:  # pragma: no cover
        raise RuntimeError("pyiceberg is not installed; `pip install .[adapters]`")
    c = catalog or CatalogSettings.from_env()
    s = store or ObjectStoreSettings.from_env()
    return load_catalog(
        "rest",
        **{
            "uri": c.uri,
            "warehouse": c.warehouse,
            "s3.endpoint": s.endpoint,
            "s3.access-key-id": s.access_key,
            "s3.secret-access-key": s.secret_key,
            "s3.region": s.region,
        },
    )


class IcebergTableWriter:
    """Writes micro-batches to one Iceberg table, carrying Kafka offsets in snapshots."""

    def __init__(self, identifier: str, catalog=None) -> None:
        if pa is None:  # pragma: no cover
            raise RuntimeError("pyarrow/pyiceberg not installed; `pip install .[adapters]`")
        self._identifier = identifier
        self._catalog = catalog or build_catalog()

    def _table(self):
        return self._catalog.load_table(self._identifier)

    def read_committed_offsets(self) -> Mapping[int, int]:
        snap = self._table().current_snapshot()
        if snap is None:
            return {}
        raw = (snap.summary.additional_properties or {}).get(_OFFSETS_KEY)
        if not raw:
            return {}
        return {int(p): int(o) for p, o in json.loads(raw).items()}

    def append_batch(
        self,
        groups: Mapping[PartitionKey, Sequence[EventEnvelope]],
        kafka_offsets: Mapping[int, int],
        batch_id: str,
    ) -> CommitResult:
        events = [e for group in groups.values() for e in group]
        table = self._table()
        arrow = self._to_arrow(events)
        try:
            table.append(
                arrow,
                snapshot_properties={
                    _OFFSETS_KEY: json.dumps({str(p): o for p, o in kafka_offsets.items()}),
                    _BATCH_KEY: batch_id,
                },
            )
        except CommitFailedException as exc:  # optimistic-concurrency loss
            raise CommitConflict(str(exc)) from exc

        snap = table.refresh().current_snapshot()
        return CommitResult(
            snapshot_id=snap.snapshot_id if snap else 0,
            rows_written=len(events),
            committed_offsets=dict(kafka_offsets),
            batch_id=batch_id,
        )

    def _to_arrow(self, events: Sequence[EventEnvelope]):
        return pa.table(
            {
                "event_id": [e.event_id for e in events],
                "event_type": [e.event_type.value for e in events],
                "event_time": [e.event_time for e in events],
                "ingest_time": [e.ingest_time for e in events],
                "player_id": [e.player_id for e in events],
                "schema_version": [e.schema_version for e in events],
                "payload": [json.dumps(dict(e.payload), separators=(",", ":")) for e in events],
            }
        )
