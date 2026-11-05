"""Iceberg table writer (PyIceberg 0.12) with offsets-in-snapshot semantics.

This adapter is the real lakehouse write path (design doc section 6):

* **Table provisioning** (:func:`bronze_table_schema`, :meth:`ensure_table`):
  creates ``bronze.events`` with partition spec ``day(event_time)`` +
  ``bucket(16, player_id)``, a ``payload`` **struct** (not a JSON string, so a
  v2 ``payload.platform`` is a real add-column-by-ID), and a sort order on
  ``event_time``.
* **Commit protocol** (:meth:`append_batch`): buffer a micro-batch, build a
  PyArrow table, append (or ``upsert`` keyed on ``event_id`` for true
  cross-batch idempotency), writing ``kafka.offsets`` + ``ingestor.batch_id``
  into the snapshot summary. A commit conflict surfaces as
  :class:`CommitConflict` so the ingestor retries with backoff.
* **Offset recovery** (:meth:`read_committed_offsets`): scans snapshot history
  newest-first so compaction/expiry snapshots (which carry no offsets) never
  erase the resume position.

Import-guarded: PyIceberg/PyArrow are optional ``[adapters]`` dependencies, so
the pure domain and its tests never require them.

NOTE: this code is API-correct against PyIceberg 0.12 but has not been run
against a live REST catalog + MinIO in this environment; see the README
"Verified vs not verified" section.
"""

from __future__ import annotations

import contextlib
from collections.abc import Mapping, Sequence

from mini_lakehouse.domain.events import EventEnvelope
from mini_lakehouse.domain.offsets import (
    BATCH_KEY,
    OFFSETS_KEY,
    encode_offsets,
    pick_committed_offsets,
)
from mini_lakehouse.domain.partitioning import DEFAULT_PLAYER_BUCKETS, PartitionKey

from .settings import CatalogSettings, ObjectStoreSettings

try:  # pragma: no cover - requires the optional dependency
    import pyarrow as pa
    from pyiceberg.catalog import load_catalog
    from pyiceberg.exceptions import (
        CommitFailedException,
        NamespaceAlreadyExistsError,
        NoSuchTableError,
        TableAlreadyExistsError,
    )
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.table.sorting import SortField, SortOrder
    from pyiceberg.transforms import BucketTransform, DayTransform, IdentityTransform
    from pyiceberg.types import (
        IntegerType,
        LongType,
        NestedField,
        StringType,
        StructType,
        TimestamptzType,
    )

    _HAVE_ICEBERG = True
except ImportError:  # pragma: no cover
    pa = None  # type: ignore[assignment]
    load_catalog = None  # type: ignore[assignment]
    CommitFailedException = Exception  # type: ignore[assignment,misc]
    NoSuchTableError = Exception  # type: ignore[assignment,misc]
    _HAVE_ICEBERG = False

from mini_lakehouse.ports.interfaces import CommitConflict, CommitResult

_TARGET_FILE_BYTES = 128 * 1024 * 1024

# Canonical payload fields across event types (all optional). Modelling payload
# as a struct -- rather than a JSON string -- is what makes schema evolution real
# and keeps the column usable for OLAP.
_PAYLOAD_FIELDS: tuple[str, ...] = (
    "match_id",
    "duration_s",
    "item_id",
    "price_cents",
    "client_version",
    "platform",  # added in schema v2; present here as optional
)


def bronze_table_schema():
    """Return (Schema, PartitionSpec, SortOrder) for ``bronze.events``.

    Field IDs are explicit and stable so schema evolution adds columns by ID.
    """
    if not _HAVE_ICEBERG:  # pragma: no cover
        raise RuntimeError("pyiceberg not installed; `pip install .[adapters]`")

    payload_struct = StructType(
        NestedField(101, "match_id", StringType(), required=False),
        NestedField(102, "duration_s", LongType(), required=False),
        NestedField(103, "item_id", StringType(), required=False),
        NestedField(104, "price_cents", LongType(), required=False),
        NestedField(105, "client_version", StringType(), required=False),
        NestedField(106, "platform", StringType(), required=False),
    )
    schema = Schema(
        NestedField(1, "event_id", StringType(), required=True),
        NestedField(2, "event_type", StringType(), required=True),
        NestedField(3, "event_time", TimestamptzType(), required=True),
        NestedField(4, "ingest_time", TimestamptzType(), required=True),
        NestedField(5, "player_id", StringType(), required=True),
        NestedField(6, "schema_version", IntegerType(), required=True),
        NestedField(7, "payload", payload_struct, required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=3, field_id=1000, transform=DayTransform(), name="event_day"),
        PartitionField(
            source_id=5,
            field_id=1001,
            transform=BucketTransform(num_buckets=DEFAULT_PLAYER_BUCKETS),
            name="player_bucket",
        ),
    )
    sort_order = SortOrder(SortField(source_id=3, transform=IdentityTransform()))
    return schema, spec, sort_order


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


def arrow_schema():
    """PyArrow schema matching ``bronze.events`` (required flags and tz preserved)."""
    payload_type = pa.struct(
        [
            pa.field("match_id", pa.string()),
            pa.field("duration_s", pa.int64()),
            pa.field("item_id", pa.string()),
            pa.field("price_cents", pa.int64()),
            pa.field("client_version", pa.string()),
            pa.field("platform", pa.string()),
        ]
    )
    return pa.schema(
        [
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("event_type", pa.string(), nullable=False),
            pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("ingest_time", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("player_id", pa.string(), nullable=False),
            pa.field("schema_version", pa.int32(), nullable=False),
            pa.field("payload", payload_type, nullable=True),
        ]
    )


class IcebergTableWriter:
    """Writes micro-batches to one Iceberg table, carrying Kafka offsets in snapshots.

    ``write_mode`` is ``"append"`` (default, matches the design doc's throughput
    trade-off) or ``"upsert"`` (MERGE on ``event_id`` for true cross-batch
    idempotency at a higher cost).
    """

    def __init__(self, identifier: str, catalog=None, *, write_mode: str = "append") -> None:
        if not _HAVE_ICEBERG:  # pragma: no cover
            raise RuntimeError("pyarrow/pyiceberg not installed; `pip install .[adapters]`")
        if write_mode not in {"append", "upsert"}:
            raise ValueError(f"write_mode must be append|upsert, got {write_mode!r}")
        self._identifier = identifier
        self._catalog = catalog or build_catalog()
        self._write_mode = write_mode

    # -- provisioning --------------------------------------------------------
    def ensure_table(self):
        """Create the namespace and table from the contract if they do not exist."""
        try:
            return self._catalog.load_table(self._identifier)
        except NoSuchTableError:
            pass
        namespace = self._identifier.split(".")[0]
        with contextlib.suppress(NamespaceAlreadyExistsError):  # pragma: no cover - race
            self._catalog.create_namespace(namespace)
        schema, spec, sort_order = bronze_table_schema()
        try:
            return self._catalog.create_table(
                self._identifier,
                schema=schema,
                partition_spec=spec,
                sort_order=sort_order,
                properties={
                    "owner": "data-platform-chapter",
                    "comment": "Validated game telemetry; append-only bronze layer.",
                    "write.target-file-size-bytes": str(_TARGET_FILE_BYTES),
                    "write.parquet.compression-codec": "zstd",
                },
            )
        except TableAlreadyExistsError:  # pragma: no cover - race
            return self._catalog.load_table(self._identifier)

    def _table(self):
        return self._catalog.load_table(self._identifier)

    # -- offsets -------------------------------------------------------------
    def read_committed_offsets(self) -> Mapping[int, int]:
        """Recover offsets by scanning snapshot history newest-first (not just HEAD)."""
        try:
            table = self._table()
        except NoSuchTableError:  # pragma: no cover - fresh table
            return {}
        summaries = [s.summary for s in reversed(table.snapshots())]
        return pick_committed_offsets(summaries)

    # -- writes --------------------------------------------------------------
    def append_batch(
        self,
        groups: Mapping[PartitionKey, Sequence[EventEnvelope]],
        kafka_offsets: Mapping[int, int],
        batch_id: str,
    ) -> CommitResult:
        events = [e for group in groups.values() for e in group]
        table = self._table()
        arrow = self._to_arrow(events)
        props = {OFFSETS_KEY: encode_offsets(kafka_offsets), BATCH_KEY: batch_id}
        try:
            if self._write_mode == "upsert":
                table.upsert(
                    arrow,
                    join_cols=["event_id"],
                    when_matched_update_all=False,
                    when_not_matched_insert_all=True,
                    snapshot_properties=props,
                )
            else:
                table.append(arrow, snapshot_properties=props)
        except CommitFailedException as exc:  # optimistic-concurrency loss
            raise CommitConflict(str(exc)) from exc

        snap = table.refresh().current_snapshot()
        return CommitResult(
            snapshot_id=snap.snapshot_id if snap else 0,
            rows_written=len(events),
            committed_offsets=dict(kafka_offsets),
            batch_id=batch_id,
        )

    # -- maintenance (copy-on-write compaction) ------------------------------
    def compact_partition(self, row_filter: str) -> int:
        """Rewrite one partition's small files into fewer target-sized files.

        Copy-on-write: read the partition via ``scan`` then ``overwrite`` it,
        which drops the many small files and writes fresh ones. The current
        offsets are carried forward into the overwrite snapshot so expiry of the
        original append snapshot cannot lose the resume position. Returns the
        number of rows rewritten. Always pass a partition ``row_filter`` so the
        rewrite is bounded.
        """
        table = self._table()
        rows = table.scan(row_filter=row_filter).to_arrow()
        if rows.num_rows == 0:
            return 0
        current = pick_committed_offsets([s.summary for s in reversed(table.snapshots())])
        table.overwrite(
            rows,
            overwrite_filter=row_filter,
            snapshot_properties={
                OFFSETS_KEY: encode_offsets(current),
                BATCH_KEY: f"compact:{row_filter}",
            },
        )
        return rows.num_rows

    # -- arrow conversion ----------------------------------------------------
    def _to_arrow(self, events: Sequence[EventEnvelope]):
        def payload_row(e: EventEnvelope) -> dict[str, object | None]:
            p = e.payload
            return {name: p.get(name) for name in _PAYLOAD_FIELDS}

        return pa.table(
            {
                "event_id": [e.event_id for e in events],
                "event_type": [e.event_type.value for e in events],
                "event_time": [e.event_time for e in events],
                "ingest_time": [e.ingest_time for e in events],
                "player_id": [e.player_id for e in events],
                "schema_version": [e.schema_version for e in events],
                "payload": [payload_row(e) for e in events],
            },
            schema=arrow_schema(),
        )
