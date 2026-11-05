"""API-correctness tests for the Iceberg adapter.

These do NOT hit a live catalog -- they assert that the schema/partition-spec we
build is valid PyIceberg, that the struct-payload Arrow conversion is right, and
that offset recovery scans snapshot history. Skipped if PyIceberg is absent.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

pytest.importorskip("pyiceberg")
pytest.importorskip("pyarrow")

from mini_lakehouse.adapters.iceberg import (
    IcebergTableWriter,
    arrow_schema,
    bronze_table_schema,
)
from mini_lakehouse.domain.events import parse_event
from mini_lakehouse.domain.offsets import OFFSETS_KEY, encode_offsets

pytestmark = pytest.mark.unit


def _event(event_id, event_type="purchase", payload=None):
    now = datetime(2026, 11, 4, 12, tzinfo=UTC).isoformat()
    return parse_event(
        {
            "event_id": event_id,
            "event_type": event_type,
            "event_time": now,
            "ingest_time": now,
            "player_id": "p1",
            "schema_version": 2,
            "payload": payload or {},
        }
    )


def test_bronze_schema_is_valid_and_partitioned():
    schema, spec, sort_order = bronze_table_schema()
    names = [f.name for f in schema.fields]
    assert names == [
        "event_id", "event_type", "event_time", "ingest_time",
        "player_id", "schema_version", "payload",
    ]
    # payload is a real struct, so v2 platform is an add-column-by-ID, not JSON.
    payload_field = schema.fields[-1]
    assert payload_field.field_type.__class__.__name__ == "StructType"
    assert {f.name for f in payload_field.field_type.fields} >= {"platform", "item_id"}
    # Partitioned by day(event_time) + bucket(16, player_id).
    part_names = [f.name for f in spec.fields]
    assert part_names == ["event_day", "player_bucket"]
    assert len(sort_order.fields) == 1


def test_to_arrow_builds_struct_payload():
    writer = IcebergTableWriter("bronze.events", catalog=SimpleNamespace())
    events = [
        _event("a", "purchase", {"item_id": "item-1", "price_cents": 999}),
        _event("b", "match_end", {"match_id": "m1", "duration_s": 120, "platform": "ios"}),
    ]
    tbl = writer._to_arrow(events)
    assert tbl.schema == arrow_schema()
    assert tbl.num_rows == 2
    payloads = tbl.column("payload").to_pylist()
    assert payloads[0]["item_id"] == "item-1"
    assert payloads[1]["platform"] == "ios"
    assert payloads[0]["platform"] is None  # absent field -> null, not an error


def test_read_committed_offsets_scans_history():
    # Newest snapshot is a compaction (no offsets); recovery must fall through.
    snaps = [
        SimpleNamespace(summary={"operation": "append", OFFSETS_KEY: encode_offsets({0: 40})}),
        SimpleNamespace(
            summary={"operation": "append", OFFSETS_KEY: encode_offsets({0: 100, 1: 7})}
        ),
        SimpleNamespace(summary={"operation": "overwrite"}),  # compaction, no offsets
    ]
    fake_table = SimpleNamespace(snapshots=lambda: snaps)
    fake_catalog = SimpleNamespace(load_table=lambda _ident: fake_table)
    writer = IcebergTableWriter("bronze.events", catalog=fake_catalog)
    assert writer.read_committed_offsets() == {0: 100, 1: 7}
