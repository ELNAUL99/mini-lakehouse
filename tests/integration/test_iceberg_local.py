"""Effectively-once against a REAL Iceberg table (no containers).

This runs the real commit protocol -- real PyIceberg writing real Parquet to a
local filesystem warehouse via a SqlCatalog -- instead of the in-memory fake.
It is the container-free half of "turn designed into ran": table creation with
the partition spec, struct-payload appends, offset recovery from snapshot
history, and the kill-and-count exactly-once guarantee, all against Iceberg.

Skipped unless PyIceberg + pyiceberg-core + SQLAlchemy are installed
(``pip install .[adapters] sqlalchemy``).
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyiceberg")
pytest.importorskip("pyiceberg_core")  # Rust transforms; required for partitioned writes
pytest.importorskip("sqlalchemy")

from pyiceberg.catalog.sql import SqlCatalog

from fakes import FakeDeadLetterSink, FakeEventSource, make_record
from mini_lakehouse.adapters.iceberg import IcebergTableWriter
from mini_lakehouse.domain.events import parse_event
from mini_lakehouse.domain.partitioning import group_by_partition
from mini_lakehouse.ingestor.config import IngestorConfig
from mini_lakehouse.ingestor.core import Ingestor

pytestmark = pytest.mark.integration


@pytest.fixture
def writer(tmp_path):
    cat = SqlCatalog(
        "local",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"file://{tmp_path}",
    )
    w = IcebergTableWriter("bronze.events", cat)
    w.ensure_table()
    return w


def _log(num_events: int, num_partitions: int):
    parts: dict[int, list] = {p: [] for p in range(num_partitions)}
    for i in range(num_events):
        p = i % num_partitions
        parts[p].append(
            make_record(
                event_id=f"evt-{i:05d}",
                player_id=f"player-{i % 50}",
                partition=p,
                offset=len(parts[p]),
            )
        )
    return parts


def test_table_is_provisioned_with_spec_and_struct(writer):
    tbl = writer._table()
    assert [f.name for f in tbl.spec().fields] == ["event_day", "player_bucket"]
    assert tbl.schema().find_field("payload").field_type.__class__.__name__ == "StructType"


def test_append_recovers_offsets_from_snapshot(writer):
    recs = [make_record(event_id=f"e{i}", player_id="p", partition=0, offset=i) for i in range(5)]
    events = [parse_event(r.value) for r in recs]
    writer.append_batch(group_by_partition(events), {0: 4}, "b1")
    assert dict(writer.read_committed_offsets()) == {0: 4}


def test_effectively_once_against_real_iceberg(writer):
    num_events, num_partitions = 300, 2
    expected = {f"evt-{i:05d}" for i in range(num_events)}
    source = FakeEventSource(_log(num_events, num_partitions))
    dlq = FakeDeadLetterSink()
    cfg = IngestorConfig(max_batch_records=50, max_batch_seconds=0.0)

    kills = 2
    guard = 0
    while source.remaining() > 0 and guard < 10_000:
        guard += 1
        ing = Ingestor(source, writer, dlq, cfg, sleep=lambda _s: None)
        for _ in range(3):
            if source.remaining() == 0:
                break
            ing.run_once()
        if kills > 0 and source.remaining() > 0:
            kills -= 1
            source.simulate_restart()  # crash: drop uncommitted Kafka offsets

    scan = writer._table().scan().to_arrow()
    ids = set(scan.column("event_id").to_pylist())
    assert ids == expected          # nothing lost
    assert scan.num_rows == len(expected)  # zero duplicates in the real table
    assert dlq.items == []
