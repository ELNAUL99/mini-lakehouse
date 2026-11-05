"""Unit tests for Kafka-offset recovery from Iceberg snapshot summaries.

These lock in the fix for the "offsets read only from the current snapshot" bug:
a compaction or expiry snapshot (no offsets) must not erase the resume position.
"""

from __future__ import annotations

import pytest

from mini_lakehouse.domain.offsets import (
    OFFSETS_KEY,
    encode_offsets,
    parse_offsets,
    pick_committed_offsets,
)

pytestmark = pytest.mark.unit


def test_parse_and_encode_roundtrip():
    offsets = {0: 10, 3: 42}
    assert parse_offsets(encode_offsets(offsets)) == offsets


def test_parse_empty():
    assert parse_offsets(None) == {}
    assert parse_offsets("") == {}


def test_pick_skips_snapshots_without_offsets():
    # Newest-first: a compaction snapshot (no offsets) sits on top of the real
    # data commit. Recovery must fall through to the data commit's offsets.
    summaries = [
        {"operation": "overwrite", "ingestor.batch_id": "compact:event_day = '2026-11-04'"},
        {"operation": "append", OFFSETS_KEY: encode_offsets({0: 100, 1: 55})},
        {"operation": "append", OFFSETS_KEY: encode_offsets({0: 40})},
    ]
    assert pick_committed_offsets(summaries) == {0: 100, 1: 55}


def test_pick_handles_none_summaries():
    summaries = [None, {"operation": "append", OFFSETS_KEY: encode_offsets({2: 7})}]
    assert pick_committed_offsets(summaries) == {2: 7}


def test_pick_returns_empty_on_fresh_table():
    assert pick_committed_offsets([]) == {}
    assert pick_committed_offsets([{"operation": "append"}]) == {}
