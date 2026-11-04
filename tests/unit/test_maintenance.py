"""Unit tests for pure maintenance health metrics."""

from __future__ import annotations

import pytest

from mini_lakehouse.maintenance.health import SMALL_FILE_BYTES, compute_health

pytestmark = pytest.mark.unit

MB = 1024 * 1024


def test_empty_table_is_healthy():
    report = compute_health([], {})
    assert report.file_count == 0
    assert not report.needs_compaction


def test_many_small_files_need_compaction():
    report = compute_health([1 * MB] * 100, {"d1": 100})
    assert report.small_file_ratio == 1.0
    assert report.needs_compaction


def test_large_files_do_not_need_compaction():
    report = compute_health([130 * MB] * 10, {"d1": 10})
    assert report.small_file_ratio == 0.0
    assert not report.needs_compaction


def test_partition_skew_detected():
    report = compute_health([SMALL_FILE_BYTES] * 30, {"hot": 20, "a": 5, "b": 5})
    assert report.partition_skew > 1.0


def test_as_row_shape():
    row = compute_health([1 * MB, 200 * MB], {"d": 2}).as_row()
    assert set(row) == {
        "file_count",
        "total_bytes",
        "avg_file_bytes",
        "small_file_ratio",
        "partition_skew",
        "needs_compaction",
    }
