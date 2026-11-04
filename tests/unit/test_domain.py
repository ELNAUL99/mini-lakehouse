"""Unit tests for the pure domain: events, hashing/partitioning, dedupe, contracts."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from hypothesis import given
from hypothesis import strategies as st

from mini_lakehouse.domain.contracts import (
    DatasetContract,
    FieldSpec,
    Schema,
    check_compatibility,
)
from mini_lakehouse.domain.dedupe import dedupe_batch, duplicate_ids, merge_committed_offsets
from mini_lakehouse.domain.events import EventParseError, EventType, parse_event
from mini_lakehouse.domain.hashing import iceberg_bucket, murmur3_x86_32
from mini_lakehouse.domain.partitioning import group_by_partition, partition_for

pytestmark = pytest.mark.unit


def _event(event_id: str, player_id: str = "p1", **kw):
    body = {
        "event_id": event_id,
        "event_type": "session_start",
        "event_time": datetime(2026, 11, 4, 8, 30, tzinfo=UTC).isoformat(),
        "ingest_time": datetime(2026, 11, 4, 8, 30, 5, tzinfo=UTC).isoformat(),
        "player_id": player_id,
        "schema_version": 1,
        "payload": {},
    }
    body.update(kw)
    return parse_event(body)


# -- events ------------------------------------------------------------------
def test_parse_valid_event():
    ev = _event("e1")
    assert ev.event_type is EventType.SESSION_START
    assert ev.event_time.tzinfo is not None
    assert ev.lateness_seconds == pytest.approx(5.0)


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ({"event_type": "nope"}, "unknown_event_type"),
        ({"schema_version": 0}, "bad_schema_version"),
        ({"event_time": "not-a-date"}, "bad_timestamp"),
    ],
)
def test_parse_rejects_with_reason(mutation, reason):
    body = {
        "event_id": "e1",
        "event_type": "purchase",
        "event_time": datetime(2026, 11, 4, tzinfo=UTC).isoformat(),
        "ingest_time": datetime(2026, 11, 4, tzinfo=UTC).isoformat(),
        "player_id": "p1",
        "schema_version": 1,
        "payload": {},
    }
    body.update(mutation)
    with pytest.raises(EventParseError) as exc:
        parse_event(body)
    assert exc.value.reason == reason


def test_parse_missing_field():
    with pytest.raises(EventParseError) as exc:
        parse_event({"event_type": "purchase"})
    assert exc.value.reason == "missing_field"


# -- hashing / bucketing -----------------------------------------------------
def test_murmur3_known_vectors():
    # Canonical Murmur3 x86_32 test vectors (seed 0).
    assert murmur3_x86_32(b"") == 0
    assert murmur3_x86_32(b"hello") == 613153351


def test_iceberg_bucket_is_stable_and_in_range():
    for pid in ("player-1", "player-2", "abc", "zzz"):
        b = iceberg_bucket(pid, 16)
        assert 0 <= b < 16
        assert iceberg_bucket(pid, 16) == b  # deterministic


# -- partitioning ------------------------------------------------------------
def test_partition_for_uses_event_day_and_bucket():
    ev = _event("e1", player_id="player-1")
    key = partition_for(ev, player_buckets=16)
    assert key.event_day == datetime(2026, 11, 4, tzinfo=UTC).date()
    assert 0 <= key.player_bucket < 16


def test_group_by_partition_preserves_order():
    events = [_event(f"e{i}", player_id="same") for i in range(5)]
    groups = group_by_partition(events)
    assert len(groups) == 1
    (only,) = groups.values()
    assert [e.event_id for e in only] == [f"e{i}" for i in range(5)]


# -- dedupe ------------------------------------------------------------------
def test_dedupe_keeps_first_occurrence():
    events = [_event("a"), _event("b"), _event("a"), _event("c"), _event("b")]
    out = dedupe_batch(events)
    assert [e.event_id for e in out] == ["a", "b", "c"]


def test_duplicate_ids():
    events = [_event("a"), _event("a"), _event("b")]
    assert duplicate_ids(events) == {"a"}


@given(ids=st.lists(st.sampled_from(["a", "b", "c", "d"]), min_size=0, max_size=50))
def test_dedupe_output_is_unique_and_order_preserving(ids):
    events = [_event(i) for i in ids]
    out = dedupe_batch(events)
    out_ids = [e.event_id for e in out]
    assert len(out_ids) == len(set(out_ids))  # unique
    first_seen = list(dict.fromkeys(ids))  # first-seen order
    assert out_ids == first_seen


def test_merge_committed_offsets_takes_max():
    merged = merge_committed_offsets({0: 10}, [(0, 5), (0, 12), (1, 3)])
    assert merged == {0: 12, 1: 3}


# -- contracts ---------------------------------------------------------------
def _schema(*fields: FieldSpec, version: int = 1) -> Schema:
    return Schema(version=version, fields=tuple(fields))


def test_adding_optional_field_is_compatible():
    old = _schema(FieldSpec("a", "string"))
    new = _schema(FieldSpec("a", "string"), FieldSpec("platform", "string", required=False))
    assert check_compatibility(old, new) == []


def test_adding_required_field_breaks():
    old = _schema(FieldSpec("a", "string"))
    new = _schema(FieldSpec("a", "string"), FieldSpec("b", "string", required=True))
    assert any("added as required" in v for v in check_compatibility(old, new))


def test_removing_field_breaks():
    old = _schema(FieldSpec("a", "string"), FieldSpec("b", "string"))
    new = _schema(FieldSpec("a", "string"))
    assert any("removed" in v for v in check_compatibility(old, new))


def test_type_change_breaks():
    old = _schema(FieldSpec("a", "string"))
    new = _schema(FieldSpec("a", "long"))
    assert any("changed type" in v for v in check_compatibility(old, new))


def test_contract_validate_catches_key_not_in_schema():
    contract = DatasetContract(
        name="telemetry.events",
        owner="platform",
        key="missing",
        retention_days=7,
        schema=_schema(FieldSpec("event_id", "string")),
    )
    assert any("key" in p for p in contract.validate())
