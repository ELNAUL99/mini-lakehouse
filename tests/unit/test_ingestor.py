"""Unit tests for the ingestor commit protocol using in-memory fakes."""

from __future__ import annotations

import json

import pytest

from fakes import FakeDeadLetterSink, FakeEventSource, FakeTableWriter, make_record
from mini_lakehouse.ingestor.config import IngestorConfig
from mini_lakehouse.ingestor.core import Ingestor
from mini_lakehouse.ports.interfaces import ConsumedRecord

pytestmark = pytest.mark.unit

CFG = IngestorConfig(max_batch_records=1000, max_batch_seconds=0.0)


def _ingestor(source, writer, dlq):
    return Ingestor(source, writer, dlq, CFG, sleep=lambda _s: None)


def test_run_once_writes_and_advances_offsets():
    recs = [
        make_record(event_id="a", player_id="p1", partition=0, offset=0),
        make_record(event_id="b", player_id="p2", partition=0, offset=1),
    ]
    source = FakeEventSource({0: recs})
    writer = FakeTableWriter()
    dlq = FakeDeadLetterSink()
    outcome = _ingestor(source, writer, dlq).run_once()

    assert outcome.written == 2
    assert writer.distinct_event_ids() == {"a", "b"}
    assert source.committed() == {0: 1}
    assert writer.read_committed_offsets() == {0: 1}


def test_within_batch_duplicates_are_dropped():
    recs = [
        make_record(event_id="a", player_id="p1", partition=0, offset=0),
        make_record(event_id="a", player_id="p1", partition=0, offset=1),
        make_record(event_id="b", player_id="p1", partition=0, offset=2),
    ]
    source = FakeEventSource({0: recs})
    writer = FakeTableWriter()
    outcome = _ingestor(source, writer, FakeDeadLetterSink()).run_once()

    assert outcome.written == 2
    assert outcome.deduped == 1
    assert writer.distinct_event_ids() == {"a", "b"}
    assert source.committed() == {0: 2}  # offsets still advance past the dup


def test_invalid_record_goes_to_dlq_with_reason():
    bad = ConsumedRecord(partition=0, offset=0, key="p1", value={"event_type": "x"}, raw=b"{}")
    undecodable = ConsumedRecord(partition=0, offset=1, key=None, value=None, raw=b"\xff")
    good = make_record(event_id="a", player_id="p1", partition=0, offset=2)
    source = FakeEventSource({0: [bad, undecodable, good]})
    writer = FakeTableWriter()
    dlq = FakeDeadLetterSink()
    outcome = _ingestor(source, writer, dlq).run_once()

    assert outcome.written == 1
    assert outcome.dead_lettered == 2
    assert "decode_error" in dlq.reasons()
    assert source.committed() == {0: 2}


def test_commit_conflict_is_retried_then_succeeds():
    recs = [make_record(event_id="a", player_id="p1", partition=0, offset=0)]
    source = FakeEventSource({0: recs})
    writer = FakeTableWriter(fail_commits=2)  # two conflicts, third succeeds
    outcome = _ingestor(source, writer, FakeDeadLetterSink()).run_once()

    assert outcome.written == 1
    assert outcome.commit_attempts == 3
    assert writer.distinct_event_ids() == {"a"}


def test_commit_conflict_exhausted_routes_batch_to_dlq():
    recs = [make_record(event_id="a", player_id="p1", partition=0, offset=0)]
    source = FakeEventSource({0: recs})
    writer = FakeTableWriter(fail_commits=99)
    dlq = FakeDeadLetterSink()
    cfg = IngestorConfig(max_batch_records=1000, commit_max_attempts=5)
    outcome = Ingestor(source, writer, dlq, cfg, sleep=lambda _s: None).run_once()

    assert outcome.written == 0
    assert "commit_conflict_exhausted" in dlq.reasons()
    assert writer.rows == []


def test_record_raw_is_json_roundtrippable():
    rec = make_record(event_id="a", player_id="p1", partition=0, offset=0)
    assert json.loads(rec.raw)["event_id"] == "a"
