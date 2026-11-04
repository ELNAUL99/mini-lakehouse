"""The test that matters most (design doc section 10).

Produce a known set of ``event_id``s across partitions, run the ingestor while
killing it at random points, and assert the table holds *exactly* those ids with
zero duplicates. This proves the effectively-once claim with no containers: the
"crash" discards the in-memory batch state and the Kafka offset commit, keeping
only what Iceberg committed (the fake writer persists rows + snapshot offsets).
"""

from __future__ import annotations

import random

import pytest

from fakes import FakeDeadLetterSink, FakeEventSource, FakeTableWriter, make_record
from mini_lakehouse.ingestor.config import IngestorConfig
from mini_lakehouse.ingestor.core import Ingestor

pytestmark = pytest.mark.chaos


def _build_log(num_events: int, num_partitions: int):
    """Spread ``num_events`` unique ids across ``num_partitions`` Kafka partitions."""
    partitions: dict[int, list] = {p: [] for p in range(num_partitions)}
    for i in range(num_events):
        p = i % num_partitions
        offset = len(partitions[p])
        partitions[p].append(
            make_record(
                event_id=f"evt-{i:07d}",
                player_id=f"player-{i % 500}",
                partition=p,
                offset=offset,
            )
        )
    return partitions


@pytest.mark.parametrize("seed", [1, 7, 42])
def test_exactly_once_under_random_kills(seed):
    rng = random.Random(seed)
    num_events = 5_000
    num_partitions = 4
    expected_ids = {f"evt-{i:07d}" for i in range(num_events)}

    source = FakeEventSource(_build_log(num_events, num_partitions))
    writer = FakeTableWriter()
    dlq = FakeDeadLetterSink()

    # Small batches so many commits happen and kills land in interesting places.
    cfg = IngestorConfig(max_batch_records=rng.choice([50, 100, 250]), max_batch_seconds=0.0)

    kills_remaining = 3
    safety = 0
    while source.remaining() > 0 and safety < 100_000:
        safety += 1
        ingestor = Ingestor(source, writer, dlq, cfg, sleep=lambda _s: None)
        # Run a random number of batches, then "crash".
        runs_before_kill = rng.randint(1, 8)
        for _ in range(runs_before_kill):
            if source.remaining() == 0:
                break
            ingestor.run_once()
        if kills_remaining > 0 and source.remaining() > 0:
            kills_remaining -= 1
            # Crash: drop in-memory state + uncommitted Kafka offsets; Iceberg persists.
            source.simulate_restart()

    # The core guarantees:
    assert writer.distinct_event_ids() == expected_ids  # nothing lost
    assert len(writer.rows) == len(expected_ids)  # zero duplicates in the table
    assert dlq.items == []  # all events were valid


def test_duplicate_delivery_after_crash_is_not_rewritten():
    """A record redelivered after a crash (already in a snapshot) is skipped, not rewritten."""
    log = _build_log(num_events=300, num_partitions=2)
    source = FakeEventSource(log)
    writer = FakeTableWriter()
    cfg = IngestorConfig(max_batch_records=50, max_batch_seconds=0.0)

    Ingestor(source, writer, FakeDeadLetterSink(), cfg, sleep=lambda _s: None).run_once()
    first_count = len(writer.rows)

    # Force redelivery of everything committed so far.
    source.simulate_restart()
    ing = Ingestor(source, writer, FakeDeadLetterSink(), cfg, sleep=lambda _s: None)
    while source.remaining() > 0:
        ing.run_once()

    assert len(writer.rows) == 300
    assert len(writer.distinct_event_ids()) == 300
    assert first_count <= 300
