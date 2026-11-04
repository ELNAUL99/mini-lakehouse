"""Unit tests for the gateway: schema validation, generator, and routing."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from mini_lakehouse.gateway.generator import GeneratorConfig, TelemetryGenerator
from mini_lakehouse.gateway.service import Gateway
from mini_lakehouse.gateway.validation import SchemaRegistry

pytestmark = pytest.mark.unit


class FakeProducer:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str | None, bytes, dict | None]] = []

    def produce(self, topic, key, value, headers=None):
        self.messages.append((topic, key, value, headers))

    def flush(self, timeout_s=10.0):
        pass


def _valid_event(**kw):
    e = {
        "event_id": "e1",
        "event_type": "purchase",
        "event_time": datetime(2026, 11, 4, tzinfo=UTC).isoformat(),
        "ingest_time": datetime(2026, 11, 4, tzinfo=UTC).isoformat(),
        "player_id": "p1",
        "schema_version": 1,
        "payload": {"item_id": "item-1", "price_cents": 999},
    }
    e.update(kw)
    return e


def test_schema_registry_accepts_valid_v1():
    assert SchemaRegistry().validate(_valid_event()).ok


def test_schema_registry_accepts_valid_v2_with_platform():
    ev = _valid_event(schema_version=2, payload={"platform": "ios"})
    assert SchemaRegistry().validate(ev).ok


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ({"event_type": "nope"}, "schema_violation"),
        ({"schema_version": 99}, "unknown_schema_version"),
        ({"player_id": ""}, "schema_violation"),
    ],
)
def test_schema_registry_rejects(mutation, reason):
    outcome = SchemaRegistry().validate(_valid_event(**mutation))
    assert not outcome.ok
    assert outcome.reason == reason


def test_gateway_routes_valid_to_topic_and_invalid_to_dlq():
    producer = FakeProducer()
    gw = Gateway(SchemaRegistry(), producer, topic="t", dlq_topic="dlq")

    assert gw.submit(_valid_event()) is True
    assert gw.submit(_valid_event(event_type="bad")) is False

    topics = [m[0] for m in producer.messages]
    assert topics == ["t", "dlq"]
    # DLQ message carries the reason header.
    assert producer.messages[1][3]["reason"] == "schema_violation"
    assert gw.stats.accepted == 1 and gw.stats.rejected == 1


def test_gateway_keys_by_player_id():
    producer = FakeProducer()
    gw = Gateway(SchemaRegistry(), producer, topic="t", dlq_topic="dlq")
    gw.submit(_valid_event(player_id="pX"))
    assert producer.messages[0][1] == "pX"


def test_generator_produces_valid_events():
    gen = TelemetryGenerator(GeneratorConfig(num_players=50), seed=1)
    registry = SchemaRegistry()
    now = datetime(2026, 11, 4, 12, tzinfo=UTC)
    for event in gen.stream(200, now=now):
        assert registry.validate(event).ok
        assert json.loads(json.dumps(event))  # JSON-serialisable


def test_generator_emits_some_late_events():
    gen = TelemetryGenerator(GeneratorConfig(num_players=50, late_fraction=0.5), seed=2)
    now = datetime(2026, 11, 4, 12, tzinfo=UTC)
    late = 0
    for event in gen.stream(300, now=now):
        if event["event_time"] < event["ingest_time"]:
            late += 1
    assert late > 0
