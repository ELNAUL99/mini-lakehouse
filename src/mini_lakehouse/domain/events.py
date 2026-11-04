"""The event envelope and event types.

This is pure data + parsing with no I/O. Validation against the versioned JSON
Schema is the gateway's job (see :mod:`mini_lakehouse.gateway.validation`); here
we only model the envelope and provide a strict structural parser used by both
the gateway and the ingestor so the two agree on the shape.

Envelope (schema v1), from the design doc section 4:

    event_id        UUID (v7)     globally unique; the idempotency key
    event_type      string        session_start | match_end | purchase
    event_time      timestamp UTC when it happened on the client
    ingest_time     timestamp UTC set by the gateway; used for late-data metrics
    player_id       string        Kafka message key, so a player's events stay ordered
    schema_version  int           drives validation and evolution
    payload         struct        type-specific fields

Schema v2 adds optional ``payload.platform`` (string). Nothing in this module
needs to change for that: the payload is carried as an opaque mapping and the
evolution is handled by the contract rules and Iceberg's add-column-by-ID.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class EventType(StrEnum):
    """The three telemetry event types."""

    SESSION_START = "session_start"
    MATCH_END = "match_end"
    PURCHASE = "purchase"


class EventParseError(ValueError):
    """Raised when a raw record cannot be parsed into an :class:`EventEnvelope`.

    The ``reason`` is a short, stable code suitable for the DLQ (design doc F4).
    """

    def __init__(self, reason: str, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason}: {detail}" if detail else reason)


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    """An immutable, validated-shape telemetry event.

    ``event_id`` is the idempotency key used for dedupe; ``player_id`` is the
    Kafka partition key; ``event_time`` drives partitioning.
    """

    event_id: str
    event_type: EventType
    event_time: datetime
    ingest_time: datetime
    player_id: str
    schema_version: int
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not self.event_id:
            raise EventParseError("missing_event_id")
        if not self.player_id:
            raise EventParseError("missing_player_id")
        if self.event_time.tzinfo is None or self.ingest_time.tzinfo is None:
            raise EventParseError("naive_timestamp", "timestamps must be timezone-aware (UTC)")
        if self.schema_version < 1:
            raise EventParseError("bad_schema_version", str(self.schema_version))

    @property
    def lateness_seconds(self) -> float:
        """How late the event arrived, i.e. ingest_time - event_time, in seconds.

        Negative values (clock skew where the client clock is ahead) are clamped
        to 0 so late-data metrics stay meaningful.
        """
        delta = (self.ingest_time - self.event_time).total_seconds()
        return max(delta, 0.0)


def _parse_timestamp(value: Any, field: str) -> datetime:
    """Parse an ISO-8601 string or epoch-seconds number into a UTC datetime."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        text = value.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError as exc:
            raise EventParseError("bad_timestamp", f"{field}={value!r}") from exc
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=UTC)
    else:
        raise EventParseError("bad_timestamp", f"{field} has type {type(value).__name__}")

    # Normalise to timezone-aware UTC; a naive value is assumed to be UTC.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def parse_event(raw: Mapping[str, Any]) -> EventEnvelope:
    """Parse a decoded JSON object into an :class:`EventEnvelope`.

    Raises :class:`EventParseError` with a DLQ-friendly ``reason`` on any
    structural problem. This is deliberately strict about required top-level
    fields; richer field-level validation belongs to the JSON Schema.
    """
    if not isinstance(raw, Mapping):
        raise EventParseError("not_an_object", type(raw).__name__)

    try:
        event_type_raw = raw["event_type"]
        event_id = raw["event_id"]
        player_id = raw["player_id"]
        schema_version = raw["schema_version"]
        event_time = raw["event_time"]
        ingest_time = raw["ingest_time"]
    except KeyError as exc:
        raise EventParseError("missing_field", str(exc.args[0])) from exc

    try:
        event_type = EventType(event_type_raw)
    except ValueError as exc:
        raise EventParseError("unknown_event_type", str(event_type_raw)) from exc

    if not isinstance(schema_version, int) or isinstance(schema_version, bool):
        raise EventParseError("bad_schema_version", repr(schema_version))

    payload = raw.get("payload", {})
    if not isinstance(payload, Mapping):
        raise EventParseError("bad_payload", type(payload).__name__)

    return EventEnvelope(
        event_id=str(event_id),
        event_type=event_type,
        event_time=_parse_timestamp(event_time, "event_time"),
        ingest_time=_parse_timestamp(ingest_time, "ingest_time"),
        player_id=str(player_id),
        schema_version=schema_version,
        payload=dict(payload),
    )
