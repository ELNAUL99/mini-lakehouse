"""Kafka/Redpanda adapters (confluent-kafka).

Implements the gateway's :class:`~mini_lakehouse.gateway.service.Producer`, the
ingestor's :class:`~mini_lakehouse.ports.interfaces.EventSource`, and the
:class:`~mini_lakehouse.ports.interfaces.DeadLetterSink`. The ingestor turns
auto-commit OFF and commits offsets itself, after the Iceberg commit.

Producer settings (design doc section 5): acks=all, enable.idempotence=true,
linger.ms=20, compression=zstd -- a little latency for throughput and safe retries.

This module is import-guarded: ``confluent_kafka`` is an optional ``[adapters]``
dependency, so the pure domain and its tests never require it.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

from mini_lakehouse.ports.interfaces import ConsumedRecord

from .settings import KafkaSettings

try:  # pragma: no cover - exercised only with the optional dependency installed
    from confluent_kafka import Consumer, Producer, TopicPartition
except ImportError:  # pragma: no cover
    Consumer = Producer = TopicPartition = None  # type: ignore[assignment,misc]


class KafkaProducer:
    """Idempotent, acks=all producer used by the gateway."""

    def __init__(self, settings: KafkaSettings | None = None) -> None:
        if Producer is None:  # pragma: no cover
            raise RuntimeError("confluent-kafka is not installed; `pip install .[adapters]`")
        s = settings or KafkaSettings.from_env()
        self._p = Producer(
            {
                "bootstrap.servers": s.bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 20,
                "compression.type": "zstd",
            }
        )

    def produce(
        self, topic: str, key: str | None, value: bytes, headers: dict[str, str] | None = None
    ) -> None:
        self._p.produce(
            topic,
            value=value,
            key=key.encode() if key else None,
            headers=list(headers.items()) if headers else None,
        )
        self._p.poll(0)

    def flush(self, timeout_s: float = 10.0) -> None:
        self._p.flush(timeout_s)


class KafkaEventSource:
    """Manual-commit consumer implementing :class:`EventSource`."""

    def __init__(self, topic: str, group_id: str, settings: KafkaSettings | None = None) -> None:
        if Consumer is None:  # pragma: no cover
            raise RuntimeError("confluent-kafka is not installed; `pip install .[adapters]`")
        s = settings or KafkaSettings.from_env()
        self._c = Consumer(
            {
                "bootstrap.servers": s.bootstrap_servers,
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "partition.assignment.strategy": "cooperative-sticky",
            }
        )
        self._topic = topic
        self._c.subscribe([topic])

    def poll(self, max_records: int, timeout_s: float) -> Sequence[ConsumedRecord]:
        out: list[ConsumedRecord] = []
        deadline = timeout_s
        while len(out) < max_records:
            msg = self._c.poll(deadline)
            if msg is None:
                break
            if msg.error():
                continue
            out.append(self._to_record(msg))
            deadline = 0  # drain what is buffered without extra waiting
        return out

    def _to_record(self, msg: Any) -> ConsumedRecord:
        raw = msg.value() or b""
        try:
            value = json.loads(raw)
            if not isinstance(value, dict):
                value = None
        except (ValueError, TypeError):
            value = None
        key = msg.key()
        return ConsumedRecord(
            partition=msg.partition(),
            offset=msg.offset(),
            key=key.decode() if key else None,
            value=value,
            raw=raw,
        )

    def commit(self, offsets: Mapping[int, int]) -> None:
        # Iceberg offsets are "last processed, inclusive"; Kafka commits the NEXT
        # offset to read, so add 1.
        tps = [TopicPartition(self._topic, p, o + 1) for p, o in offsets.items()]
        self._c.commit(offsets=tps, asynchronous=False)

    def committed(self) -> Mapping[int, int]:
        assignment = self._c.assignment()
        if not assignment:
            return {}
        committed = self._c.committed(assignment, timeout=10)
        return {tp.partition: tp.offset - 1 for tp in committed if tp.offset and tp.offset > 0}

    def pause(self, partitions: Sequence[int]) -> None:
        self._c.pause([TopicPartition(self._topic, p) for p in partitions])

    def resume(self, partitions: Sequence[int]) -> None:
        self._c.resume([TopicPartition(self._topic, p) for p in partitions])

    def close(self) -> None:
        self._c.close()


class KafkaDeadLetterSink:
    """Routes rejected records to the DLQ topic with a reason header (F4)."""

    def __init__(self, dlq_topic: str, producer: KafkaProducer | None = None) -> None:
        self._topic = dlq_topic
        self._producer = producer or KafkaProducer()

    def send(self, record: ConsumedRecord, reason: str, detail: str = "") -> None:
        self._producer.produce(
            self._topic,
            key=record.key,
            value=record.raw,
            headers={"reason": reason, "detail": detail},
        )
