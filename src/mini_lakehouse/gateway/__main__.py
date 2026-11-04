"""Gateway entrypoint: generate synthetic telemetry, validate, produce to Kafka.

Run with ``python -m mini_lakehouse.gateway`` or the ``ml-gateway`` script. Rate,
topic, and schema version come from the environment so the same image serves the
demo producer and the load generator (design doc F1).
"""

from __future__ import annotations

import logging
import os
import time

from .generator import GeneratorConfig, TelemetryGenerator
from .service import Gateway
from .validation import SchemaRegistry

log = logging.getLogger("gateway")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    rate = int(os.environ.get("ML_EVENTS_PER_SEC", "5000"))
    topic = os.environ.get("ML_TOPIC", "telemetry.events.v1")
    dlq = os.environ.get("ML_DLQ_TOPIC", "telemetry.events.dlq")
    schema_version = int(os.environ.get("ML_SCHEMA_VERSION", "1"))

    from mini_lakehouse.adapters.kafka import KafkaProducer  # local import: optional dep

    gen = TelemetryGenerator(GeneratorConfig(schema_version=schema_version))
    gw = Gateway(SchemaRegistry(), KafkaProducer(), topic=topic, dlq_topic=dlq)

    log.info("gateway producing %d events/s to %s", rate, topic)
    interval = 1.0 / rate if rate > 0 else 0.0
    sent = 0
    start = time.monotonic()
    try:
        while True:
            gw.submit(gen.one())
            sent += 1
            if interval:
                time.sleep(interval)
            if sent % 10_000 == 0:
                gw.flush()
                elapsed = time.monotonic() - start
                log.info("sent=%d accepted=%d rejected=%d (%.0f ev/s)",
                         sent, gw.stats.accepted, gw.stats.rejected, sent / elapsed)
    except KeyboardInterrupt:  # pragma: no cover
        log.info("shutting down; flushing")
        gw.flush()


if __name__ == "__main__":  # pragma: no cover
    main()
