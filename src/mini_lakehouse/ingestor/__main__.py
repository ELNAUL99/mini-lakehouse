"""Ingestor entrypoint: wire Kafka + Iceberg adapters into the commit loop.

Run with ``python -m mini_lakehouse.ingestor`` or the ``ml-ingestor`` script.
"""

from __future__ import annotations

import logging
import signal

from .config import IngestorConfig
from .core import Ingestor

log = logging.getLogger("ingestor")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = IngestorConfig.from_env()

    # Optional-dependency adapters imported lazily so the domain stays import-light.
    from mini_lakehouse.adapters.iceberg import IcebergTableWriter
    from mini_lakehouse.adapters.kafka import KafkaDeadLetterSink, KafkaEventSource

    source = KafkaEventSource(cfg.topic, cfg.group_id)
    writer = IcebergTableWriter(cfg.table)
    writer.ensure_table()  # create namespace + table from the contract if absent
    dlq = KafkaDeadLetterSink("telemetry.events.dlq")
    ingestor = Ingestor(source, writer, dlq, cfg)

    stopping = {"flag": False}

    def _stop(*_a: object) -> None:  # pragma: no cover
        log.info("signal received; finishing current batch")
        stopping["flag"] = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    log.info("ingestor consuming %s -> %s", cfg.topic, cfg.table)
    try:
        ingestor.run_forever(should_stop=lambda: stopping["flag"])
    finally:
        source.close()
        m = ingestor.metrics
        log.info(
            "stopped: batches=%d written=%d dups=%d dlq=%d retries=%d",
            m.batches,
            m.events_written,
            m.duplicates_dropped,
            m.dead_lettered,
            m.commit_retries,
        )


if __name__ == "__main__":  # pragma: no cover
    main()
