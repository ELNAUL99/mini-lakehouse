"""End-to-end integration test (design doc section 10).

Spins up Redpanda, MinIO, and the Iceberg REST catalog with Testcontainers, runs
the gateway -> ingestor path, and queries the resulting Iceberg table with DuckDB.
Skipped unless the optional dependencies and Docker are available, so the default
`make test` stays container-free.
"""

from __future__ import annotations

import pytest

pytest.importorskip("testcontainers")
pytest.importorskip("confluent_kafka")
pytest.importorskip("pyiceberg")

pytestmark = pytest.mark.integration


@pytest.mark.skip(reason="scaffold: wire Testcontainers (Redpanda/MinIO/REST) here")
def test_gateway_to_iceberg_roundtrip():
    """Produce N valid events through the gateway, consume with the ingestor, and
    assert DuckDB reads back N distinct event_ids from bronze.events.

    Steps to implement:
      1. Start Redpanda, MinIO, iceberg-rest containers.
      2. Create the lakehouse bucket + bronze.events table from the contract.
      3. Run the gateway for a fixed count; run the ingestor until lag is zero.
      4. Query with DuckDB's Iceberg extension and assert row/id counts.
    """
