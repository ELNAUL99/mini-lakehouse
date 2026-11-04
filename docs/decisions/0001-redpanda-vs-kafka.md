# ADR-0001: Redpanda locally, Apache Kafka in production

- Status: accepted
- Date: 2026-11-04

## Context

We need a Kafka-API broker that starts fast on a laptop and has one moving part,
without giving up the option to run real Apache Kafka in production.

## Decision

Use **Redpanda** locally. The application code uses only the Kafka protocol
(`confluent-kafka`), so moving to Apache Kafka is a config change, not a code
change.

## Consequences

- One binary, no ZooKeeper/KRaft ceremony, quick `make up`.
- Client code (`adapters/kafka.py`) is broker-agnostic.
- Broker-specific tuning (replication factor 3, rack awareness) is documented for
  production but not exercised locally (RF=1).
