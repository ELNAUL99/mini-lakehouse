# ADR-0002: Offsets-in-snapshot, not Kafka transactions

- Status: accepted
- Date: 2026-11-04

## Context

We want effectively-once output in Iceberg. Iceberg commits are not part of a
Kafka transaction, so a Kafka transaction alone cannot make "write to Iceberg" and
"commit offset" atomic.

## Decision

Store the batch's Kafka offsets in the **Iceberg snapshot summary**
(`kafka.offsets`) and commit the snapshot *before* committing offsets to Kafka. On
restart, read the offsets from the latest snapshot and skip any redelivered record
at or below that high-water mark. Dedupe within a batch by `event_id`.

## Consequences

- At-least-once delivery + idempotent output = effectively-once **in the table**,
  proven by the kill-and-count chaos test.
- No dependency on Kafka transactions or a stream processor.
- The ordering (Iceberg commit → Kafka commit) is load-bearing; it is encoded in
  `ingestor/core.py` and asserted by `tests/chaos/test_exactly_once.py`.
- Flink would be the production choice at scale; this keeps the moving parts
  visible and explainable.
