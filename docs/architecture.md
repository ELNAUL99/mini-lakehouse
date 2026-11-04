# Architecture

Telemetry is validated at the gateway, buffered in Kafka, written to Iceberg by
the ingestor, and read, maintained, or provisioned by three services that share
only the catalog and the tables.

## Layers

| Layer          | Choice                         | Why                                               | Production alternative          |
|----------------|--------------------------------|---------------------------------------------------|---------------------------------|
| Broker         | Redpanda                       | Single binary, Kafka API, fast local start        | Apache Kafka (same client code) |
| Object storage | MinIO                          | S3 API on a laptop                                | S3 or GCS                       |
| Table format   | Apache Iceberg v2              | Open format, hidden partitioning, schema evolution | Same                            |
| Catalog        | Iceberg REST on Postgres       | Engine-neutral source of truth for DuckDB + Trino | Polaris / Nessie / Glue         |
| Ingest runtime | Python consumer + PyIceberg    | Matches the role; keeps the commit protocol visible | Flink / Spark Structured Streaming |
| Query          | DuckDB (+ optional Trino)      | Zero-ops OLAP + a distributed engine to compare    | Trino / Spark cluster           |
| Platform API   | FastAPI + Postgres             | Typed, self-documenting API for internal customers | Same                            |
| Packaging      | Docker Compose, then kind      | Reproducible locally; cloud-native shape           | Managed Kubernetes              |

## Principles

- **Hexagonal core.** Domain logic has no I/O and is tested without containers.
  Kafka, Iceberg and Postgres sit behind ports.
- **Independent processes.** Ingestion, maintenance and the API scale separately;
  a maintenance failure never blocks ingestion.
- **Contracts at the edges.** Schemas are versioned and checked on the way in;
  compatibility is checked on the way out.
- **Open formats only.** Everything stored is Parquet + Iceberg, so any engine
  can read it.

## The commit protocol (effectively-once)

One micro-batch:

1. Poll up to 10,000 records or 10 s.
2. Read `kafka.offsets` from the latest Iceberg snapshot summary.
3. Parse; route undecodable/invalid records to the DLQ with a reason code.
4. **Skip** records whose offset ≤ the snapshot high-water (cross-batch dedupe).
5. **Dedupe** the rest by `event_id` (within-batch, first-wins).
6. Group by partition and **append + commit atomically**, writing `kafka.offsets`
   and `ingestor.batch_id` into the snapshot summary. Retry on commit conflict
   with exponential backoff (up to 5), then DLQ + alert.
7. Only after the Iceberg commit, commit offsets back to Kafka.

A crash between (6) and (7) replays the batch; step (4) skips what is already
written. The result is effectively-once **in the table**. See
[ADR-0002](decisions/0002-offsets-in-snapshot.md).

## Failure modes

| Failure                                   | Response                                               | Test                          |
|-------------------------------------------|--------------------------------------------------------|-------------------------------|
| Ingestor crashes mid-batch                | Resume from snapshot offsets; orphans cleaned later    | kill -9 under load, count ids |
| Crash between Iceberg and Kafka commit    | Dedupe + snapshot offsets skip it                      | inject crash at that point    |
| Broker restart                            | Idempotent producer retries; consumer resumes          | restart broker under load     |
| Iceberg commit conflict                   | Refresh, retry with backoff, then DLQ + alert          | two writers on one table      |
| Object storage unavailable                | Pause consumption, retry; Kafka retains data           | stop MinIO 60 s               |
| Poison message                            | DLQ with reason code; pipeline continues               | malformed JSON                |
| Breaking schema change                    | Contract check rejects before deploy                   | CI test with breaking contract|
| Small-file explosion                      | Compaction + alert when avg file < 16 MB               | burst, watch health metrics   |

## Observability

Prometheus metrics (consumer lag, batch size, commit latency, events/s, DLQ rate,
commit retries, small-file ratio, snapshot age); structured JSON logs carrying
`batch_id`, `partition`, `offset_range` and a correlation id from producer to
table. SLO: 99% of events queryable within 60 s of ingest.
