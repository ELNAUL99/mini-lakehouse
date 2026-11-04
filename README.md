# Mini-Lakehouse

A small, production-shaped data platform: game telemetry flows through **Kafka
(Redpanda)** into **Apache Iceberg** tables on object storage (**MinIO**) and is
queried with an OLAP engine (**DuckDB**, optionally **Trino**). It is built to show
platform-building skill — storage, streaming, compute, and a self-serve interface
for internal customers — not pipeline-running skill.

> This is a **learning build with production-style design**, not a claim of
> production experience with Kafka or Iceberg.

## Why this exists

Many teams need a shared, reliable data foundation that is self-serve: they should
discover data, trust its shape, and query it without knowing how ingestion works.
This project demonstrates that end to end on open-source infrastructure.

## Architecture

```
 gateway            Kafka (Redpanda)          ingestor                 Iceberg (MinIO + REST catalog)
 ───────            ────────────────          ────────                 ──────────────────────────────
 generate   ──▶  telemetry.events.v1  ──▶  poll micro-batch   ──▶  append + commit (offsets in snapshot)
 validate        telemetry.events.dlq       dedupe by event_id       bronze.events / bronze.events_dlq
 (JSON Schema)                               skip committed            silver.player_daily / meta.table_health
                                                                              │
     platform-api (FastAPI) ── register datasets, schema, lineage, health ────┘
     maintenance ── compaction, snapshot expiry, orphan cleanup, health report
```

Four application services share only the **catalog** and the **tables**:

| Service        | Role                                               | Port |
|----------------|----------------------------------------------------|------|
| `gateway`      | Produce + validate synthetic telemetry             | 8001 |
| `ingestor`     | Kafka → Iceberg consumer, effectively-once commits | —    |
| `maintenance`  | Compaction, snapshot expiry, health report         | —    |
| `platform-api` | FastAPI control plane for internal customers       | 8000 |

### Hexagonal core

Domain logic (dedupe, partition rules, contract checks) has **no I/O** and is
tested without containers. Kafka, Iceberg, and Postgres sit behind **ports**
(`src/mini_lakehouse/ports`). That is what lets the entire commit protocol —
including crashes — be proven with in-memory fakes.

```
src/mini_lakehouse/
  domain/       pure logic: events, hashing/bucketing, dedupe, partitioning, contracts
  ports/        EventSource, TableWriter, DeadLetterSink, Catalog
  adapters/     kafka (confluent-kafka), iceberg (pyiceberg), settings
  gateway/      producer + validation + synthetic generator
  ingestor/     consume loop, batching, offsets-in-snapshot commit protocol
  maintenance/  compaction, expiry, health
  platform_api/ FastAPI app
contracts/      dataset YAML + versioned JSON Schemas
tests/          unit / integration / chaos
deploy/         compose / k8s
docs/           architecture.md + ADRs
```

## Headline result: effectively-once under crashes

The ingestor gives **at-least-once delivery with idempotent output** by committing
the Iceberg snapshot (with the batch's Kafka offsets in its summary) *before*
committing offsets back to Kafka. If it dies in between, the restart reads the
offsets from the snapshot and skips whatever is already written.

The chaos test produces a known set of `event_id`s, kills the ingestor at random
points, and asserts the table holds **exactly** those ids with **zero duplicates**:

```bash
make test-chaos
```

`tests/chaos/test_exactly_once.py` runs this across several seeds with no
containers (the fake writer persists rows + snapshot offsets across a simulated
crash). This is the gate that protects the project's core claim.

## Quickstart

```bash
# 1. Run the tested core with no infrastructure
make install
make test            # unit + chaos, 80%+ coverage gate on the domain

# 2. Bring up the full local stack + demo producer
cp deploy/compose/.env.example deploy/compose/.env
make up               # docker compose, core profile
#    Platform API docs: http://localhost:8000/docs
#    MinIO console:      http://localhost:9001

# 3. Optional extras (Trino, Prometheus, Grafana, Redpanda Console)
make up-extras
```

## Data model

One event envelope, three event types (`session_start`, `match_end`, `purchase`),
and a small set of Iceberg tables:

| Table                 | Layer   | Partitioning                               | Purpose                        |
|-----------------------|---------|--------------------------------------------|--------------------------------|
| `bronze.events`       | Raw     | `day(event_time)`, `bucket(16, player_id)` | every valid event, append-only |
| `bronze.events_dlq`   | Raw     | `day(ingest_time)`                         | rejected events + error reason |
| `silver.player_daily` | Derived | `month(day)`                               | per-player daily aggregates    |
| `meta.table_health`   | Control | —                                          | file counts, sizes, compaction |

Partitioning is by **event time** (not ingest time) so queries prune on when
things happened and late events land in the right day. `player_id` is bucketed
into 16 buckets — the bucket is computed in pure Python with the **same Murmur3
transform Iceberg uses**, so Python-side buckets match the engine's.

## Schema evolution & contracts

A dataset is declared in a YAML contract (`contracts/telemetry_events.yaml`); CI
validates it and the platform provisions the topic, DLQ, and table from it. The
compatibility checker (`domain/contracts.py`) allows adding **optional** fields and
rejects removals, renames, and type changes — the exact rule the Platform API's
`POST /schemas/{name}/versions` enforces. v2 adds optional `payload.platform` with
no data rewrite (Iceberg adds the column by ID; old rows read as null).

## Testing

| Layer       | Scope                                              | How                     |
|-------------|----------------------------------------------------|-------------------------|
| Unit        | domain logic, gateway, maintenance, API            | `make test-unit`        |
| Chaos       | kill-and-count exactly-once                         | `make test-chaos`       |
| Integration | gateway→Redpanda→Iceberg on MinIO (Testcontainers) | `make test-integration` |

## What changes at scale

Flink for ingest; merge-on-read instead of copy-on-write; multi-tenancy and
authn/z; lineage with OpenLineage; Kafka replication factor 3; tiered storage;
a managed catalog (Polaris/Nessie/Glue).

## Out of scope (stated honestly)

No authn/z, no encryption at rest, no multi-tenancy, no petabyte benchmarks, no
ML or dashboards beyond one demo. See `docs/decisions/` for the trade-offs behind
the big choices.
