# Mini-Lakehouse

A small, production-shaped data platform: game telemetry flows through **Kafka
(Redpanda)** into **Apache Iceberg** tables on object storage (**MinIO**) and is
queried with an OLAP engine (**DuckDB**, optionally **Trino**). It is built to show
platform-building skill — storage, streaming, compute, and a self-serve interface
for internal customers — not pipeline-running skill.

> This is a **learning build with production-style design**, not a claim of
> production experience with Kafka or Iceberg. See
> [Verified vs not verified](#verified-vs-not-verified) for exactly what has and
> has not been run.

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
     maintenance ── copy-on-write compaction, snapshot expiry, health report
```

Four application services share only the **catalog** and the **tables**:

| Service        | Role                                               | Port |
|----------------|----------------------------------------------------|------|
| `gateway`      | Produce + validate synthetic telemetry             | 8001 |
| `ingestor`     | Kafka → Iceberg consumer, effectively-once commits | —    |
| `maintenance`  | Copy-on-write compaction, snapshot expiry, health  | —    |
| `platform-api` | FastAPI control plane for internal customers       | 8000 |

### Hexagonal core

Domain logic (dedupe, partition rules, offset recovery, contract checks) has **no
I/O** and is tested without containers. Kafka, Iceberg, and Postgres sit behind
**ports** (`src/mini_lakehouse/ports`). That is what lets the entire commit protocol
— including crashes — be proven with in-memory fakes.

```
src/mini_lakehouse/
  domain/       pure logic: events, hashing/bucketing, dedupe, partitioning, offsets, contracts
  ports/        EventSource, TableWriter, DeadLetterSink, Catalog
  adapters/     kafka (confluent-kafka), iceberg (pyiceberg 0.12), settings
  gateway/      producer + validation + synthetic generator
  ingestor/     consume loop, batching, offsets-in-snapshot commit protocol
  maintenance/  copy-on-write compaction, expiry, health
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
offsets from the snapshot history and skips whatever is already written.

```bash
make test-chaos
```

`tests/chaos/test_exactly_once.py` produces a known set of `event_id`s, kills the
ingestor at random points across several seeds, and asserts the table holds
**exactly** those ids with **zero duplicates**. It runs with no containers — the
fake writer persists rows + snapshot offsets across a simulated crash — so it
proves the *commit-protocol logic*, not the live Iceberg/Kafka path (see below).

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

Only `bronze.events` is provisioned in code today (`IcebergTableWriter.ensure_table`);
the silver and meta tables are modelled in the contract/docs and created by the
jobs that populate them (see limitations).

Partitioning is by **event time** (not ingest time) so queries prune on when
things happened and late events land in the right day. `player_id` is bucketed
into 16 buckets — the bucket is computed in pure Python with the **same Murmur3
transform Iceberg uses** (verified against canonical vectors), so Python-side
buckets match the engine's. `payload` is a real Iceberg **struct**, not a JSON
string, so it stays queryable and evolves by adding columns.

## Schema evolution & contracts

A dataset is declared in a YAML contract (`contracts/telemetry_events.yaml`); CI
validates it and the platform provisions the topic, DLQ, and table from it. The
compatibility checker (`domain/contracts.py`) allows adding **optional** fields and
rejects removals, renames, and type changes — the exact rule the Platform API's
`POST /schemas/{name}/versions` enforces. Because `payload` is a struct, v2 adds
optional `payload.platform` as an add-column-by-ID with **no data rewrite**; old
rows read as null.

## Testing

| Layer       | Scope                                                      | How                     |
|-------------|------------------------------------------------------------|-------------------------|
| Unit        | domain, offsets, gateway, maintenance, API, Iceberg schema | `make test-unit`        |
| Chaos       | kill-and-count exactly-once (against fakes)                | `make test-chaos`       |
| Integration | gateway→Redpanda→Iceberg on MinIO (Testcontainers)         | `make test-integration` |

## Verified vs not verified

**Verified here** (runs in CI / `make test`, no live infra):

- Domain logic: event parsing + DLQ reasons, Murmur3/`bucket(16,…)` against
  canonical Iceberg vectors, dedupe (property-tested), partitioning, contract
  compatibility, and snapshot-history **offset recovery**.
- The effectively-once **commit protocol** under random mid-stream kills, against
  in-memory fakes.
- Gateway JSON-Schema validation + DLQ routing + the synthetic generator.
- Platform API registration and the schema-compatibility endpoint.
- Maintenance **health metrics**, and the Iceberg **schema / partition spec /
  struct-payload Arrow conversion**, unit-checked for API-correctness against
  PyIceberg 0.12.
- **The Iceberg write path against a real Iceberg table** — `pytest -m integration`
  (`tests/integration/test_iceberg_local.py`) provisions `bronze.events` through a
  PyIceberg `SqlCatalog` on a local filesystem warehouse and runs `ensure_table`,
  partitioned struct-payload `append_batch`, snapshot-history offset recovery, and
  the **kill-and-count exactly-once** loop against real Parquet — no containers.
  (Requires `pip install .[adapters] sqlalchemy`, incl. `pyiceberg-core` for the
  day/bucket partition transforms.)

**Not yet verified** (needs the full container stack):

- The write path specifically against the **REST-catalog-on-MinIO** deployment
  (vs. the local filesystem catalog above). `make up` is currently blocked here by
  MinIO's public images now requiring authentication on both Docker Hub and
  quay.io — see the note in `deploy/compose/docker-compose.yml`.
- The Kafka adapter against a real broker (manual-commit, cooperative rebalance)
  and end-to-end gateway → Kafka → Iceberg.
- **Benchmarks**: no throughput/latency numbers are published yet — they go here
  once measured, with the hardware stated. No number is claimed until it is run.

## Known limitations

- **Cross-batch dedupe granularity.** In the default `append` mode, duplicates are
  caught per micro-batch (by `event_id`) and across crashes (by Kafka offset in the
  snapshot). A *re-produced* `event_id` arriving at a different offset is not
  deduped in append mode — run the ingestor in **`upsert` mode** (MERGE on
  `event_id`) for exactly-once-by-id regardless of offset, at higher write cost.
- **Commit-conflict exhaustion.** After the retry budget, the batch is routed to
  the DLQ rather than blocking ingestion — a liveness-over-completeness choice;
  the rows are recoverable from the DLQ.
- **Compaction scope.** Local compaction is per-partition copy-on-write; a
  whole-table bin-pack rewrite is deferred to a Spark/Trino pass (PyIceberg 0.12
  has no native bin-packer). Orphan-file cleanup is not implemented yet.

## What changes at scale

Flink for ingest; merge-on-read instead of copy-on-write; multi-tenancy and
authn/z; lineage with OpenLineage; Kafka replication factor 3; tiered storage;
a managed catalog (Polaris/Nessie/Glue).

## Out of scope (stated honestly)

No authn/z, no encryption at rest, no multi-tenancy, no petabyte benchmarks, no
ML or dashboards beyond one demo. See `docs/decisions/` for the trade-offs behind
the big choices.
