# ADR-0003: Copy-on-write dedupe now, merge-on-read later

- Status: accepted
- Date: 2026-11-04

## Context

The ingestor needs idempotent writes. Iceberg supports copy-on-write (rewrite
affected files on change) and merge-on-read (write delete/position files, resolve
at read time).

## Decision

Use **append + per-batch dedupe** (a copy-on-write-style path): dedupe by
`event_id` within the batch and skip already-committed offsets across batches, so
the common case is a plain append with no rewrite. Reserve full merge-on-read as a
future option.

## Consequences

- Simple and fast to read; appends dominate, so rewrite cost is low in practice.
- Late/duplicate handling stays in the ingestor, not in reader queries.
- If write amplification becomes a problem (heavy late data), revisit
  merge-on-read; noted as the scale-up path.
- Streaming creates many small files; compaction (the maintenance worker) is a
  prerequisite, not an afterthought.
