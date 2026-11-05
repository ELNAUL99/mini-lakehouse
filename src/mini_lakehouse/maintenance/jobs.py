"""Maintenance jobs: compaction, snapshot expiry, health report.

A separate process from ingestion (design doc section 6) so a maintenance failure
never blocks writes. Written against the real PyIceberg 0.12 API:

* **compaction** is copy-on-write via :meth:`IcebergTableWriter.compact_partition`
  (scan a partition, ``overwrite`` it into fewer target-sized files). PyIceberg
  0.12 has no bin-pack ``rewrite_data_files``; copy-on-write is the honest local
  equivalent and matches the design doc's copy-on-write choice. The nightly
  all-partition pass is where a Spark/Trino rewrite would slot in at scale.
* **snapshot expiry** uses ``table.maintenance.expire_snapshots().older_than(dt).commit()``.
* **health** is computed by the pure :func:`compute_health`.

NOTE: API-correct against PyIceberg 0.12; not yet run against a live catalog here.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from .health import HealthReport, compute_health

log = logging.getLogger("maintenance")


class MaintenanceWorker:
    def __init__(self, catalog=None) -> None:
        if catalog is None:
            from mini_lakehouse.adapters.iceberg import build_catalog

            catalog = build_catalog()
        self._catalog = catalog

    def health_report(self, identifier: str) -> HealthReport:
        """Compute file/skew health from ``table.inspect.files()`` (every 15 min)."""
        table = self._catalog.load_table(identifier)
        files = table.inspect.files().to_pylist()
        sizes = [int(f["file_size_in_bytes"]) for f in files]
        partition_counts: dict[str, int] = {}
        for f in files:
            key = str(f.get("partition", ""))
            partition_counts[key] = partition_counts.get(key, 0) + 1
        report = compute_health(sizes, partition_counts)
        if report.small_file_ratio > 0.5:
            log.warning(
                "%s: small-file ratio %.2f -- compaction needed",
                identifier,
                report.small_file_ratio,
            )
        return report

    def compact(self, identifier: str, row_filter: str = "") -> int:
        """Copy-on-write compaction of one partition selected by ``row_filter``.

        Returns rows rewritten. Pass a bounded partition filter on the 15-minute
        hot-partition pass, e.g. ``"event_day = '2026-11-04'"``. A whole-table
        rewrite is intentionally NOT done here (expensive, and risky without a
        bin-pack planner); that is the nightly Spark/Trino pass noted in the ADR.
        """
        if not row_filter:
            log.info("%s: whole-table compaction deferred to nightly Spark/Trino", identifier)
            return 0
        from mini_lakehouse.adapters.iceberg import IcebergTableWriter

        writer = IcebergTableWriter(identifier, self._catalog)
        rewritten = writer.compact_partition(row_filter)
        log.info("%s: compacted %d rows (filter=%r)", identifier, rewritten, row_filter)
        return rewritten

    def expire_snapshots(self, identifier: str, *, older_than_days: int = 7) -> None:
        """Expire snapshots older than ``older_than_days`` (daily).

        PyIceberg 0.12 exposes ``older_than``/``by_id`` (no ``retain_last``); we
        expire strictly by age, which keeps time travel useful within the window
        and bounds metadata growth (design doc section 7).
        """
        table = self._catalog.load_table(identifier)
        cutoff = datetime.now(UTC) - timedelta(days=older_than_days)
        table.maintenance.expire_snapshots().older_than(cutoff).commit()
        log.info("%s: expired snapshots older than %s", identifier, cutoff.isoformat())
