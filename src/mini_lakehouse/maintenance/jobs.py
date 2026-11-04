"""Maintenance jobs: compaction, snapshot expiry, orphan cleanup, health report.

A separate process from ingestion (design doc section 6) so a maintenance failure
never blocks writes. PyIceberg's rewrite/expiry surface is still maturing, so the
honest implementation here does what the current API supports and documents the
rest -- matching the risk noted in the design doc ("PyIceberg write API is less
mature for streaming").
"""

from __future__ import annotations

import logging

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
            log.warning("%s: small-file ratio %.2f -- compaction needed",
                        identifier, report.small_file_ratio)
        return report

    def compact(self, identifier: str) -> None:
        """Rewrite small files toward the 128 MB target.

        Uses PyIceberg data-file rewrite where available; on engines/versions
        without it, this logs and defers to the scheduled nightly Spark/Trino
        compaction documented in the ADR. Intentionally conservative: compaction
        must never corrupt a table.
        """
        table = self._catalog.load_table(identifier)
        rewrite = getattr(table, "rewrite_data_files", None)
        if callable(rewrite):
            rewrite(target_file_size_bytes=128 * 1024 * 1024)
            log.info("%s: compaction committed", identifier)
        else:  # pragma: no cover
            log.info("%s: rewrite_data_files unavailable in this PyIceberg; skipped", identifier)

    def expire_snapshots(
        self, identifier: str, *, retain_last: int = 20, older_than_days: int = 7
    ) -> None:
        """Expire snapshots older than N days, keeping the last ``retain_last`` (daily)."""
        table = self._catalog.load_table(identifier)
        expire = getattr(table, "expire_snapshots", None)
        if callable(expire):  # pragma: no cover - depends on PyIceberg version
            expire(retain_last=retain_last, older_than_days=older_than_days)
            log.info("%s: snapshot expiry committed", identifier)
        else:  # pragma: no cover
            log.info("%s: expire_snapshots unavailable; deferring", identifier)
