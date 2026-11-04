"""Pure table-health metrics (design doc sections 6-8).

Kept free of PyIceberg so it is unit-tested directly; the job adapter feeds it
file sizes and per-partition counts pulled from ``table.inspect.files()``.
"""

from __future__ import annotations

from dataclasses import dataclass

TARGET_FILE_BYTES = 128 * 1024 * 1024  # 128 MB target (section 6)
SMALL_FILE_BYTES = 16 * 1024 * 1024    # alert threshold (section 8)


@dataclass(frozen=True, slots=True)
class HealthReport:
    file_count: int
    total_bytes: int
    avg_file_bytes: float
    small_file_ratio: float
    partition_skew: float
    needs_compaction: bool

    def as_row(self) -> dict[str, object]:
        """Shape written to ``meta.table_health``."""
        return {
            "file_count": self.file_count,
            "total_bytes": self.total_bytes,
            "avg_file_bytes": round(self.avg_file_bytes, 2),
            "small_file_ratio": round(self.small_file_ratio, 4),
            "partition_skew": round(self.partition_skew, 4),
            "needs_compaction": self.needs_compaction,
        }


def compute_health(
    file_sizes: list[int],
    partition_counts: dict[str, int] | None = None,
    *,
    small_file_bytes: int = SMALL_FILE_BYTES,
) -> HealthReport:
    """Summarise file-size and skew health for one table.

    ``partition_skew`` is max/mean of per-partition file counts (1.0 == perfectly
    even; higher means one partition is hot).
    """
    count = len(file_sizes)
    total = sum(file_sizes)
    avg = total / count if count else 0.0
    small = sum(1 for s in file_sizes if s < small_file_bytes)
    small_ratio = small / count if count else 0.0

    skew = 1.0
    if partition_counts:
        values = list(partition_counts.values())
        mean = sum(values) / len(values)
        if mean > 0:
            skew = max(values) / mean

    needs_compaction = count > 0 and avg < small_file_bytes
    return HealthReport(
        file_count=count,
        total_bytes=total,
        avg_file_bytes=avg,
        small_file_ratio=small_ratio,
        partition_skew=skew,
        needs_compaction=needs_compaction,
    )
