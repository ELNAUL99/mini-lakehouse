"""Murmur3 x86 32-bit hash and the Iceberg ``bucket`` transform.

Iceberg's ``bucket[N]`` transform is defined as::

    (murmur3_x86_32(value) & Integer.MAX_VALUE) % N

with seed 0, hashing the UTF-8 bytes for strings. Implementing it here (rather
than leaning on a C extension) keeps the partition rule in the pure domain so it
can be unit-tested and, crucially, so the Python ingestor computes *exactly* the
same bucket Iceberg/Trino/Spark would, which is what makes bucket pruning correct.

Reference: Apache Iceberg spec, "Appendix B: 32-bit Hash Requirements".
"""

from __future__ import annotations

_C1 = 0xCC9E2D51
_C2 = 0x1B873593
_MASK32 = 0xFFFFFFFF
_INT_MAX = 0x7FFFFFFF


def _rotl32(x: int, r: int) -> int:
    return ((x << r) | (x >> (32 - r))) & _MASK32


def _fmix32(h: int) -> int:
    h ^= h >> 16
    h = (h * 0x85EBCA6B) & _MASK32
    h ^= h >> 13
    h = (h * 0xC2B2AE35) & _MASK32
    h ^= h >> 16
    return h & _MASK32


def murmur3_x86_32(data: bytes, seed: int = 0) -> int:
    """Return the 32-bit Murmur3 (x86 variant) hash of ``data`` as an unsigned int."""
    length = len(data)
    h = seed & _MASK32
    nblocks = length // 4

    for i in range(nblocks):
        k = int.from_bytes(data[i * 4 : i * 4 + 4], "little")
        k = (k * _C1) & _MASK32
        k = _rotl32(k, 15)
        k = (k * _C2) & _MASK32
        h ^= k
        h = _rotl32(h, 13)
        h = (h * 5 + 0xE6546B64) & _MASK32

    # tail
    tail_index = nblocks * 4
    k1 = 0
    rem = length & 3
    if rem == 3:
        k1 ^= data[tail_index + 2] << 16
    if rem >= 2:
        k1 ^= data[tail_index + 1] << 8
    if rem >= 1:
        k1 ^= data[tail_index]
        k1 = (k1 * _C1) & _MASK32
        k1 = _rotl32(k1, 15)
        k1 = (k1 * _C2) & _MASK32
        h ^= k1

    h ^= length
    return _fmix32(h)


def iceberg_bucket(value: str, num_buckets: int) -> int:
    """Compute the Iceberg ``bucket[num_buckets]`` of a string value.

    Matches the engine-side transform so a Python-computed bucket equals the
    bucket Iceberg stores in partition metadata.
    """
    if num_buckets <= 0:
        raise ValueError("num_buckets must be positive")
    h = murmur3_x86_32(value.encode("utf-8"))
    return (h & _INT_MAX) % num_buckets
