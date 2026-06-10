"""Metrics collection for the cache subsystem.

This module provides comprehensive statistics tracking for cache operations.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(slots=True)
class CacheMetrics:
    """Cache metrics collector and exporter.

    Tracks various statistics about cache performance including hits, misses,
    bandwidth savings, and eviction statistics.

    Attributes:
        cache_hits: Total number of cache hits.
        cache_misses: Total number of cache misses.
        bytes_saved: Total bytes served from cache instead of origin.
        bytes_stored: Current total size of cached content.
        evictions: Total number of entries evicted.
        expired_entries: Total number of entries removed due to expiration.
        duplicate_blobs_prevented: Number of times deduplication saved storage.
        entries_count: Current number of cache entries.
        blobs_count: Current number of unique blobs.
        start_time: When metrics collection started.
        last_reset: When metrics were last reset.
        _lock: Thread lock for thread-safe updates.
    """

    cache_hits: int = 0
    cache_misses: int = 0
    bytes_saved: int = 0
    bytes_stored: int = 0
    evictions: int = 0
    expired_entries: int = 0
    duplicate_blobs_prevented: int = 0
    entries_count: int = 0
    blobs_count: int = 0
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_reset: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @property
    def hit_ratio(self) -> float:
        """Calculate the cache hit ratio.

        Returns:
            Hit ratio as a float between 0.0 and 1.0.
            Returns 0.0 if no requests have been made.
        """
        total = self.cache_hits + self.cache_misses
        if total == 0:
            return 0.0
        return self.cache_hits / total

    @property
    def hit_percentage(self) -> float:
        """Calculate the cache hit percentage.

        Returns:
            Hit percentage as a float between 0.0 and 100.0.
        """
        return self.hit_ratio * 100.0

    @property
    def total_requests(self) -> int:
        """Get total number of cache requests."""
        return self.cache_hits + self.cache_misses

    @property
    def uptime_seconds(self) -> float:
        """Get uptime in seconds since metrics started."""
        now = datetime.now(timezone.utc)
        return (now - self.start_time).total_seconds()

    def record_hit(self, bytes_served: int = 0) -> None:
        """Record a cache hit.

        Args:
            bytes_served: Number of bytes served from cache.
        """
        with self._lock:
            self.cache_hits += 1
            self.bytes_saved += bytes_served

    def record_miss(self) -> None:
        """Record a cache miss."""
        with self._lock:
            self.cache_misses += 1

    def record_eviction(self, count: int = 1) -> None:
        """Record cache evictions.

        Args:
            count: Number of entries evicted.
        """
        with self._lock:
            self.evictions += count

    def record_expiration(self, count: int = 1) -> None:
        """Record entries removed due to expiration.

        Args:
            count: Number of expired entries removed.
        """
        with self._lock:
            self.expired_entries += count

    def record_duplicate_prevented(self, count: int = 1) -> None:
        """Record prevented duplicate blob storage.

        Args:
            count: Number of duplicate blobs prevented.
        """
        with self._lock:
            self.duplicate_blobs_prevented += count

    def update_entry_count(self, count: int) -> None:
        """Update the current entry count.

        Args:
            count: Current number of cache entries.
        """
        with self._lock:
            self.entries_count = count

    def update_blob_count(self, count: int) -> None:
        """Update the current blob count.

        Args:
            count: Current number of unique blobs.
        """
        with self._lock:
            self.blobs_count = count

    def update_bytes_stored(self, bytes_stored: int) -> None:
        """Update the total bytes stored.

        Args:
            bytes_stored: Total bytes currently stored.
        """
        with self._lock:
            self.bytes_stored = bytes_stored

    def snapshot(self) -> CacheMetricsSnapshot:
        """Create an immutable snapshot of current metrics.

        Returns:
            A snapshot of current metrics state.
        """
        with self._lock:
            return CacheMetricsSnapshot(
                cache_hits=self.cache_hits,
                cache_misses=self.cache_misses,
                bytes_saved=self.bytes_saved,
                bytes_stored=self.bytes_stored,
                evictions=self.evictions,
                expired_entries=self.expired_entries,
                duplicate_blobs_prevented=self.duplicate_blobs_prevented,
                entries_count=self.entries_count,
                blobs_count=self.blobs_count,
                hit_ratio=self.hit_ratio,
                hit_percentage=self.hit_percentage,
                total_requests=self.total_requests,
                uptime_seconds=self.uptime_seconds,
                timestamp=datetime.now(timezone.utc),
            )

    def reset(self) -> None:
        """Reset all metrics to initial state."""
        with self._lock:
            self.cache_hits = 0
            self.cache_misses = 0
            self.bytes_saved = 0
            self.bytes_stored = 0
            self.evictions = 0
            self.expired_entries = 0
            self.duplicate_blobs_prevented = 0
            self.entries_count = 0
            self.blobs_count = 0
            self.last_reset = datetime.now(timezone.utc)

    def export_dict(self) -> dict[str, Any]:
        """Export metrics as a dictionary.

        Returns:
            Dictionary containing all metrics.
        """
        with self._lock:
            return {
                "cache_hits": self.cache_hits,
                "cache_misses": self.cache_misses,
                "hit_ratio": self.hit_ratio,
                "hit_percentage": self.hit_percentage,
                "total_requests": self.total_requests,
                "bytes_saved": self.bytes_saved,
                "bytes_stored": self.bytes_stored,
                "evictions": self.evictions,
                "expired_entries": self.expired_entries,
                "duplicate_blobs_prevented": self.duplicate_blobs_prevented,
                "entries_count": self.entries_count,
                "blobs_count": self.blobs_count,
                "uptime_seconds": self.uptime_seconds,
                "start_time": self.start_time.isoformat(),
                "last_reset": self.last_reset.isoformat(),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

    def export_json(self) -> str:
        """Export metrics as JSON string.

        Returns:
            JSON string representation of metrics.
        """
        import json
        return json.dumps(self.export_dict(), indent=2)

    def increment_hits(self, count: int = 1) -> None:
        """Increment cache hits by a count.

        Args:
            count: Number of hits to add.
        """
        with self._lock:
            self.cache_hits += count

    def increment_misses(self, count: int = 1) -> None:
        """Increment cache misses by a count.

        Args:
            count: Number of misses to add.
        """
        with self._lock:
            self.cache_misses += count

    def add_bytes_saved(self, bytes_count: int) -> None:
        """Add to bytes saved counter.

        Args:
            bytes_count: Number of bytes to add.
        """
        with self._lock:
            self.bytes_saved += bytes_count


@dataclass(slots=True, frozen=True)
class CacheMetricsSnapshot:
    """Immutable snapshot of cache metrics at a point in time.

    Attributes:
        cache_hits: Total number of cache hits.
        cache_misses: Total number of cache misses.
        bytes_saved: Total bytes served from cache.
        bytes_stored: Current total size of cached content.
        evictions: Total number of entries evicted.
        expired_entries: Total entries removed due to expiration.
        duplicate_blobs_prevented: Deduplication savings count.
        entries_count: Current number of cache entries.
        blobs_count: Current number of unique blobs.
        hit_ratio: Calculated hit ratio (0.0-1.0).
        hit_percentage: Calculated hit percentage (0.0-100.0).
        total_requests: Total cache requests.
        uptime_seconds: Uptime in seconds.
        timestamp: When the snapshot was taken.
    """

    cache_hits: int
    cache_misses: int
    bytes_saved: int
    bytes_stored: int
    evictions: int
    expired_entries: int
    duplicate_blobs_prevented: int
    entries_count: int
    blobs_count: int
    hit_ratio: float
    hit_percentage: float
    total_requests: int
    uptime_seconds: float
    timestamp: datetime

    def to_dict(self) -> dict[str, Any]:
        """Convert snapshot to dictionary.

        Returns:
            Dictionary representation of the snapshot.
        """
        return {
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "bytes_saved": self.bytes_saved,
            "bytes_stored": self.bytes_stored,
            "evictions": self.evictions,
            "expired_entries": self.expired_entries,
            "duplicate_blobs_prevented": self.duplicate_blobs_prevented,
            "entries_count": self.entries_count,
            "blobs_count": self.blobs_count,
            "hit_ratio": self.hit_ratio,
            "hit_percentage": self.hit_percentage,
            "total_requests": self.total_requests,
            "uptime_seconds": self.uptime_seconds,
            "timestamp": self.timestamp.isoformat(),
        }
