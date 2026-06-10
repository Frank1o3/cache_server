"""Configuration system for the cache subsystem.

This module provides typed configuration classes using dataclasses.
All configurations use sensible defaults and can be customized.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class DatabaseConfig:
    """SQLite database configuration.

    Attributes:
        path: Path to the SQLite database file.
        wal_mode: Enable WAL mode for better concurrency.
        journal_mode: Journal mode (WAL, DELETE, TRUNCATE, etc.).
        synchronous: Synchronous mode (NORMAL, FULL, OFF).
        cache_size: Page cache size in pages.
        busy_timeout: Timeout in milliseconds for busy locks.
        max_connections: Maximum concurrent connections (for future pool support).
    """

    path: Path = field(default_factory=lambda: Path("cache.db"))
    wal_mode: bool = True
    journal_mode: str = "WAL"
    synchronous: str = "NORMAL"
    cache_size: int = 10000
    busy_timeout: int = 5000
    max_connections: int = 1


@dataclass(slots=True)
class BlobConfig:
    """Blob storage configuration.

    Attributes:
        root_path: Root directory for blob storage.
        hash_algorithm: Hash algorithm for content addressing.
        hash_length: Length of hash prefix for directory structure.
        chunk_size: Size of chunks for streaming operations.
        max_blob_size: Maximum allowed blob size (0 = unlimited).
        create_dirs: Automatically create directories if missing.
    """

    root_path: Path = field(default_factory=lambda: Path("blobs"))
    hash_algorithm: str = "sha256"
    hash_length: int = 2
    chunk_size: int = 8192
    max_blob_size: int = 0
    create_dirs: bool = True


@dataclass(slots=True)
class EvictionConfig:
    """Cache eviction policy configuration.

    The scoring system combines multiple factors:
    - LFU (Least Frequently Used): Based on hit count
    - TLRU (Time-aware LRU): Based on age and last access
    - ARC-inspired: Balances recency and frequency
    - FIFO: Age consideration
    - MRU: Recent access bonus
    - Bandwidth savings: Preference for high-value entries
    - Origin latency: Preference for slow-origin entries

    Attributes:
        weight_lfu: Weight for frequency-based scoring.
        weight_recency: Weight for recency-based scoring.
        weight_age: Weight for age-based scoring.
        weight_bandwidth: Weight for bandwidth savings.
        weight_latency: Weight for origin latency savings.
        weight_mru: Weight for MRU bonus.
        min_score: Minimum possible score.
        max_score: Maximum possible score.
        target_count: Target number of entries before eviction.
        target_size_bytes: Target total size before eviction.
        batch_size: Number of entries to evict in one batch.
        grace_period_seconds: Don't evict entries newer than this.
    """

    weight_lfu: float = 0.25
    weight_recency: float = 0.25
    weight_age: float = 0.15
    weight_bandwidth: float = 0.15
    weight_latency: float = 0.10
    weight_mru: float = 0.10
    min_score: float = 0.0
    max_score: float = 100.0
    target_count: int = 100000
    target_size_bytes: int = 1099511627776  # 1 TB
    batch_size: int = 100
    grace_period_seconds: float = 60.0


@dataclass(slots=True)
class MetricsConfig:
    """Metrics collection configuration.

    Attributes:
        enabled: Whether metrics collection is enabled.
        track_individual_entries: Track per-entry statistics.
        export_interval_seconds: Interval for periodic exports.
        retention_hours: How long to keep historical metrics.
    """

    enabled: bool = True
    track_individual_entries: bool = False
    export_interval_seconds: float = 60.0
    retention_hours: int = 24


@dataclass(slots=True)
class CacheConfig:
    """Main cache configuration.

    Aggregates all sub-configurations into a single interface.

    Attributes:
        database: SQLite database configuration.
        blob: Blob storage configuration.
        eviction: Eviction policy configuration.
        metrics: Metrics collection configuration.
        cache_dir: Base directory for all cache files.
        auto_initialize: Automatically initialize on engine creation.
    """

    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    blob: BlobConfig = field(default_factory=BlobConfig)
    eviction: EvictionConfig = field(default_factory=EvictionConfig)
    metrics: MetricsConfig = field(default_factory=MetricsConfig)
    cache_dir: Path = field(default_factory=lambda: Path("cache"))
    auto_initialize: bool = True

    def __post_init__(self) -> None:
        """Set up paths relative to cache_dir."""
        if not self.database.path.is_absolute():
            self.database.path = self.cache_dir / self.database.path
        if not self.blob.root_path.is_absolute():
            self.blob.root_path = self.cache_dir / self.blob.root_path

    @classmethod
    def with_defaults(cls, cache_dir: Path | str = "cache") -> CacheConfig:
        """Create a configuration with default values.

        Args:
            cache_dir: Base directory for cache files.

        Returns:
            A new CacheConfig instance.
        """
        return cls(cache_dir=Path(cache_dir))

    @classmethod
    def for_testing(cls) -> CacheConfig:
        """Create a configuration optimized for testing.

        Returns:
            A new CacheConfig with test-friendly settings.
        """
        import tempfile

        temp_dir = Path(tempfile.mkdtemp())
        return cls(
            cache_dir=temp_dir,
            eviction=EvictionConfig(
                target_count=100,
                target_size_bytes=1024 * 1024,
            ),
            metrics=MetricsConfig(
                enabled=True,
                track_individual_entries=True,
            ),
        )
