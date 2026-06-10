"""CacheForge - A high-performance disk-backed cache subsystem.

This module provides a complete caching infrastructure with:
- Persistent SQLite metadata storage
- Content-addressed blob storage
- Configurable eviction policies
- Comprehensive metrics tracking
- Streaming support for large files

Example:
    ```python
    from cacheforge import CacheDB, CacheConfig

    config = CacheConfig()
    db = CacheDB(config.database)
    await db.connect()
    await db.initialize_schema()
    ```
"""

from types import CacheEntry, CacheHitResult, CacheKey

from blob_store import BlobStore
from cache_db import CacheDB
from cache_engine import EvictionEngine, ScoringWeights
from config import CacheConfig
from metrics import CacheMetrics

__version__ = "0.1.0"
__all__ = [
    "CacheConfig",
    "EvictionEngine",
    "ScoringWeights",
    "CacheDB",
    "BlobStore",
    "CacheMetrics",
    "CacheEntry",
    "CacheHitResult",
    "CacheKey",
]
