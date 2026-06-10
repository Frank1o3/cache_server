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

from cacheforge.config import CacheConfig
from cacheforge.cache_engine import EvictionEngine, ScoringWeights
from cacheforge.cache_db import CacheDB
from cacheforge.blob_store import BlobStore
from cacheforge.metrics import CacheMetrics
from cacheforge.types import CacheEntry, CacheHitResult, CacheKey

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
