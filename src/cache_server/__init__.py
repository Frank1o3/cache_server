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

from src.cache_server.blob_store import BlobStore
from src.cache_server.cache_db import CacheDB
from src.cache_server.cache_engine import EvictionEngine, ScoringWeights
from src.cache_server.config import CacheConfig
from src.cache_server.metrics import CacheMetrics
from src.cache_server.model_types import CacheEntry, CacheHitResult, CacheKey

__version__ = "0.1.0"
__all__ = [
    "BlobStore",
    "CacheConfig",
    "CacheDB",
    "CacheEntry",
    "CacheHitResult",
    "CacheKey",
    "CacheMetrics",
    "EvictionEngine",
    "ScoringWeights",
]
