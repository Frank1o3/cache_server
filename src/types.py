"""Type definitions for the cache subsystem.

This module defines all core data structures used throughout the cache system.
All dataclasses use slots=True for memory efficiency.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from pathlib import Path
from typing import Any


@dataclass(slots=True, frozen=True)
class CacheKey:
    """Unique identifier for a cache entry.

    Attributes:
        url: The request URL.
        method: HTTP method (GET, POST, etc.).
    """

    url: str
    method: str = "GET"

    def __hash__(self) -> int:
        """Generate hash from URL and method."""
        return hash((self.url, self.method))

    def __eq__(self, other: object) -> bool:
        """Check equality with another CacheKey."""
        if not isinstance(other, CacheKey):
            return NotImplemented
        return self.url == other.url and self.method == other.method


@dataclass(slots=True)
class CacheEntry:
    """Represents a single cache entry with metadata.

    Attributes:
        key: Unique cache key.
        blob_hash: SHA256 hash of the content.
        content_type: MIME type of the response.
        status_code: HTTP status code.
        size_bytes: Size of the cached content.
        etag: Entity tag for validation.
        last_modified: Last-Modified header value.
        first_seen: When the entry was first cached.
        last_hit: When the entry was last accessed.
        hit_count: Number of cache hits.
        miss_count: Number of cache misses.
        bandwidth_saved: Total bytes saved by caching.
        score: Current eviction score (lower = more likely to evict).
        expiration: When the entry expires.
    """

    key: CacheKey
    blob_hash: str
    content_type: str
    status_code: int
    size_bytes: int
    etag: str | None = None
    last_modified: str | None = None
    first_seen: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_hit: datetime | None = None
    hit_count: int = 0
    miss_count: int = 0
    bandwidth_saved: int = 0
    score: float = 0.0
    expiration: datetime | None = None

    @property
    def is_expired(self) -> bool:
        """Check if the entry has expired."""
        if self.expiration is None:
            return False
        return datetime.now(timezone.utc) > self.expiration

    @property
    def age_seconds(self) -> float:
        """Get the age of this entry in seconds."""
        now = datetime.now(timezone.utc)
        return (now - self.first_seen).total_seconds()


@dataclass(slots=True, frozen=True)
class CacheHitResult:
    """Result of a cache lookup operation.

    Attributes:
        hit: Whether the lookup was successful.
        entry: Cache entry metadata (None if miss).
        blob_path: Path to the blob file (None if miss).
    """

    hit: bool
    entry: CacheEntry | None = None
    blob_path: Path | None = None


@dataclass(slots=True)
class BlobInfo:
    """Information about a stored blob.

    Attributes:
        blob_hash: SHA256 hash of the blob.
        blob_path: Path to the blob file.
        size_bytes: Size of the blob in bytes.
        reference_count: Number of cache entries referencing this blob.
        created_at: When the blob was created.
    """

    blob_hash: str
    blob_path: Path
    size_bytes: int
    reference_count: int
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class CacheStatus(Enum):
    """Status codes for cache operations."""

    HIT = auto()
    MISS = auto()
    STALE = auto()
    BYPASS = auto()
    ERROR = auto()


@dataclass(slots=True, frozen=True)
class StreamChunk:
    """A chunk of streamed data.

    Attributes:
        data: Raw bytes of the chunk.
        offset: Byte offset of this chunk in the stream.
        is_final: Whether this is the last chunk.
    """

    data: bytes
    offset: int
    is_final: bool = False


@dataclass(slots=True)
class ValidationContext:
    """Context for cache validation decisions.

    Attributes:
        entry: The cache entry being validated.
        request_headers: Headers from the incoming request.
        response_headers: Headers from the origin response.
        current_time: Current timestamp for validation.
    """

    entry: CacheEntry
    request_headers: dict[str, str] = field(default_factory=dict)
    response_headers: dict[str, str] = field(default_factory=dict)
    current_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True, frozen=True)
class EvictionCandidate:
    """A candidate for cache eviction.

    Attributes:
        entry: The cache entry being considered.
        score: Computed eviction score.
        reason: Why this entry is a candidate.
    """

    entry: CacheEntry
    score: float
    reason: str
