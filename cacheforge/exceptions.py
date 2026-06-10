"""Custom exceptions for the cache subsystem.

This module defines all exception types used throughout the cache system.
"""

from __future__ import annotations


class CacheError(Exception):
    """Base exception for all cache-related errors."""


class CacheInitializationError(CacheError):
    """Raised when cache initialization fails."""


class DatabaseError(CacheError):
    """Raised when a database operation fails."""


class DatabaseConnectionError(DatabaseError):
    """Raised when database connection cannot be established."""


class DatabaseOperationError(DatabaseError):
    """Raised when a specific database operation fails."""


class BlobStoreError(CacheError):
    """Raised when blob storage operations fail."""


class BlobNotFoundError(BlobStoreError):
    """Raised when a requested blob does not exist."""


class BlobWriteError(BlobStoreError):
    """Raised when writing a blob fails."""


class BlobReadError(BlobStoreError):
    """Raised when reading a blob fails."""


class BlobHashMismatchError(BlobStoreError):
    """Raised when computed hash doesn't match expected hash."""


class CacheMissError(CacheError):
    """Raised when a cache lookup fails to find an entry."""


class CacheEntryNotFoundError(CacheError):
    """Raised when a cache entry does not exist."""


class CacheEvictionError(CacheError):
    """Raised when cache eviction fails."""


class ConfigurationError(CacheError):
    """Raised when configuration is invalid or missing."""


class ValidationError(CacheError):
    """Raised when cache validation fails."""


class StreamError(CacheError):
    """Raised when streaming operations fail."""


class MetricsError(CacheError):
    """Raised when metrics operations fail."""
