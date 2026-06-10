"""SQLite database layer for cache metadata storage.

This module provides async database operations using aiosqlite.
Only metadata is stored in SQLite; response bodies live in blob storage.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import aiosqlite

from src.cache_server.config import DatabaseConfig
from src.cache_server.exceptions import (
    DatabaseConnectionError,
    DatabaseOperationError,
)
from src.cache_server.model_types import CacheEntry, CacheKey


@dataclass(slots=True)
class CacheDB:
    """Async SQLite database for cache metadata.

    Stores cache entry metadata with content-addressed blob references.
    Uses WAL mode for better concurrency and proper transaction handling.

    Attributes:
        config: Database configuration.
        _db: Internal database connection.
        _initialized: Whether the database has been initialized.
    """

    config: DatabaseConfig = field(default_factory=DatabaseConfig)
    _db: aiosqlite.Connection | None = field(default=None, repr=False)
    _initialized: bool = field(default=False, repr=False)

    async def connect(self) -> None:
        """Establish database connection.

        Raises:
            DatabaseConnectionError: If connection fails.
        """
        if self._db is not None:
            return

        try:
            # Ensure parent directory exists
            self.config.path.parent.mkdir(parents=True, exist_ok=True)

            self._db = await aiosqlite.connect(
                str(self.config.path),
            )

            # Configure pragmas
            await self._execute_pragma("journal_mode", self.config.journal_mode)
            await self._execute_pragma("synchronous", self.config.synchronous)
            await self._execute_pragma("cache_size", self.config.cache_size)
            await self._execute_pragma("busy_timeout", self.config.busy_timeout)

            if self.config.wal_mode:
                await self._execute_pragma("wal_autocheckpoint", 1000)

        except aiosqlite.Error as e:
            raise DatabaseConnectionError(f"Failed to connect to database: {e}") from e

    async def _execute_pragma(self, name: str, value: Any) -> None:
        """Execute a PRAGMA statement."""
        if self._db is None:
            return
        await self._db.execute(f"PRAGMA {name} = {value!r}")

    async def close(self) -> None:
        """Close database connection."""
        if self._db is not None:
            await self._db.close()
            self._db = None
        self._initialized = False

    async def initialize_schema(self) -> None:
        """Create database schema if it doesn't exist.

        Creates tables and indexes for cache entries and blobs.
        """
        if self._db is None:
            raise DatabaseConnectionError("Database not connected")

        # Enable foreign keys
        await self._db.execute("PRAGMA foreign_keys = ON")

        # Create blobs table
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS blobs (
                blob_hash TEXT PRIMARY KEY,
                blob_path TEXT NOT NULL,
                size_bytes INTEGER NOT NULL DEFAULT 0,
                reference_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create cache_entries table
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS cache_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                method TEXT NOT NULL DEFAULT 'GET',
                blob_hash TEXT NOT NULL,
                content_type TEXT NOT NULL,
                status_code INTEGER NOT NULL,
                size_bytes INTEGER NOT NULL DEFAULT 0,
                etag TEXT,
                last_modified TEXT,
                first_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_hit TIMESTAMP,
                hit_count INTEGER NOT NULL DEFAULT 0,
                miss_count INTEGER NOT NULL DEFAULT 0,
                bandwidth_saved INTEGER NOT NULL DEFAULT 0,
                score REAL NOT NULL DEFAULT 0.0,
                expiration TIMESTAMP,
                FOREIGN KEY (blob_hash) REFERENCES blobs(blob_hash)
                    ON DELETE CASCADE
            )
        """)

        # Create indexes for efficient lookups
        await self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_cache_entries_url_method
            ON cache_entries(url, method)
        """)

        await self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_cache_entries_blob_hash
            ON cache_entries(blob_hash)
        """)

        await self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_cache_entries_score
            ON cache_entries(score)
        """)

        await self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_cache_entries_expiration
            ON cache_entries(expiration)
        """)

        await self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_cache_entries_last_hit
            ON cache_entries(last_hit)
        """)

        await self._db.commit()
        self._initialized = True

    async def add_entry(self, entry: CacheEntry) -> None:
        """Add a new cache entry.

        Args:
            entry: Cache entry to add.

        Raises:
            DatabaseOperationError: If the operation fails.
        """
        if self._db is None:
            raise DatabaseConnectionError("Database not connected")

        try:
            # First ensure the blob record exists
            await self.increment_blob_reference(entry.blob_hash)

            await self._db.execute(
                """
                INSERT INTO cache_entries (
                    url, method, blob_hash, content_type, status_code,
                    size_bytes, etag, last_modified, first_seen,
                    last_hit, hit_count, miss_count, bandwidth_saved,
                    score, expiration
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    entry.key.url,
                    entry.key.method,
                    entry.blob_hash,
                    entry.content_type,
                    entry.status_code,
                    entry.size_bytes,
                    entry.etag,
                    entry.last_modified,
                    entry.first_seen.isoformat(),
                    entry.last_hit.isoformat() if entry.last_hit else None,
                    entry.hit_count,
                    entry.miss_count,
                    entry.bandwidth_saved,
                    entry.score,
                    entry.expiration.isoformat() if entry.expiration else None,
                ),
            )
            await self._db.commit()
        except aiosqlite.Error as e:
            raise DatabaseOperationError(f"Failed to add cache entry: {e}") from e

    async def get_entry(self, url: str, method: str = "GET") -> CacheEntry | None:
        """Get a cache entry by URL and method.

        Args:
            url: Request URL.
            method: HTTP method.

        Returns:
            CacheEntry if found, None otherwise.

        Raises:
            DatabaseOperationError: If the operation fails.
        """
        if self._db is None:
            raise DatabaseConnectionError("Database not connected")

        try:
            async with self._db.execute(
                """
                SELECT url, method, blob_hash, content_type, status_code,
                       size_bytes, etag, last_modified, first_seen,
                       last_hit, hit_count, miss_count, bandwidth_saved,
                       score, expiration
                FROM cache_entries
                WHERE url = ? AND method = ?
                """,
                (url, method),
            ) as cursor:
                row = await cursor.fetchone()

                if row is None:
                    return None

                return self._row_to_entry(row)
        except aiosqlite.Error as e:
            raise DatabaseOperationError(f"Failed to get cache entry: {e}") from e

    async def delete_entry(self, url: str, method: str = "GET") -> bool:
        """Delete a cache entry.

        Args:
            url: Request URL.
            method: HTTP method.

        Returns:
            True if entry was deleted, False if not found.

        Raises:
            DatabaseOperationError: If the operation fails.
        """
        if self._db is None:
            raise DatabaseConnectionError("Database not connected")

        try:
            cursor = await self._db.execute(
                "DELETE FROM cache_entries WHERE url = ? AND method = ?",
                (url, method),
            )
            await self._db.commit()
            return cursor.rowcount > 0
        except aiosqlite.Error as e:
            raise DatabaseOperationError(f"Failed to delete cache entry: {e}") from e

    async def record_hit(self, url: str, method: str = "GET", bytes_served: int = 0) -> None:
        """Record a cache hit.

        Updates hit count, last hit time, and bandwidth saved.

        Args:
            url: Request URL.
            method: HTTP method.
            bytes_served: Number of bytes served.

        Raises:
            DatabaseOperationError: If the operation fails.
        """
        if self._db is None:
            raise DatabaseConnectionError("Database not connected")

        now = datetime.now(timezone.utc).isoformat()

        try:
            await self._db.execute(
                """
                UPDATE cache_entries
                SET hit_count = hit_count + 1,
                    last_hit = ?,
                    bandwidth_saved = bandwidth_saved + ?
                WHERE url = ? AND method = ?
            """,
                (now, bytes_served, url, method),
            )
            await self._db.commit()
        except aiosqlite.Error as e:
            raise DatabaseOperationError(f"Failed to record cache hit: {e}") from e

    async def record_miss(self, url: str, method: str = "GET") -> None:
        """Record a cache miss.

        Args:
            url: Request URL.
            method: HTTP method.

        Raises:
            DatabaseOperationError: If the operation fails.
        """
        if self._db is None:
            raise DatabaseConnectionError("Database not connected")

        try:
            await self._db.execute(
                """
                UPDATE cache_entries
                SET miss_count = miss_count + 1
                WHERE url = ? AND method = ?
            """,
                (url, method),
            )
            await self._db.commit()
        except aiosqlite.Error as e:
            raise DatabaseOperationError(f"Failed to record cache miss: {e}") from e

    async def update_score(self, url: str, method: str, score: float) -> None:
        """Update the eviction score for an entry.

        Args:
            url: Request URL.
            method: HTTP method.
            score: New eviction score.

        Raises:
            DatabaseOperationError: If the operation fails.
        """
        if self._db is None:
            raise DatabaseConnectionError("Database not connected")

        try:
            await self._db.execute(
                "UPDATE cache_entries SET score = ? WHERE url = ? AND method = ?",
                (score, url, method),
            )
            await self._db.commit()
        except aiosqlite.Error as e:
            raise DatabaseOperationError(f"Failed to update score: {e}") from e

    async def update_expiration(
        self,
        url: str,
        method: str,
        expiration: datetime | None,
    ) -> None:
        """Update the expiration time for an entry.

        Args:
            url: Request URL.
            method: HTTP method.
            expiration: New expiration time.

        Raises:
            DatabaseOperationError: If the operation fails.
        """
        if self._db is None:
            raise DatabaseConnectionError("Database not connected")

        try:
            await self._db.execute(
                "UPDATE cache_entries SET expiration = ? WHERE url = ? AND method = ?",
                (expiration.isoformat() if expiration else None, url, method),
            )
            await self._db.commit()
        except aiosqlite.Error as e:
            raise DatabaseOperationError(f"Failed to update expiration: {e}") from e

    async def list_lowest_scores(self, limit: int = 100) -> list[CacheEntry]:
        """Get entries with lowest eviction scores.

        Args:
            limit: Maximum number of entries to return.

        Returns:
            List of CacheEntry objects sorted by score ascending.

        Raises:
            DatabaseOperationError: If the operation fails.
        """
        if self._db is None:
            raise DatabaseConnectionError("Database not connected")

        try:
            async with self._db.execute(
                """
                SELECT url, method, blob_hash, content_type, status_code,
                       size_bytes, etag, last_modified, first_seen,
                       last_hit, hit_count, miss_count, bandwidth_saved,
                       score, expiration
                FROM cache_entries
                ORDER BY score ASC
                LIMIT ?
                """,
                (limit,),
            ) as cursor:
                rows = await cursor.fetchall()
                return [self._row_to_entry(row) for row in rows]
        except aiosqlite.Error as e:
            raise DatabaseOperationError(f"Failed to list lowest scores: {e}") from e

    async def list_expired_entries(self) -> list[CacheEntry]:
        """Get all expired entries.

        Returns:
            List of expired CacheEntry objects.

        Raises:
            DatabaseOperationError: If the operation fails.
        """
        if self._db is None:
            raise DatabaseConnectionError("Database not connected")

        now = datetime.now(timezone.utc).isoformat()

        try:
            async with self._db.execute(
                """
                SELECT url, method, blob_hash, content_type, status_code,
                       size_bytes, etag, last_modified, first_seen,
                       last_hit, hit_count, miss_count, bandwidth_saved,
                       score, expiration
                FROM cache_entries
                WHERE expiration IS NOT NULL AND expiration < ?
                """,
                (now,),
            ) as cursor:
                rows = await cursor.fetchall()
                return [self._row_to_entry(row) for row in rows]
        except aiosqlite.Error as e:
            raise DatabaseOperationError(f"Failed to list expired entries: {e}") from e

    async def increment_blob_reference(self, blob_hash: str) -> None:
        """Increment reference count for a blob.

        Args:
            blob_hash: SHA256 hash of the blob.

        Raises:
            DatabaseOperationError: If the operation fails.
        """
        if self._db is None:
            raise DatabaseConnectionError("Database not connected")

        try:
            await self._db.execute(
                """
                INSERT INTO blobs (blob_hash, blob_path, size_bytes, reference_count)
                VALUES (?, '', 0, 1)
                ON CONFLICT(blob_hash) DO UPDATE SET
                    reference_count = reference_count + 1
                """,
                (blob_hash,),
            )
            await self._db.commit()
        except aiosqlite.Error as e:
            raise DatabaseOperationError(f"Failed to increment blob reference: {e}") from e

    async def decrement_blob_reference(self, blob_hash: str) -> int:
        """Decrement reference count for a blob.

        Args:
            blob_hash: SHA256 hash of the blob.

        Returns:
            New reference count after decrement.

        Raises:
            DatabaseOperationError: If the operation fails.
        """
        if self._db is None:
            raise DatabaseConnectionError("Database not connected")

        try:
            await self._db.execute(
                """
                UPDATE blobs
                SET reference_count = reference_count - 1
                WHERE blob_hash = ?
                """,
                (blob_hash,),
            )
            await self._db.commit()

            async with self._db.execute(
                "SELECT reference_count FROM blobs WHERE blob_hash = ?",
                (blob_hash,),
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0
        except aiosqlite.Error as e:
            raise DatabaseOperationError(f"Failed to decrement blob reference: {e}") from e

    async def get_blob(self, blob_hash: str) -> dict[str, Any] | None:
        """Get blob metadata.

        Args:
            blob_hash: SHA256 hash of the blob.

        Returns:
            Dictionary with blob metadata or None if not found.

        Raises:
            DatabaseOperationError: If the operation fails.
        """
        if self._db is None:
            raise DatabaseConnectionError("Database not connected")

        try:
            async with self._db.execute(
                """
                SELECT blob_hash, blob_path, size_bytes, reference_count, created_at
                FROM blobs
                WHERE blob_hash = ?
                """,
                (blob_hash,),
            ) as cursor:
                row = await cursor.fetchone()

                if row is None:
                    return None

                return {
                    "blob_hash": row[0],
                    "blob_path": row[1],
                    "size_bytes": row[2],
                    "reference_count": row[3],
                    "created_at": row[4],
                }
        except aiosqlite.Error as e:
            raise DatabaseOperationError(f"Failed to get blob: {e}") from e

    async def add_blob(
        self,
        blob_hash: str,
        blob_path: str,
        size_bytes: int,
        reference_count: int = 1,
    ) -> None:
        """Add a blob record.

        Args:
            blob_hash: SHA256 hash of the blob.
            blob_path: Path to the blob file.
            size_bytes: Size of the blob in bytes.
            reference_count: Initial reference count.

        Raises:
            DatabaseOperationError: If the operation fails.
        """
        if self._db is None:
            raise DatabaseConnectionError("Database not connected")

        now = datetime.now(timezone.utc).isoformat()

        try:
            await self._db.execute(
                """
                INSERT OR REPLACE INTO blobs
                (blob_hash, blob_path, size_bytes, reference_count, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (blob_hash, blob_path, size_bytes, reference_count, now),
            )
            await self._db.commit()
        except aiosqlite.Error as e:
            raise DatabaseOperationError(f"Failed to add blob: {e}") from e

    async def delete_blob(self, blob_hash: str) -> bool:
        """Delete a blob record.

        Args:
            blob_hash: SHA256 hash of the blob.

        Returns:
            True if blob was deleted, False if not found.

        Raises:
            DatabaseOperationError: If the operation fails.
        """
        if self._db is None:
            raise DatabaseConnectionError("Database not connected")

        try:
            cursor = await self._db.execute(
                "DELETE FROM blobs WHERE blob_hash = ?",
                (blob_hash,),
            )
            await self._db.commit()
            return cursor.rowcount > 0
        except aiosqlite.Error as e:
            raise DatabaseOperationError(f"Failed to delete blob: {e}") from e

    async def get_entry_count(self) -> int:
        """Get total number of cache entries.

        Returns:
            Number of entries in the cache.
        """
        if self._db is None:
            raise DatabaseConnectionError("Database not connected")

        async with self._db.execute("SELECT COUNT(*) FROM cache_entries") as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_total_size(self) -> int:
        """Get total size of all cached content in bytes.

        Returns:
            Total bytes stored.
        """
        if self._db is None:
            raise DatabaseConnectionError("Database not connected")

        async with self._db.execute(
            "SELECT COALESCE(SUM(size_bytes), 0) FROM cache_entries"
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_all_entries(self) -> AsyncIterator[CacheEntry]:
        """Iterate over all cache entries.

        Yields:
            CacheEntry objects.
        """
        if self._db is None:
            raise DatabaseConnectionError("Database not connected")

        async with self._db.execute(
            """
            SELECT url, method, blob_hash, content_type, status_code,
                   size_bytes, etag, last_modified, first_seen,
                   last_hit, hit_count, miss_count, bandwidth_saved,
                   score, expiration
            FROM cache_entries
            """
        ) as cursor:
            async for row in cursor:
                yield self._row_to_entry(row)

    def _row_to_entry(self, row: tuple[Any, ...]) -> CacheEntry:
        """Convert a database row to a CacheEntry.

        Args:
            row: Database row tuple.

        Returns:
            CacheEntry object.
        """
        key = CacheKey(url=row[0], method=row[1])

        first_seen_str = row[8]
        if isinstance(first_seen_str, str):
            first_seen = datetime.fromisoformat(first_seen_str.replace("Z", "+00:00"))
        else:
            first_seen = datetime.now(timezone.utc)

        last_hit_str = row[9]
        last_hit: datetime | None = None
        if last_hit_str:
            if isinstance(last_hit_str, str):
                last_hit = datetime.fromisoformat(last_hit_str.replace("Z", "+00:00"))

        expiration_str = row[14]
        expiration: datetime | None = None
        if expiration_str:
            if isinstance(expiration_str, str):
                expiration = datetime.fromisoformat(expiration_str.replace("Z", "+00:00"))

        return CacheEntry(
            key=key,
            blob_hash=row[2],
            content_type=row[3],
            status_code=row[4],
            size_bytes=row[5],
            etag=row[6],
            last_modified=row[7],
            first_seen=first_seen,
            last_hit=last_hit,
            hit_count=row[10],
            miss_count=row[11],
            bandwidth_saved=row[12],
            score=row[13],
            expiration=expiration,
        )
