"""Blob storage for content-addressed file storage.

This module provides efficient disk-based blob storage with:
- Content-addressed storage using SHA256 hashes
- Directory sharding for filesystem efficiency
- Streaming support for large files
- Reference counting for deduplication
"""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import AsyncIterator, overload

from config import BlobConfig
from exceptions import (
    BlobNotFoundError,
    BlobReadError,
    BlobWriteError,
)
from types import BlobInfo


@dataclass(slots=True)
class BlobStore:
    """Content-addressed blob storage.

    Stores binary data on disk using SHA256 hashes as identifiers.
    Supports streaming for multi-gigabyte files without loading
    entire files into memory.

    Attributes:
        config: Blob storage configuration.
        _hasher_class: Hash algorithm class.
        _initialized: Whether the store has been initialized.
    """

    config: BlobConfig = field(default_factory=BlobConfig)
    _hasher_class: type[hashlib._Hash] = field(default=hashlib.sha256, repr=False)
    _initialized: bool = field(default=False, repr=False)

    async def initialize(self) -> None:
        """Initialize the blob store.

        Creates necessary directories and validates configuration.
        """
        if self._initialized:
            return

        # Ensure root directory exists
        if self.config.create_dirs:
            await asyncio.to_thread(self.config.root_path.mkdir, parents=True, exist_ok=True)

            # Create hash prefix directories (00-ff for sha256)
            for i in range(256):
                prefix = f"{i:02x}"
                dir_path = self.config.root_path / prefix
                await asyncio.to_thread(dir_path.mkdir, exist_ok=True)

        self._initialized = True

    def calculate_hash(self, data: bytes) -> str:
        """Calculate SHA256 hash of data.

        Args:
            data: Binary data to hash.

        Returns:
            Hexadecimal hash string.
        """
        hasher = self._hasher_class()
        hasher.update(data)
        return hasher.hexdigest()

    async def calculate_hash_streaming(self, chunk_iterator: AsyncIterator[bytes]) -> str:
        """Calculate SHA256 hash from an async stream of chunks.

        Args:
            chunk_iterator: Async iterator yielding byte chunks.

        Returns:
            Hexadecimal hash string.
        """
        hasher = self._hasher_class()
        async for chunk in chunk_iterator:
            hasher.update(chunk)
        return hasher.hexdigest()

    def calculate_hash_sync_streaming(self, chunk_iterator: Iterator[bytes]) -> str:  # type: ignore[name-defined]
        """Calculate SHA256 hash from a sync stream of chunks.

        Args:
            chunk_iterator: Iterator yielding byte chunks.

        Returns:
            Hexadecimal hash string.
        """
        hasher = self._hasher_class()
        for chunk in chunk_iterator:
            hasher.update(chunk)
        return hasher.hexdigest()

    def _get_blob_path(self, blob_hash: str) -> Path:
        """Get the full path for a blob.

        Args:
            blob_hash: SHA256 hash of the blob.

        Returns:
            Full path to the blob file.
        """
        prefix = blob_hash[: self.config.hash_length]
        return self.config.root_path / prefix / blob_hash

    def get_blob_path(self, blob_hash: str) -> Path:
        """Get the full path for a blob (public interface).

        Args:
            blob_hash: SHA256 hash of the blob.

        Returns:
            Full path to the blob file.
        """
        return self._get_blob_path(blob_hash)

    async def blob_exists(self, blob_hash: str) -> bool:
        """Check if a blob exists.

        Args:
            blob_hash: SHA256 hash of the blob.

        Returns:
            True if the blob exists, False otherwise.
        """
        blob_path = self._get_blob_path(blob_hash)
        return await asyncio.to_thread(blob_path.exists)

    async def save_blob(self, data: bytes, expected_hash: str | None = None) -> tuple[str, bool]:
        """Save a blob to storage.

        Args:
            data: Binary data to store.
            expected_hash: Optional expected hash for verification.

        Returns:
            Tuple of (blob_hash, is_new) where is_new indicates if
            this was a new blob or a duplicate.

        Raises:
            BlobWriteError: If writing fails.
            BlobHashMismatchError: If computed hash doesn't match expected.
        """
        # Calculate hash
        blob_hash = self.calculate_hash(data)

        # Verify hash if expected
        if expected_hash is not None and blob_hash != expected_hash:
            from exceptions import BlobHashMismatchError
            raise BlobHashMismatchError(
                f"Computed hash {blob_hash} does not match expected {expected_hash}"
            )

        blob_path = self._get_blob_path(blob_hash)

        # Check if already exists (deduplication)
        if await asyncio.to_thread(blob_path.exists):
            return blob_hash, False

        # Write the blob
        try:
            await asyncio.to_thread(blob_path.write_bytes, data)
        except OSError as e:
            raise BlobWriteError(f"Failed to write blob {blob_hash}: {e}") from e

        return blob_hash, True

    async def save_blob_from_stream(
        self,
        chunk_iterator: AsyncIterator[bytes],
        expected_hash: str | None = None,
    ) -> tuple[str, bool]:
        """Save a blob from an async stream of chunks.

        Args:
            chunk_iterator: Async iterator yielding byte chunks.
            expected_hash: Optional expected hash for verification.

        Returns:
            Tuple of (blob_hash, is_new) where is_new indicates if
            this was a new blob or a duplicate.

        Raises:
            BlobWriteError: If writing fails.
            BlobHashMismatchError: If computed hash doesn't match expected.
        """
        # First pass: calculate hash and collect chunks
        hasher = self._hasher_class()
        chunks: list[bytes] = []
        total_size = 0

        async for chunk in chunk_iterator:
            hasher.update(chunk)
            chunks.append(chunk)
            total_size += len(chunk)

        blob_hash = hasher.hexdigest()

        # Verify hash if expected
        if expected_hash is not None and blob_hash != expected_hash:
            from exceptions import BlobHashMismatchError
            raise BlobHashMismatchError(
                f"Computed hash {blob_hash} does not match expected {expected_hash}"
            )

        blob_path = self._get_blob_path(blob_hash)

        # Check if already exists
        if await asyncio.to_thread(blob_path.exists):
            return blob_hash, False

        # Write the blob
        try:
            with open(blob_path, "wb") as f:
                for chunk in chunks:
                    f.write(chunk)
        except OSError as e:
            raise BlobWriteError(f"Failed to write blob {blob_hash}: {e}") from e

        return blob_hash, True

    async def delete_blob(self, blob_hash: str) -> bool:
        """Delete a blob from storage.

        Args:
            blob_hash: SHA256 hash of the blob.

        Returns:
            True if the blob was deleted, False if it didn't exist.
        """
        blob_path = self._get_blob_path(blob_hash)

        if not await asyncio.to_thread(blob_path.exists):
            return False

        try:
            await asyncio.to_thread(blob_path.unlink)
            return True
        except OSError:
            return False

    async def open_blob(self, blob_hash: str) -> bytes:
        """Read an entire blob into memory.

        Warning: Do not use for large blobs. Use stream_blob instead.

        Args:
            blob_hash: SHA256 hash of the blob.

        Returns:
            Binary data of the blob.

        Raises:
            BlobNotFoundError: If the blob does not exist.
            BlobReadError: If reading fails.
        """
        blob_path = self._get_blob_path(blob_hash)

        if not await asyncio.to_thread(blob_path.exists):
            raise BlobNotFoundError(f"Blob {blob_hash} not found")

        try:
            return await asyncio.to_thread(blob_path.read_bytes)
        except OSError as e:
            raise BlobReadError(f"Failed to read blob {blob_hash}: {e}") from e

    async def stream_blob(self, blob_hash: str) -> AsyncIterator[bytes]:
        """Stream a blob in chunks.

        Suitable for multi-gigabyte files without loading into memory.

        Args:
            blob_hash: SHA256 hash of the blob.

        Yields:
            Byte chunks of the blob.

        Raises:
            BlobNotFoundError: If the blob does not exist.
            BlobReadError: If reading fails.
        """
        blob_path = self._get_blob_path(blob_hash)

        if not await asyncio.to_thread(blob_path.exists):
            raise BlobNotFoundError(f"Blob {blob_hash} not found")

        try:
            with open(blob_path, "rb") as f:
                while True:
                    chunk = await asyncio.to_thread(f.read, self.config.chunk_size)
                    if not chunk:
                        break
                    yield chunk
        except OSError as e:
            raise BlobReadError(f"Failed to stream blob {blob_hash}: {e}") from e

    async def get_blob_size(self, blob_hash: str) -> int:
        """Get the size of a blob in bytes.

        Args:
            blob_hash: SHA256 hash of the blob.

        Returns:
            Size in bytes.

        Raises:
            BlobNotFoundError: If the blob does not exist.
        """
        blob_path = self._get_blob_path(blob_hash)

        if not await asyncio.to_thread(blob_path.exists):
            raise BlobNotFoundError(f"Blob {blob_hash} not found")

        stat_result = await asyncio.to_thread(blob_path.stat)
        return stat_result.st_size

    async def get_blob_info(self, blob_hash: str, reference_count: int = 1) -> BlobInfo:
        """Get information about a blob.

        Args:
            blob_hash: SHA256 hash of the blob.
            reference_count: Number of cache entries referencing this blob.

        Returns:
            BlobInfo with metadata.

        Raises:
            BlobNotFoundError: If the blob does not exist.
        """
        blob_path = self._get_blob_path(blob_hash)

        if not await asyncio.to_thread(blob_path.exists):
            raise BlobNotFoundError(f"Blob {blob_hash} not found")

        stat_result = await asyncio.to_thread(blob_path.stat)
        created_at = await asyncio.to_thread(lambda: blob_path.stat().st_ctime)

        from datetime import datetime, timezone
        created_dt = datetime.fromtimestamp(created_at, tz=timezone.utc)

        return BlobInfo(
            blob_hash=blob_hash,
            blob_path=blob_path,
            size_bytes=stat_result.st_size,
            reference_count=reference_count,
            created_at=created_dt,
        )

    async def verify_blob_integrity(self, blob_hash: str) -> bool:
        """Verify that a blob's content matches its hash.

        Args:
            blob_hash: Expected SHA256 hash of the blob.

        Returns:
            True if integrity verified, False otherwise.
        """
        blob_path = self._get_blob_path(blob_hash)

        if not await asyncio.to_thread(blob_path.exists):
            return False

        try:
            hasher = self._hasher_class()
            with open(blob_path, "rb") as f:
                while True:
                    chunk = await asyncio.to_thread(f.read, self.config.chunk_size)
                    if not chunk:
                        break
                    hasher.update(chunk)
            return hasher.hexdigest() == blob_hash
        except OSError:
            return False

    async def get_total_size(self) -> int:
        """Get total size of all blobs in bytes.

        Returns:
            Total bytes stored.
        """
        total = 0
        for prefix_dir in self.config.root_path.iterdir():
            if prefix_dir.is_dir() and len(prefix_dir.name) == 2:
                for blob_file in prefix_dir.iterdir():
                    if blob_file.is_file():
                        stat_result = await asyncio.to_thread(blob_file.stat)
                        total += stat_result.st_size
        return total

    async def get_blob_count(self) -> int:
        """Get total number of blobs.

        Returns:
            Number of blobs stored.
        """
        count = 0
        for prefix_dir in self.config.root_path.iterdir():
            if prefix_dir.is_dir() and len(prefix_dir.name) == 2:
                for blob_file in prefix_dir.iterdir():
                    if blob_file.is_file():
                        count += 1
        return count
