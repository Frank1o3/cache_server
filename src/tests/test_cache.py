"""Unit tests for the cache subsystem."""

from __future__ import annotations

import asyncio
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from blob_store import BlobStore
from cache_db import CacheDB
from cache_engine import EvictionEngine, ScoringWeights
from config import (
    BlobConfig,
    CacheConfig,
    DatabaseConfig,
    EvictionConfig,
)
from exceptions import BlobNotFoundError
from metrics import CacheMetrics, CacheMetricsSnapshot
from types import CacheEntry, CacheKey
from validators import (
    is_cacheable_content_type,
    is_cacheable_method,
    is_cacheable_status,
    is_expired,
    parse_cache_control,
    should_revalidate,
    ttl_from_headers,
)


# ============================================================================
# Hashing Tests
# ============================================================================


class TestHashing:
    """Tests for blob hashing functionality."""

    def test_calculate_hash_deterministic(self) -> None:
        """Hash calculation is deterministic."""
        store = BlobStore()
        data = b"Hello, World!"

        hash1 = store.calculate_hash(data)
        hash2 = store.calculate_hash(data)

        assert hash1 == hash2
        assert len(hash1) == 64  # SHA256 hex length

    def test_calculate_hash_different_inputs(self) -> None:
        """Different inputs produce different hashes."""
        store = BlobStore()

        hash1 = store.calculate_hash(b"data1")
        hash2 = store.calculate_hash(b"data2")

        assert hash1 != hash2

    def test_calculate_hash_empty(self) -> None:
        """Empty data produces valid hash."""
        store = BlobStore()
        hash_result = store.calculate_hash(b"")

        assert len(hash_result) == 64
        # Known SHA256 of empty string
        assert hash_result == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


# ============================================================================
# Blob Storage Tests
# ============================================================================


class TestBlobStorage:
    """Tests for blob storage operations."""

    @pytest.fixture
    async def blob_store(self) -> BlobStore:
        """Create a temporary blob store for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = BlobConfig(root_path=Path(tmpdir))
            store = BlobStore(config=config)
            await store.initialize()
            yield store

    @pytest.mark.asyncio
    async def test_save_and_read_blob(self, blob_store: BlobStore) -> None:
        """Test saving and reading a blob."""
        data = b"Test content for blob storage"

        blob_hash, is_new = await blob_store.save_blob(data)

        assert is_new is True
        assert len(blob_hash) == 64

        read_data = await blob_store.open_blob(blob_hash)
        assert read_data == data

    @pytest.mark.asyncio
    async def test_blob_deduplication(self, blob_store: BlobStore) -> None:
        """Test that duplicate blobs are detected."""
        data = b"Duplicate test data"

        hash1, is_new1 = await blob_store.save_blob(data)
        hash2, is_new2 = await blob_store.save_blob(data)

        assert hash1 == hash2
        assert is_new1 is True
        assert is_new2 is False

    @pytest.mark.asyncio
    async def test_blob_exists(self, blob_store: BlobStore) -> None:
        """Test blob existence check."""
        data = b"Existence test"
        blob_hash, _ = await blob_store.save_blob(data)

        assert await blob_store.blob_exists(blob_hash) is True
        assert await blob_store.blob_exists("nonexistent" * 8) is False

    @pytest.mark.asyncio
    async def test_delete_blob(self, blob_store: BlobStore) -> None:
        """Test blob deletion."""
        data = b"Delete test"
        blob_hash, _ = await blob_store.save_blob(data)

        result = await blob_store.delete_blob(blob_hash)
        assert result is True

        assert await blob_store.blob_exists(blob_hash) is False

        # Deleting non-existent blob returns False
        result2 = await blob_store.delete_blob(blob_hash)
        assert result2 is False

    @pytest.mark.asyncio
    async def test_stream_blob(self, blob_store: BlobStore) -> None:
        """Test streaming large blobs."""
        # Create larger test data
        data = b"X" * 10000

        blob_hash, _ = await blob_store.save_blob(data)

        chunks = []
        async for chunk in blob_store.stream_blob(blob_hash):
            chunks.append(chunk)

        reconstructed = b"".join(chunks)
        assert reconstructed == data

    @pytest.mark.asyncio
    async def test_get_blob_size(self, blob_store: BlobStore) -> None:
        """Test getting blob size."""
        data = b"Size test data"
        blob_hash, _ = await blob_store.save_blob(data)

        size = await blob_store.get_blob_size(blob_hash)
        assert size == len(data)

    @pytest.mark.asyncio
    async def test_blob_not_found(self, blob_store: BlobStore) -> None:
        """Test BlobNotFoundError on missing blob."""
        with pytest.raises(BlobNotFoundError):
            await blob_store.open_blob("nonexistent" * 8)


# ============================================================================
# Cache Scoring Tests
# ============================================================================


class TestCacheScoring:
    """Tests for cache eviction scoring."""

    def test_score_entry_basic(self) -> None:
        """Test basic entry scoring."""
        engine = EvictionEngine()
        key = CacheKey(url="http://example.com/test", method="GET")
        entry = CacheEntry(
            key=key,
            blob_hash="abc123",
            content_type="text/html",
            status_code=200,
            size_bytes=1000,
        )

        score = engine.score_entry(entry)

        assert engine.config.min_score <= score <= engine.config.max_score

    def test_score_entry_frequency_impact(self) -> None:
        """Test that hit count affects score."""
        engine = EvictionEngine()
        key = CacheKey(url="http://example.com/test", method="GET")

        entry_low_hits = CacheEntry(
            key=key,
            blob_hash="abc123",
            content_type="text/html",
            status_code=200,
            size_bytes=1000,
            hit_count=1,
        )

        entry_high_hits = CacheEntry(
            key=key,
            blob_hash="def456",
            content_type="text/html",
            status_code=200,
            size_bytes=1000,
            hit_count=100,
        )

        score_low = engine.score_entry(entry_low_hits, max_hit_count=100)
        score_high = engine.score_entry(entry_high_hits, max_hit_count=100)

        # Higher hits = lower score (less likely to evict)
        # Lower score means keep, higher score means evict
        assert score_low > score_high  # Low hits should have higher eviction score

    def test_score_entry_recency_impact(self) -> None:
        """Test that recency affects score."""
        engine = EvictionEngine()
        now = datetime.now(timezone.utc)

        key = CacheKey(url="http://example.com/test", method="GET")

        entry_recent = CacheEntry(
            key=key,
            blob_hash="abc123",
            content_type="text/html",
            status_code=200,
            size_bytes=1000,
            last_hit=now - timedelta(minutes=1),
        )

        entry_stale = CacheEntry(
            key=key,
            blob_hash="def456",
            content_type="text/html",
            status_code=200,
            size_bytes=1000,
            last_hit=now - timedelta(hours=5),
        )

        score_recent = engine.score_entry(entry_recent, current_time=now)
        score_stale = engine.score_entry(entry_stale, current_time=now)

        # More recent = lower score (less likely to evict)
        assert score_stale > score_recent  # Stale entries should have higher eviction score

    def test_score_entry_bandwidth_impact(self) -> None:
        """Test that bandwidth savings affects score."""
        engine = EvictionEngine()
        key = CacheKey(url="http://example.com/test", method="GET")

        entry_low_bw = CacheEntry(
            key=key,
            blob_hash="abc123",
            content_type="text/html",
            status_code=200,
            size_bytes=1000,
            bandwidth_saved=100,
        )

        entry_high_bw = CacheEntry(
            key=key,
            blob_hash="def456",
            content_type="text/html",
            status_code=200,
            size_bytes=1000,
            bandwidth_saved=1000000,
        )

        score_low = engine.score_entry(entry_low_bw, max_bandwidth=1000000)
        score_high = engine.score_entry(entry_high_bw, max_bandwidth=1000000)

        # More bandwidth saved = lower score (less likely to evict)
        assert score_low > score_high  # Low bandwidth should have higher eviction score

    def test_should_evict_under_limits(self) -> None:
        """Test that entries aren't evicted when under limits."""
        config = EvictionConfig(target_count=1000, target_size_bytes=1000000)
        engine = EvictionEngine(config=config)

        key = CacheKey(url="http://example.com/test", method="GET")
        entry = CacheEntry(
            key=key,
            blob_hash="abc123",
            content_type="text/html",
            status_code=200,
            size_bytes=1000,
        )

        should_evict = engine.should_evict(
            entry,
            current_count=100,
            current_size=100000,
        )

        assert should_evict is False

    def test_rank_entries(self) -> None:
        """Test entry ranking."""
        engine = EvictionEngine()
        now = datetime.now(timezone.utc)

        entries = [
            CacheEntry(
                key=CacheKey(url=f"http://example.com/{i}", method="GET"),
                blob_hash=f"hash{i}",
                content_type="text/html",
                status_code=200,
                size_bytes=1000,
                hit_count=i * 10,
                last_hit=now - timedelta(hours=i),
            )
            for i in range(1, 6)
        ]

        candidates = engine.rank_entries(entries, current_time=now)

        assert len(candidates) == 5
        # Sorted by score ascending
        for i in range(len(candidates) - 1):
            assert candidates[i].score <= candidates[i + 1].score

    def test_custom_weights(self) -> None:
        """Test custom scoring weights."""
        # Create new engine with custom weights directly
        weights = ScoringWeights(lfu=0.5, recency=0.3, age=0.2, bandwidth=0.0, latency=0.0, mru=0.0)
        engine = EvictionEngine(weights=weights)

        current_weights = engine.get_weights()
        # Weights are normalized, so 0.5/(0.5+0.3+0.2) = 0.5
        assert abs(current_weights.lfu - 0.5) < 0.01
        assert abs(current_weights.recency - 0.3) < 0.01


# ============================================================================
# Database CRUD Tests
# ============================================================================


class TestDatabaseCRUD:
    """Tests for database operations."""

    @pytest.fixture
    async def cache_db(self) -> CacheDB:
        """Create a temporary database for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            config = DatabaseConfig(path=db_path)
            db = CacheDB(config=config)
            await db.connect()
            await db.initialize_schema()
            yield db
            await db.close()

    @pytest.mark.asyncio
    async def test_add_and_get_entry(self, cache_db: CacheDB) -> None:
        """Test adding and retrieving an entry."""
        key = CacheKey(url="http://example.com/test", method="GET")
        entry = CacheEntry(
            key=key,
            blob_hash="abc123",
            content_type="text/html",
            status_code=200,
            size_bytes=1000,
        )

        await cache_db.add_entry(entry)
        retrieved = await cache_db.get_entry("http://example.com/test", "GET")

        assert retrieved is not None
        assert retrieved.key.url == entry.key.url
        assert retrieved.blob_hash == entry.blob_hash

    @pytest.mark.asyncio
    async def test_get_nonexistent_entry(self, cache_db: CacheDB) -> None:
        """Test getting a nonexistent entry."""
        result = await cache_db.get_entry("http://nonexistent.com", "GET")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_entry(self, cache_db: CacheDB) -> None:
        """Test deleting an entry."""
        key = CacheKey(url="http://example.com/delete", method="GET")
        entry = CacheEntry(
            key=key,
            blob_hash="abc123",
            content_type="text/html",
            status_code=200,
            size_bytes=1000,
        )

        await cache_db.add_entry(entry)

        deleted = await cache_db.delete_entry("http://example.com/delete", "GET")
        assert deleted is True

        retrieved = await cache_db.get_entry("http://example.com/delete", "GET")
        assert retrieved is None

    @pytest.mark.asyncio
    async def test_record_hit(self, cache_db: CacheDB) -> None:
        """Test recording cache hits."""
        key = CacheKey(url="http://example.com/hit", method="GET")
        entry = CacheEntry(
            key=key,
            blob_hash="abc123",
            content_type="text/html",
            status_code=200,
            size_bytes=1000,
            hit_count=0,
        )

        await cache_db.add_entry(entry)
        await cache_db.record_hit("http://example.com/hit", "GET", bytes_served=500)

        updated = await cache_db.get_entry("http://example.com/hit", "GET")
        assert updated is not None
        assert updated.hit_count == 1
        assert updated.bandwidth_saved == 500

    @pytest.mark.asyncio
    async def test_record_miss(self, cache_db: CacheDB) -> None:
        """Test recording cache misses."""
        key = CacheKey(url="http://example.com/miss", method="GET")
        entry = CacheEntry(
            key=key,
            blob_hash="abc123",
            content_type="text/html",
            status_code=200,
            size_bytes=1000,
            miss_count=0,
        )

        await cache_db.add_entry(entry)
        await cache_db.record_miss("http://example.com/miss", "GET")

        updated = await cache_db.get_entry("http://example.com/miss", "GET")
        assert updated is not None
        assert updated.miss_count == 1

    @pytest.mark.asyncio
    async def test_update_score(self, cache_db: CacheDB) -> None:
        """Test updating entry score."""
        key = CacheKey(url="http://example.com/score", method="GET")
        entry = CacheEntry(
            key=key,
            blob_hash="abc123",
            content_type="text/html",
            status_code=200,
            size_bytes=1000,
            score=50.0,
        )

        await cache_db.add_entry(entry)
        await cache_db.update_score("http://example.com/score", "GET", 25.0)

        updated = await cache_db.get_entry("http://example.com/score", "GET")
        assert updated is not None
        assert updated.score == 25.0


# ============================================================================
# Expiration Handling Tests
# ============================================================================


class TestExpirationHandling:
    """Tests for cache expiration."""

    @pytest.fixture
    async def cache_db(self) -> CacheDB:
        """Create a temporary database for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            config = DatabaseConfig(path=db_path)
            db = CacheDB(config=config)
            await db.connect()
            await db.initialize_schema()
            yield db
            await db.close()

    @pytest.mark.asyncio
    async def test_list_expired_entries(self, cache_db: CacheDB) -> None:
        """Test listing expired entries."""
        now = datetime.now(timezone.utc)
        past = now - timedelta(hours=1)
        future = now + timedelta(hours=1)

        # Add expired entry
        expired_key = CacheKey(url="http://example.com/expired", method="GET")
        expired_entry = CacheEntry(
            key=expired_key,
            blob_hash="abc123",
            content_type="text/html",
            status_code=200,
            size_bytes=1000,
            expiration=past,
        )
        await cache_db.add_entry(expired_entry)

        # Add valid entry
        valid_key = CacheKey(url="http://example.com/valid", method="GET")
        valid_entry = CacheEntry(
            key=valid_key,
            blob_hash="def456",
            content_type="text/html",
            status_code=200,
            size_bytes=1000,
            expiration=future,
        )
        await cache_db.add_entry(valid_entry)

        expired = await cache_db.list_expired_entries()

        assert len(expired) == 1
        assert expired[0].key.url == "http://example.com/expired"

    @pytest.mark.asyncio
    async def test_update_expiration(self, cache_db: CacheDB) -> None:
        """Test updating entry expiration."""
        key = CacheKey(url="http://example.com/exp", method="GET")
        now = datetime.now(timezone.utc)
        future = now + timedelta(hours=1)

        entry = CacheEntry(
            key=key,
            blob_hash="abc123",
            content_type="text/html",
            status_code=200,
            size_bytes=1000,
        )
        await cache_db.add_entry(entry)

        await cache_db.update_expiration("http://example.com/exp", "GET", future)

        updated = await cache_db.get_entry("http://example.com/exp", "GET")
        assert updated is not None
        assert updated.expiration is not None

    def test_is_expired_function(self) -> None:
        """Test is_expired validator function."""
        now = datetime.now(timezone.utc)
        past = now - timedelta(hours=1)
        future = now + timedelta(hours=1)

        assert is_expired(future, now) is False
        assert is_expired(past, now) is True
        assert is_expired(None, now) is False


# ============================================================================
# Deduplication Tests
# ============================================================================


class TestDeduplication:
    """Tests for blob deduplication."""

    @pytest.fixture
    async def cache_db(self) -> CacheDB:
        """Create a temporary database for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            config = DatabaseConfig(path=db_path)
            db = CacheDB(config=config)
            await db.connect()
            await db.initialize_schema()
            yield db
            await db.close()

    @pytest.mark.asyncio
    async def test_increment_blob_reference(self, cache_db: CacheDB) -> None:
        """Test incrementing blob reference count."""
        blob_hash = "test_hash_" + "a" * 54

        await cache_db.increment_blob_reference(blob_hash)
        blob_info = await cache_db.get_blob(blob_hash)

        assert blob_info is not None
        assert blob_info["reference_count"] == 1

        # Increment again
        await cache_db.increment_blob_reference(blob_hash)
        blob_info = await cache_db.get_blob(blob_hash)

        assert blob_info["reference_count"] == 2

    @pytest.mark.asyncio
    async def test_decrement_blob_reference(self, cache_db: CacheDB) -> None:
        """Test decrementing blob reference count."""
        blob_hash = "test_hash_" + "b" * 54

        await cache_db.increment_blob_reference(blob_hash)
        await cache_db.increment_blob_reference(blob_hash)

        count = await cache_db.decrement_blob_reference(blob_hash)
        assert count == 1

        count = await cache_db.decrement_blob_reference(blob_hash)
        assert count == 0

    @pytest.mark.asyncio
    async def test_blob_reference_cleanup(self, cache_db: CacheDB) -> None:
        """Test that blobs can be cleaned up when references reach zero."""
        blob_hash = "test_hash_" + "c" * 54

        await cache_db.increment_blob_reference(blob_hash)
        await cache_db.decrement_blob_reference(blob_hash)

        # Reference count should be 0
        blob_info = await cache_db.get_blob(blob_hash)
        assert blob_info is not None
        assert blob_info["reference_count"] == 0


# ============================================================================
# Reference Counting Tests
# ============================================================================


class TestReferenceCounting:
    """Tests for reference counting system."""

    @pytest.fixture
    async def cache_db(self) -> CacheDB:
        """Create a temporary database for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            config = DatabaseConfig(path=db_path)
            db = CacheDB(config=config)
            await db.connect()
            await db.initialize_schema()
            yield db
            await db.close()

    @pytest.mark.asyncio
    async def test_multiple_entries_same_blob(self, cache_db: CacheDB) -> None:
        """Test multiple entries referencing the same blob."""
        blob_hash = "shared_blob_" + "d" * 52

        # Add multiple entries with same blob hash
        for i in range(3):
            key = CacheKey(url=f"http://example.com/{i}", method="GET")
            entry = CacheEntry(
                key=key,
                blob_hash=blob_hash,
                content_type="text/html",
                status_code=200,
                size_bytes=1000,
            )
            await cache_db.add_entry(entry)
            # Note: add_entry already increments reference, so we don't do it again

        blob_info = await cache_db.get_blob(blob_hash)
        assert blob_info is not None
        assert blob_info["reference_count"] == 3

    @pytest.mark.asyncio
    async def test_get_entry_count(self, cache_db: CacheDB) -> None:
        """Test getting total entry count."""
        for i in range(5):
            key = CacheKey(url=f"http://example.com/{i}", method="GET")
            entry = CacheEntry(
                key=key,
                blob_hash=f"hash{i}",
                content_type="text/html",
                status_code=200,
                size_bytes=1000,
            )
            await cache_db.add_entry(entry)

        count = await cache_db.get_entry_count()
        assert count == 5

    @pytest.mark.asyncio
    async def test_get_total_size(self, cache_db: CacheDB) -> None:
        """Test getting total cached size."""
        for i in range(3):
            key = CacheKey(url=f"http://example.com/{i}", method="GET")
            entry = CacheEntry(
                key=key,
                blob_hash=f"hash{i}",
                content_type="text/html",
                status_code=200,
                size_bytes=1000 * (i + 1),
            )
            await cache_db.add_entry(entry)

        total_size = await cache_db.get_total_size()
        assert total_size == 1000 + 2000 + 3000


# ============================================================================
# Validator Tests
# ============================================================================


class TestValidators:
    """Tests for cache validators."""

    def test_is_cacheable_status(self) -> None:
        """Test HTTP status code cacheability."""
        assert is_cacheable_status(200) is True
        assert is_cacheable_status(204) is True
        assert is_cacheable_status(301) is True
        assert is_cacheable_status(404) is False
        assert is_cacheable_status(500) is False
        assert is_cacheable_status(201) is False

    def test_is_cacheable_method(self) -> None:
        """Test HTTP method cacheability."""
        assert is_cacheable_method("GET") is True
        assert is_cacheable_method("HEAD") is True
        assert is_cacheable_method("OPTIONS") is True
        assert is_cacheable_method("POST") is False
        assert is_cacheable_method("PUT") is False
        assert is_cacheable_method("DELETE") is False

    def test_is_cacheable_content_type(self) -> None:
        """Test content type cacheability."""
        assert is_cacheable_content_type("text/html") is True
        assert is_cacheable_content_type("application/json") is True
        assert is_cacheable_content_type("image/png") is True
        assert is_cacheable_content_type("text/event-stream") is False
        assert is_cacheable_content_type(None) is True

    def test_ttl_from_headers(self) -> None:
        """Test TTL extraction from headers."""
        headers_max_age = {"cache-control": "max-age=300"}
        assert ttl_from_headers(headers_max_age) == 300

        headers_no_store = {"cache-control": "no-store"}
        assert ttl_from_headers(headers_no_store) == -1

        headers_no_cache = {"cache-control": "no-cache"}
        assert ttl_from_headers(headers_no_cache) == 0

        headers_default = {}
        assert ttl_from_headers(headers_default) == 3600  # default

    def test_should_revalidate(self) -> None:
        """Test revalidation decision."""
        # No-cache forces revalidation
        request_headers = {"cache-control": "no-cache"}
        assert should_revalidate(None, None, request_headers, {}) is True

        # Normal case without validation headers
        assert should_revalidate(None, None, {}, {}) is False

        # With ETag available
        assert should_revalidate("abc123", None, {}, {}) is False

    def test_parse_cache_control(self) -> None:
        """Test Cache-Control header parsing."""
        result = parse_cache_control("max-age=300, public, no-transform")

        assert result["max-age"] == 300
        assert result["public"] is True
        assert result["no-transform"] is True


# ============================================================================
# Metrics Tests
# ============================================================================


class TestMetrics:
    """Tests for metrics collection."""

    def test_record_hit(self) -> None:
        """Test recording cache hits."""
        metrics = CacheMetrics()

        metrics.record_hit(bytes_served=1000)
        assert metrics.cache_hits == 1
        assert metrics.bytes_saved == 1000

        metrics.record_hit(bytes_served=500)
        assert metrics.cache_hits == 2
        assert metrics.bytes_saved == 1500

    def test_record_miss(self) -> None:
        """Test recording cache misses."""
        metrics = CacheMetrics()

        metrics.record_miss()
        assert metrics.cache_misses == 1

        metrics.record_miss()
        assert metrics.cache_misses == 2

    def test_hit_ratio(self) -> None:
        """Test hit ratio calculation."""
        metrics = CacheMetrics()

        # No requests = 0 ratio
        assert metrics.hit_ratio == 0.0

        metrics.record_hit(bytes_served=100)
        metrics.record_miss()

        assert metrics.hit_ratio == 0.5

        metrics.record_hit(bytes_served=100)
        assert metrics.hit_ratio == 2 / 3

    def test_snapshot(self) -> None:
        """Test metrics snapshot."""
        metrics = CacheMetrics()
        metrics.record_hit(bytes_served=1000)
        metrics.record_miss()

        snapshot = metrics.snapshot()

        assert isinstance(snapshot, CacheMetricsSnapshot)
        assert snapshot.cache_hits == 1
        assert snapshot.cache_misses == 1
        assert snapshot.hit_ratio == 0.5

    def test_reset(self) -> None:
        """Test metrics reset."""
        metrics = CacheMetrics()
        metrics.record_hit(bytes_served=1000)
        metrics.record_miss()

        metrics.reset()

        assert metrics.cache_hits == 0
        assert metrics.cache_misses == 0
        assert metrics.bytes_saved == 0

    def test_export_dict(self) -> None:
        """Test metrics dictionary export."""
        metrics = CacheMetrics()
        metrics.record_hit(bytes_served=1000)

        exported = metrics.export_dict()

        assert "cache_hits" in exported
        assert "hit_ratio" in exported
        assert "timestamp" in exported
        assert exported["cache_hits"] == 1

    def test_eviction_tracking(self) -> None:
        """Test eviction tracking."""
        metrics = CacheMetrics()

        metrics.record_eviction(count=5)
        assert metrics.evictions == 5

        metrics.record_expiration(count=3)
        assert metrics.expired_entries == 3

    def test_duplicate_prevention_tracking(self) -> None:
        """Test duplicate prevention tracking."""
        metrics = CacheMetrics()

        metrics.record_duplicate_prevented(count=2)
        assert metrics.duplicate_blobs_prevented == 2


# ============================================================================
# Configuration Tests
# ============================================================================


class TestConfiguration:
    """Tests for configuration system."""

    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = CacheConfig.with_defaults()

        assert config.cache_dir == Path("cache")
        assert config.database.wal_mode is True
        assert config.blob.hash_algorithm == "sha256"

    def test_config_paths_relative_to_cache_dir(self) -> None:
        """Test that paths are relative to cache_dir."""
        config = CacheConfig.with_defaults(cache_dir="/tmp/mycache")

        assert config.database.path == Path("/tmp/mycache/cache.db")
        assert config.blob.root_path == Path("/tmp/mycache/blobs")

    def test_eviction_config_weights(self) -> None:
        """Test eviction configuration weights."""
        config = EvictionConfig(
            weight_lfu=0.3,
            weight_recency=0.3,
            weight_age=0.2,
            weight_bandwidth=0.1,
            weight_latency=0.05,
            weight_mru=0.05,
        )

        total = (
            config.weight_lfu
            + config.weight_recency
            + config.weight_age
            + config.weight_bandwidth
            + config.weight_latency
            + config.weight_mru
        )
        assert total == 1.0

    def test_for_testing_config(self) -> None:
        """Test testing configuration helper."""
        config = CacheConfig.for_testing()

        assert config.eviction.target_count == 100
        assert config.metrics.track_individual_entries is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
