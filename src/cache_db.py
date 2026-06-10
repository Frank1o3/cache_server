from __future__ import annotations

import aiosqlite

from datetime import UTC
from datetime import datetime
from pathlib import Path

from models import CacheRecord


class CacheDB:
    def __init__(
        self,
        db_path: Path,
    ) -> None:
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self._db = await aiosqlite.connect(
            self.db_path,
        )

        self._db.row_factory = aiosqlite.Row

        await self._enable_pragmas()
        await self._create_schema()

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    async def _enable_pragmas(self) -> None:
        assert self._db is not None

        await self._db.execute("PRAGMA journal_mode=WAL;")

        await self._db.execute("PRAGMA synchronous=NORMAL;")

        await self._db.execute("PRAGMA temp_store=MEMORY;")

        await self._db.commit()

    async def _create_schema(self) -> None:
        assert self._db is not None

        await self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS cache_entries (
                url TEXT PRIMARY KEY,

                method TEXT NOT NULL,

                blob_hash TEXT NOT NULL,
                blob_path TEXT NOT NULL,

                content_type TEXT NOT NULL,

                status_code INTEGER NOT NULL,

                size_bytes INTEGER NOT NULL,

                etag TEXT,
                last_modified TEXT,

                first_seen TEXT NOT NULL,
                last_hit TEXT NOT NULL,

                hit_count INTEGER NOT NULL DEFAULT 0,
                miss_count INTEGER NOT NULL DEFAULT 0,

                bandwidth_saved INTEGER NOT NULL DEFAULT 0,

                score REAL NOT NULL DEFAULT 0,

                expires_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_blob_hash
            ON cache_entries(blob_hash);

            CREATE INDEX IF NOT EXISTS idx_last_hit
            ON cache_entries(last_hit);

            CREATE INDEX IF NOT EXISTS idx_score
            ON cache_entries(score);
            """
        )

        await self._db.commit()

    async def add(
        self,
        record: CacheRecord,
    ) -> None:
        assert self._db is not None

        await self._db.execute(
            """
            INSERT OR REPLACE INTO cache_entries (
                url,
                method,
                blob_hash,
                blob_path,
                content_type,
                status_code,
                size_bytes,
                etag,
                last_modified,
                first_seen,
                last_hit,
                hit_count,
                miss_count,
                bandwidth_saved,
                score,
                expires_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                record.url,
                record.method,
                record.blob_hash,
                record.blob_path,
                record.content_type,
                record.status_code,
                record.size_bytes,
                record.etag,
                record.last_modified,
                record.first_seen.isoformat(),
                record.last_hit.isoformat(),
                record.hit_count,
                record.miss_count,
                record.bandwidth_saved,
                record.score,
                record.expires_at.isoformat() if record.expires_at else None,
            ),
        )

        await self._db.commit()

    async def get(
        self,
        url: str,
    ) -> CacheRecord | None:
        assert self._db is not None

        cursor = await self._db.execute(
            """
            SELECT *
            FROM cache_entries
            WHERE url = ?
            """,
            (url,),
        )

        row = await cursor.fetchone()

        if row is None:
            return None

        return CacheRecord(
            url=row["url"],
            method=row["method"],
            blob_hash=row["blob_hash"],
            blob_path=row["blob_path"],
            content_type=row["content_type"],
            status_code=row["status_code"],
            size_bytes=row["size_bytes"],
            etag=row["etag"],
            last_modified=row["last_modified"],
            first_seen=datetime.fromisoformat(row["first_seen"]),
            last_hit=datetime.fromisoformat(row["last_hit"]),
            hit_count=row["hit_count"],
            miss_count=row["miss_count"],
            bandwidth_saved=row["bandwidth_saved"],
            score=row["score"],
            expires_at=(
                datetime.fromisoformat(row["expires_at"]) if row["expires_at"] else None
            ),
        )

    async def record_hit(
        self,
        url: str,
        bytes_served: int,
    ) -> None:
        assert self._db is not None

        await self._db.execute(
            """
            UPDATE cache_entries
            SET
                hit_count = hit_count + 1,
                bandwidth_saved =
                    bandwidth_saved + ?,
                last_hit = ?
            WHERE url = ?
            """,
            (
                bytes_served,
                datetime.now(
                    UTC,
                ).isoformat(),
                url,
            ),
        )

        await self._db.commit()

    async def record_miss(
        self,
        url: str,
    ) -> None:
        assert self._db is not None

        await self._db.execute(
            """
            UPDATE cache_entries
            SET miss_count = miss_count + 1
            WHERE url = ?
            """,
            (url,),
        )

        await self._db.commit()

    async def update_score(
        self,
        url: str,
        score: float,
    ) -> None:
        assert self._db is not None

        await self._db.execute(
            """
            UPDATE cache_entries
            SET score = ?
            WHERE url = ?
            """,
            (
                score,
                url,
            ),
        )

        await self._db.commit()

    async def lowest_scores(
        self,
        limit: int,
    ) -> list[CacheRecord]:
        assert self._db is not None

        cursor = await self._db.execute(
            """
            SELECT *
            FROM cache_entries
            ORDER BY score ASC
            LIMIT ?
            """,
            (limit,),
        )

        rows = await cursor.fetchall()

        return [self._row_to_record(row) for row in rows]
