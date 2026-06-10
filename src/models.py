from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class CacheRecord:
    url: str

    method: str

    blob_hash: str
    blob_path: str

    content_type: str

    status_code: int

    size_bytes: int

    etag: str | None
    last_modified: str | None

    first_seen: datetime
    last_hit: datetime

    hit_count: int
    miss_count: int

    bandwidth_saved: int

    score: float

    expires_at: datetime | None
