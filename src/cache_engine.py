"""Cache eviction engine with hybrid scoring algorithm.

This module implements a configurable eviction policy that combines:
- LFU (Least Frequently Used)
- TLRU (Time-aware LRU)
- ARC-inspired recency/frequency balancing
- FIFO age consideration
- MRU consideration
- Bandwidth savings weighting
- Origin latency savings weighting
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

from config import EvictionConfig
from types import CacheEntry, EvictionCandidate


@dataclass(slots=True)
class ScoringWeights:
    """Configurable weights for the scoring algorithm.

    All weights are normalized internally to sum to 1.0.

    Attributes:
        lfu: Weight for frequency-based scoring.
        recency: Weight for recency-based scoring.
        age: Weight for age-based scoring.
        bandwidth: Weight for bandwidth savings.
        latency: Weight for origin latency savings.
        mru: Weight for MRU bonus.
    """

    lfu: float = 0.25
    recency: float = 0.25
    age: float = 0.15
    bandwidth: float = 0.15
    latency: float = 0.10
    mru: float = 0.10

    def normalize(self) -> ScoringWeights:
        """Normalize weights to sum to 1.0.

        Returns:
            New ScoringWeights with normalized values.
        """
        total = self.lfu + self.recency + self.age + self.bandwidth + self.latency + self.mru
        if total == 0:
            return ScoringWeights()
        return ScoringWeights(
            lfu=self.lfu / total,
            recency=self.recency / total,
            age=self.age / total,
            bandwidth=self.bandwidth / total,
            latency=self.latency / total,
            mru=self.mru / total,
        )


@dataclass(slots=True)
class EvictionEngine:
    """Hybrid cache eviction engine.

    Implements a configurable scoring algorithm that balances multiple
    factors to determine which entries should be evicted.

    The score is calculated as a weighted combination of:
    - Frequency score (LFU): Higher hit count = lower eviction priority
    - Recency score: More recent access = lower eviction priority
    - Age score: Older entries = higher eviction priority
    - Bandwidth score: More bytes saved = lower eviction priority
    - Latency score: Higher origin latency = lower eviction priority
    - MRU bonus: Very recently accessed entries get protection

    Attributes:
        config: Eviction configuration.
        weights: Normalized scoring weights.
        _origin_latencies: Cached origin latencies per URL.
    """

    config: EvictionConfig = field(default_factory=EvictionConfig)
    weights: ScoringWeights = field(default_factory=ScoringWeights)
    _origin_latencies: dict[str, float] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        """Initialize normalized weights."""
        self.weights = self.weights.normalize()

    def score_entry(
        self,
        entry: CacheEntry,
        current_time: datetime | None = None,
        max_hit_count: int = 1,
        max_bandwidth: int = 1,
        max_age_seconds: float = 1.0,
    ) -> float:
        """Calculate eviction score for a cache entry.

        Lower scores indicate higher eviction priority.
        Scores are normalized to the range [min_score, max_score].

        Args:
            entry: Cache entry to score.
            current_time: Current time for calculations. Defaults to now.
            max_hit_count: Maximum hit count for normalization.
            max_bandwidth: Maximum bandwidth saved for normalization.
            max_age_seconds: Maximum age for normalization.

        Returns:
            Eviction score (lower = more likely to evict).
        """
        if current_time is None:
            current_time = datetime.now(timezone.utc)

        # Ensure current_time is timezone-aware
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=timezone.utc)

        # Calculate individual component scores (0.0 to 1.0, higher = keep)
        frequency_score = self._calculate_frequency_score(entry, max_hit_count)
        recency_score = self._calculate_recency_score(entry, current_time)
        age_score = self._calculate_age_score(entry, current_time, max_age_seconds)
        bandwidth_score = self._calculate_bandwidth_score(entry, max_bandwidth)
        latency_score = self._calculate_latency_score(entry)
        mru_bonus = self._calculate_mru_bonus(entry, current_time)

        # Combine scores using weights
        # Higher combined score = keep, Lower combined score = evict
        combined = (
            self.weights.lfu * frequency_score
            + self.weights.recency * recency_score
            + self.weights.age * age_score
            + self.weights.bandwidth * bandwidth_score
            + self.weights.latency * latency_score
            + self.weights.mru * mru_bonus
        )

        # Invert and scale to configured range
        # Lower score = evict first
        score_range = self.config.max_score - self.config.min_score
        final_score = self.config.max_score - (combined * score_range)

        return final_score

    def _calculate_frequency_score(self, entry: CacheEntry, max_hit_count: int) -> float:
        """Calculate LFU component score.

        Uses logarithmic scaling to prevent very high hit counts from
        dominating the score.

        Args:
            entry: Cache entry.
            max_hit_count: Maximum hit count for normalization.

        Returns:
            Frequency score between 0.0 and 1.0.
        """
        if max_hit_count <= 0:
            return 0.5

        hit_count = max(entry.hit_count, 1)
        max_hits = max(max_hit_count, 1)

        # Logarithmic scaling
        log_hits = math.log(hit_count + 1)
        log_max = math.log(max_hits + 1)

        return min(log_hits / log_max, 1.0)

    def _calculate_recency_score(self, entry: CacheEntry, current_time: datetime) -> float:
        """Calculate recency component score.

        More recently accessed entries get higher scores.

        Args:
            entry: Cache entry.
            current_time: Current time.

        Returns:
            Recency score between 0.0 and 1.0.
        """
        if entry.last_hit is None:
            # No hits yet - use first_seen as proxy
            last_access = entry.first_seen
        else:
            last_access = entry.last_hit

        # Ensure timezone awareness
        if last_access.tzinfo is None:
            last_access = last_access.replace(tzinfo=timezone.utc)

        seconds_since_access = (current_time - last_access).total_seconds()

        # Exponential decay - recent accesses worth much more
        # Half-life of 1 hour
        half_life = 3600.0
        decay_constant = math.log(2) / half_life

        score = math.exp(-decay_constant * seconds_since_access)
        return max(score, 0.0)

    def _calculate_age_score(
        self,
        entry: CacheEntry,
        current_time: datetime,
        max_age_seconds: float,
    ) -> float:
        """Calculate age component score.

        Older entries get lower scores (higher eviction priority).

        Args:
            entry: Cache entry.
            current_time: Current time.
            max_age_seconds: Maximum age for normalization.

        Returns:
            Age score between 0.0 and 1.0.
        """
        if entry.first_seen.tzinfo is None:
            first_seen = entry.first_seen.replace(tzinfo=timezone.utc)
        else:
            first_seen = entry.first_seen

        age_seconds = (current_time - first_seen).total_seconds()

        if max_age_seconds <= 0:
            return 0.5

        # Linear normalization
        normalized_age = min(age_seconds / max_age_seconds, 1.0)

        # Invert: newer entries get higher scores
        return 1.0 - normalized_age

    def _calculate_bandwidth_score(self, entry: CacheEntry, max_bandwidth: int) -> float:
        """Calculate bandwidth savings component score.

        Entries that have saved more bandwidth get higher scores.

        Args:
            entry: Cache entry.
            max_bandwidth: Maximum bandwidth saved for normalization.

        Returns:
            Bandwidth score between 0.0 and 1.0.
        """
        if max_bandwidth <= 0:
            return 0.5

        # Logarithmic scaling to prevent outliers from dominating
        bandwidth = max(entry.bandwidth_saved, 0)
        log_bandwidth = math.log(bandwidth + 1)
        log_max = math.log(max_bandwidth + 1)

        return min(log_bandwidth / log_max, 1.0)

    def _calculate_latency_score(self, entry: CacheEntry) -> float:
        """Calculate origin latency component score.

        Entries from slow origins get higher scores (more valuable to cache).

        Args:
            entry: Cache entry.

        Returns:
            Latency score between 0.0 and 1.0.
        """
        url = entry.key.url
        latency_ms = self._origin_latencies.get(url, 100.0)  # Default 100ms

        # Normalize: assume max latency of 5000ms
        max_latency = 5000.0
        normalized = min(latency_ms / max_latency, 1.0)

        return normalized

    def _calculate_mru_bonus(self, entry: CacheEntry, current_time: datetime) -> float:
        """Calculate MRU bonus score.

        Very recently accessed entries get a bonus to protect them
        from immediate eviction (ARC-inspired behavior).

        Args:
            entry: Cache entry.
            current_time: Current time.

        Returns:
            MRU bonus between 0.0 and 1.0.
        """
        if entry.last_hit is None:
            return 0.0

        if entry.last_hit.tzinfo is None:
            last_hit = entry.last_hit.replace(tzinfo=timezone.utc)
        else:
            last_hit = entry.last_hit

        seconds_since_hit = (current_time - last_hit).total_seconds()

        # Full bonus for hits within 5 minutes
        bonus_window = 300.0

        if seconds_since_hit < bonus_window:
            # Linear decay within the window
            return 1.0 - (seconds_since_hit / bonus_window)

        return 0.0

    def should_evict(
        self,
        entry: CacheEntry,
        current_count: int,
        current_size: int,
        current_time: datetime | None = None,
    ) -> bool:
        """Determine if an entry should be evicted.

        Considers both count and size limits, plus grace period.

        Args:
            entry: Cache entry to evaluate.
            current_count: Current number of cache entries.
            current_size: Current total cache size in bytes.
            current_time: Current time for calculations.

        Returns:
            True if the entry should be evicted.
        """
        if current_time is None:
            current_time = datetime.now(timezone.utc)

        # Check if under limits
        under_count_limit = current_count <= self.config.target_count
        under_size_limit = current_size <= self.config.target_size_bytes

        if under_count_limit and under_size_limit:
            return False

        # Check grace period
        if entry.first_seen.tzinfo is None:
            first_seen = entry.first_seen.replace(tzinfo=timezone.utc)
        else:
            first_seen = entry.first_seen

        age_seconds = (current_time - first_seen).total_seconds()
        if age_seconds < self.config.grace_period_seconds:
            return False

        # Calculate score and compare to threshold
        score = self.score_entry(entry, current_time)

        # Evict if score is below midpoint
        midpoint = (self.config.min_score + self.config.max_score) / 2
        return score < midpoint

    def rank_entries(
        self,
        entries: list[CacheEntry],
        current_time: datetime | None = None,
    ) -> list[EvictionCandidate]:
        """Rank entries by eviction priority.

        Args:
            entries: List of cache entries to rank.
            current_time: Current time for calculations.

        Returns:
            List of EvictionCandidate objects sorted by score ascending.
        """
        if not entries:
            return []

        if current_time is None:
            current_time = datetime.now(timezone.utc)

        # Calculate max values for normalization
        max_hit_count = max((e.hit_count for e in entries), default=1)
        max_bandwidth = max((e.bandwidth_saved for e in entries), default=1)

        max_age_seconds = 0.0
        for entry in entries:
            if entry.first_seen.tzinfo is None:
                first_seen = entry.first_seen.replace(tzinfo=timezone.utc)
            else:
                first_seen = entry.first_seen
            age = (current_time - first_seen).total_seconds()
            max_age_seconds = max(max_age_seconds, age)

        candidates: list[EvictionCandidate] = []

        for entry in entries:
            score = self.score_entry(
                entry,
                current_time,
                max_hit_count=max_hit_count,
                max_bandwidth=max_bandwidth,
                max_age_seconds=max_age_seconds,
            )

            reason = self._determine_eviction_reason(entry, score)
            candidates.append(EvictionCandidate(entry=entry, score=score, reason=reason))

        # Sort by score ascending (lowest = evict first)
        candidates.sort(key=lambda c: c.score)

        return candidates

    def recommend_evictions(
        self,
        entries: list[CacheEntry],
        target_count_reduction: int = 0,
        target_size_reduction: int = 0,
        current_time: datetime | None = None,
    ) -> list[EvictionCandidate]:
        """Recommend entries for eviction to meet targets.

        Args:
            entries: List of cache entries.
            target_count_reduction: Number of entries to remove.
            target_size_reduction: Bytes to free up.
            current_time: Current time for calculations.

        Returns:
            List of recommended eviction candidates.
        """
        if not entries:
            return []

        if current_time is None:
            current_time = datetime.now(timezone.utc)

        candidates = self.rank_entries(entries, current_time)

        if target_count_reduction == 0 and target_size_reduction == 0:
            # Just return bottom 10%
            count = max(len(candidates) // 10, 1)
            return candidates[:count]

        result: list[EvictionCandidate] = []
        freed_count = 0
        freed_size = 0

        for candidate in candidates:
            if freed_count >= target_count_reduction and freed_size >= target_size_reduction:
                break

            result.append(candidate)
            freed_count += 1
            freed_size += candidate.entry.size_bytes

        return result

    def _determine_eviction_reason(self, entry: CacheEntry, score: float) -> str:
        """Determine the primary reason for eviction.

        Args:
            entry: Cache entry.
            score: Calculated eviction score.

        Returns:
            Human-readable reason string.
        """
        reasons: list[tuple[float, str]] = []

        # Check each factor
        if entry.hit_count == 0:
            reasons.append((self.weights.lfu, "no_hits"))
        elif entry.hit_count < 3:
            reasons.append((self.weights.lfu, "low_frequency"))

        if entry.last_hit is None:
            reasons.append((self.weights.recency, "never_accessed"))

        now = datetime.now(timezone.utc)
        if entry.first_seen.tzinfo is None:
            first_seen = entry.first_seen.replace(tzinfo=timezone.utc)
        else:
            first_seen = entry.first_seen

        age_days = (now - first_seen).total_seconds() / 86400
        if age_days > 30:
            reasons.append((self.weights.age, "old_entry"))

        if entry.bandwidth_saved == 0:
            reasons.append((self.weights.bandwidth, "no_bandwidth_saved"))

        if not reasons:
            return "low_combined_score"

        # Return the highest-weighted reason
        reasons.sort(key=lambda r: r[0], reverse=True)
        return reasons[0][1]

    def set_origin_latency(self, url: str, latency_ms: float) -> None:
        """Record origin latency for a URL.

        Args:
            url: Request URL.
            latency_ms: Origin response latency in milliseconds.
        """
        self._origin_latencies[url] = latency_ms

    def get_origin_latency(self, url: str) -> float:
        """Get recorded origin latency for a URL.

        Args:
            url: Request URL.

        Returns:
            Latency in milliseconds, or default if not recorded.
        """
        return self._origin_latencies.get(url, 100.0)

    def clear_origin_latencies(self) -> None:
        """Clear all recorded origin latencies."""
        self._origin_latencies.clear()

    def update_weights(self, weights: ScoringWeights) -> None:
        """Update scoring weights.

        Args:
            weights: New weights to use.
        """
        self.weights = weights.normalize()

    def get_weights(self) -> ScoringWeights:
        """Get current scoring weights.

        Returns:
            Current weights.
        """
        return self.weights
