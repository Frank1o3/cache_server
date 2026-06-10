"""Validation utilities for cache operations.

This module provides functions for determining cacheability and expiration
of HTTP responses. These validators support future HTTP caching behavior.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Final

# Cacheable HTTP status codes
CACHEABLE_STATUS_CODES: Final[frozenset[int]] = frozenset({
    200,  # OK
    203,  # Non-Authoritative Information
    204,  # No Content
    206,  # Partial Content
    300,  # Multiple Choices
    301,  # Moved Permanently
    308,  # Permanent Redirect
})

# Non-cacheable HTTP status codes (explicitly)
NON_CACHEABLE_STATUS_CODES: Final[frozenset[int]] = frozenset({
    201,  # Created
    202,  # Accepted
    205,  # Reset Content
    302,  # Found (unless explicitly cached)
    303,  # See Other
    304,  # Not Modified
    307,  # Temporary Redirect
    400,  # Bad Request
    401,  # Unauthorized
    403,  # Forbidden
    404,  # Not Found
    405,  # Method Not Allowed
    410,  # Gone
    414,  # URI Too Long
    416,  # Range Not Satisfiable
    423,  # Locked
    500,  # Internal Server Error
    502,  # Bad Gateway
    503,  # Service Unavailable
    504,  # Gateway Timeout
})

# Cacheable HTTP methods
CACHEABLE_METHODS: Final[frozenset[str]] = frozenset({
    "GET",
    "HEAD",
    "OPTIONS",
})

# Non-cacheable HTTP methods
NON_CACHEABLE_METHODS: Final[frozenset[str]] = frozenset({
    "POST",
    "PUT",
    "DELETE",
    "PATCH",
    "CONNECT",
    "TRACE",
})

# Content types that are generally cacheable
CACHEABLE_CONTENT_TYPES: Final[frozenset[str]] = frozenset({
    "text/html",
    "text/css",
    "text/plain",
    "text/xml",
    "application/json",
    "application/javascript",
    "application/x-javascript",
    "text/javascript",
    "application/xml",
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/svg+xml",
    "image/webp",
    "image/avif",
    "image/x-icon",
    "image/vnd.microsoft.icon",
    "font/woff",
    "font/woff2",
    "application/font-woff",
    "application/font-woff2",
    "audio/mpeg",
    "audio/ogg",
    "audio/webm",
    "video/mp4",
    "video/webm",
    "video/ogg",
    "application/octet-stream",
})

# Content types that should never be cached
NON_CACHEABLE_CONTENT_TYPES: Final[frozenset[str]] = frozenset({
    "text/event-stream",  # Server-Sent Events
    "application/x-server-side-parsed-html",  # SSHTML
})


def is_cacheable_status(status_code: int) -> bool:
    """Check if an HTTP status code is cacheable.

    Args:
        status_code: The HTTP status code to check.

    Returns:
        True if the status code is cacheable, False otherwise.

    Examples:
        >>> is_cacheable_status(200)
        True
        >>> is_cacheable_status(404)
        False
    """
    if status_code in CACHEABLE_STATUS_CODES:
        return True
    if status_code in NON_CACHEABLE_STATUS_CODES:
        return False
    # Default: 2xx and 3xx are potentially cacheable
    return 200 <= status_code < 400


def is_cacheable_method(method: str) -> bool:
    """Check if an HTTP method is cacheable.

    Args:
        method: The HTTP method to check (case-insensitive).

    Returns:
        True if the method is cacheable, False otherwise.

    Examples:
        >>> is_cacheable_method("GET")
        True
        >>> is_cacheable_method("POST")
        False
    """
    method_upper = method.upper()
    if method_upper in CACHEABLE_METHODS:
        return True
    if method_upper in NON_CACHEABLE_METHODS:
        return False
    # Default: only GET, HEAD, OPTIONS are cacheable
    return False


def is_cacheable_content_type(content_type: str | None) -> bool:
    """Check if a content type is cacheable.

    Args:
        content_type: The Content-Type header value.

    Returns:
        True if the content type is cacheable, False otherwise.

    Examples:
        >>> is_cacheable_content_type("text/html")
        True
        >>> is_cacheable_content_type("text/event-stream")
        False
    """
    if content_type is None:
        return True  # Unknown content types may be cacheable

    # Extract base content type (ignore charset and other parameters)
    base_type = content_type.split(";")[0].strip().lower()

    if base_type in NON_CACHEABLE_CONTENT_TYPES:
        return False
    if base_type in CACHEABLE_CONTENT_TYPES:
        return True

    # Default: text/* and application/* are generally cacheable
    if base_type.startswith("text/") or base_type.startswith("application/"):
        return True
    if base_type.startswith("image/") or base_type.startswith("video/") or base_type.startswith("audio/"):
        return True

    return True


def is_expired(entry_expiration: datetime | None, current_time: datetime | None = None) -> bool:
    """Check if a cache entry has expired.

    Args:
        entry_expiration: The expiration datetime of the entry.
        current_time: Optional current time for testing. Defaults to now.

    Returns:
        True if the entry is expired, False if still valid.

    Examples:
        >>> from datetime import datetime, timezone, timedelta
        >>> future = datetime.now(timezone.utc) + timedelta(hours=1)
        >>> is_expired(future)
        False
        >>> past = datetime.now(timezone.utc) - timedelta(hours=1)
        >>> is_expired(past)
        True
    """
    if entry_expiration is None:
        return False

    if current_time is None:
        current_time = datetime.now(timezone.utc)

    # Ensure both datetimes are timezone-aware
    if entry_expiration.tzinfo is None:
        entry_expiration = entry_expiration.replace(tzinfo=timezone.utc)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=timezone.utc)

    return current_time > entry_expiration


def should_revalidate(
    entry_etag: str | None,
    entry_last_modified: str | None,
    request_headers: dict[str, str],
    response_headers: dict[str, str],
) -> bool:
    """Determine if a cache entry needs revalidation with the origin.

    Implements HTTP cache validation logic based on ETag and Last-Modified.

    Args:
        entry_etag: ETag from the cached response.
        entry_last_modified: Last-Modified from the cached response.
        request_headers: Headers from the incoming request.
        response_headers: Headers from the origin response (if available).

    Returns:
        True if revalidation is needed, False if cache can be used directly.

    Examples:
        >>> should_revalidate("abc123", None, {}, {})
        False
        >>> should_revalidate(None, None, {"cache-control": "no-cache"}, {})
        True
    """
    # Check request directives that force revalidation
    cache_control = request_headers.get("cache-control", "").lower()

    if "no-cache" in cache_control:
        return True
    if "max-age=0" in cache_control:
        return True

    # Check pragma header (HTTP/1.0 compatibility)
    pragma = request_headers.get("pragma", "").lower()
    if "no-cache" in pragma:
        return True

    # If we have ETag or Last-Modified, the entry can be validated
    # This doesn't mean it MUST be validated, just that it CAN be
    if entry_etag is not None or entry_last_modified is not None:
        # Check if origin requires validation (must-revalidate)
        resp_cache_control = response_headers.get("cache-control", "").lower()
        if "must-revalidate" in resp_cache_control:
            return True
        if "proxy-revalidate" in resp_cache_control:
            return True

    return False


def ttl_from_headers(headers: dict[str, str], default_ttl: int = 3600) -> int:
    """Calculate TTL from HTTP cache headers.

    Parses Cache-Control and Expires headers to determine remaining TTL.

    Args:
        headers: HTTP response headers.
        default_ttl: Default TTL in seconds if no headers present.

    Returns:
        TTL in seconds (0 means no-cache, -1 means do-not-cache).

    Examples:
        >>> ttl_from_headers({"cache-control": "max-age=300"})
        300
        >>> ttl_from_headers({"cache-control": "no-store"})
        -1
    """
    cache_control = headers.get("cache-control", "").lower()

    # Check for no-store (do not cache)
    if "no-store" in cache_control:
        return -1

    # Check for max-age
    if "max-age=" in cache_control:
        try:
            max_age_str = cache_control.split("max-age=")[1].split(",")[0]
            max_age = int(max_age_str.strip())
            return max_age
        except (IndexError, ValueError):
            pass

    # Check for s-maxage (shared cache)
    if "s-maxage=" in cache_control:
        try:
            s_maxage_str = cache_control.split("s-maxage=")[1].split(",")[0]
            s_maxage = int(s_maxage_str.strip())
            return s_maxage
        except (IndexError, ValueError):
            pass

    # Check Expires header
    expires = headers.get("expires")
    if expires:
        try:
            # Parse HTTP date format
            from email.utils import parsedate_to_datetime
            expires_dt = parsedate_to_datetime(expires)
            if expires_dt.tzinfo is None:
                expires_dt = expires_dt.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            ttl = int((expires_dt - now).total_seconds())
            return max(0, ttl)
        except (ValueError, TypeError):
            pass

    # Check for no-cache (can cache but must revalidate)
    if "no-cache" in cache_control:
        return 0

    # Return default TTL
    return default_ttl


def parse_cache_control(header_value: str) -> dict[str, str | bool | int]:
    """Parse Cache-Control header into a dictionary.

    Args:
        header_value: The Cache-Control header value.

    Returns:
        Dictionary of directives with their values.

    Examples:
        >>> parse_cache_control("max-age=300, public, no-transform")
        {'max-age': 300, 'public': True, 'no-transform': True}
    """
    result: dict[str, str | bool | int] = {}

    if not header_value:
        return result

    directives = [d.strip() for d in header_value.split(",")]

    for directive in directives:
        if "=" in directive:
            key, value = directive.split("=", 1)
            key = key.strip().lower()
            value = value.strip()
            # Try to parse as integer
            try:
                result[key] = int(value)
            except ValueError:
                result[key] = value
        else:
            result[directive.lower()] = True

    return result


def compute_freshness_lifetime(
    headers: dict[str, str],
    status_code: int,
    default_ttl: int = 3600,
) -> int:
    """Compute the freshness lifetime of a response.

    Implements RFC 7234 Section 4.2.1 for computing freshness lifetime.

    Args:
        headers: HTTP response headers.
        status_code: HTTP status code.
        default_ttl: Default TTL if no explicit headers.

    Returns:
        Freshness lifetime in seconds.
    """
    # First, try explicit Cache-Control directives
    cache_control = parse_cache_control(headers.get("cache-control", ""))

    if "s-maxage" in cache_control:
        val = cache_control["s-maxage"]
        if isinstance(val, int):
            return val
    if "max-age" in cache_control:
        val = cache_control["max-age"]
        if isinstance(val, int):
            return val

    # Try Expires header
    expires = headers.get("expires")
    date = headers.get("date")

    if expires and date:
        try:
            from email.utils import parsedate_to_datetime
            expires_dt = parsedate_to_datetime(expires)
            date_dt = parsedate_to_datetime(date)
            if expires_dt.tzinfo is None:
                expires_dt = expires_dt.replace(tzinfo=timezone.utc)
            if date_dt.tzinfo is None:
                date_dt = date_dt.replace(tzinfo=timezone.utc)
            delta = int((expires_dt - date_dt).total_seconds())
            return max(0, delta)
        except (ValueError, TypeError):
            pass

    # Heuristic for responses without explicit expiration
    # Use 10% of age since Last-Modified
    last_modified = headers.get("last-modified")
    if last_modified:
        try:
            from email.utils import parsedate_to_datetime
            modified_dt = parsedate_to_datetime(last_modified)
            if modified_dt.tzinfo is None:
                modified_dt = modified_dt.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            age = (now - modified_dt).total_seconds()
            heuristic_ttl = int(age * 0.1)
            # Cap heuristic TTL
            return min(heuristic_ttl, default_ttl)
        except (ValueError, TypeError):
            pass

    return default_ttl
