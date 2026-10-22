"""
upstash_cache.py

Shared Upstash Redis client for the "is this title already on Telegram"
cache - used by BOTH the Render backend (serves /cache/check, /debrid/leech,
/debrid/status to the app) and the GitHub Actions upload worker (writes
progress/status while it downloads a magnet via aria2c and uploads the
result to Telegram via MTProto). Keeping this one module shared between
both means the two can never disagree about a key format or a row's shape.

Replaces the old Supabase `telegram_cache` table. Same fields, one JSON
blob per key instead of one database row:

    key:   cache:{imdb_id}:{quality}
    value: {
        "imdb_id":       str,
        "quality":       str,
        "magnet_link":   str | None,
        "info_hash":     str | None,
        "file_id":       str | None,   # Telegram file reference once uploaded
        "file_size":     int | None,   # bytes
        "status":        "queued" | "downloading" | "uploading" | "cached" | "error",
        "progress":      float,        # 0-100
        "downloaded_mb": float,
        "total_mb":      float,
        "error_msg":     str | None,
        "created_at":    str,          # ISO 8601, UTC
        "updated_at":    str,
    }

Two small side-index keys make "what's cached for this title" and "what's
been cached recently" cheap, without ever having to scan the whole
database:

    title:{imdb_id}:qualities   a Redis SET of every quality string ever
                                 upserted for that title - lets find_all()
                                 do an SADD-then-GET instead of a key-pattern
                                 scan.
    cache:index                 a Redis SORTED SET, one "{imdb_id}:{quality}"
                                 member per title that has actually reached
                                 "cached", scored by the time it got there -
                                 a future "Available Instantly" row is one
                                 ZRANGE call against this, nothing more to
                                 build.

Auth: two environment variables, set as secrets wherever this runs (Render
environment variables; GitHub Actions repository secrets) - the official
SDK's Redis.from_env() reads these on its own:
    UPSTASH_REDIS_REST_URL
    UPSTASH_REDIS_REST_TOKEN

Dependency: `pip install upstash-redis` (the official SDK - handles auth,
retries and JSON decoding of the REST API's responses; this module just
calls it the same way either caller would).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

from upstash_redis import Redis

_INDEX_KEY = "cache:index"

_STATUSES = {"queued", "downloading", "uploading", "cached", "error"}

_redis: Optional[Redis] = None


def _client() -> Redis:
    """Lazy singleton - built on first use so importing this module never
    requires the environment variables to already be set (useful for the
    GitHub Actions worker, where they're injected a step after checkout)."""
    global _redis
    if _redis is None:
        _redis = Redis.from_env()
    return _redis


def _key(imdb_id: str, quality: str) -> str:
    return f"cache:{imdb_id}:{quality}"


def _qualities_key(imdb_id: str) -> str:
    return f"title:{imdb_id}:qualities"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _decode(raw: Any) -> Optional[dict]:
    if raw is None:
        return None
    return json.loads(raw)


def find(imdb_id: str, quality: str) -> Optional[dict]:
    """The cache row for one exact imdb_id+quality, or None if it has
    never been requested."""
    return _decode(_client().get(_key(imdb_id, quality)))


def find_all(imdb_id: str) -> list[dict]:
    """Every quality ever requested for this title - queued, failed, or
    cached, in whatever order Redis returns the set in (callers that care
    about order should sort by `updated_at`)."""
    client = _client()
    qualities = client.smembers(_qualities_key(imdb_id))
    rows = []
    for quality in qualities:
        row = _decode(client.get(_key(imdb_id, quality)))
        if row is not None:
            rows.append(row)
    return rows


def upsert_job(
    imdb_id: str,
    quality: str,
    *,
    magnet_link: Optional[str] = None,
    info_hash: Optional[str] = None,
    status: str = "queued",
) -> dict:
    """Creates a fresh row - called when a leech is first requested for
    this imdb_id+quality (also safe to call again to reset a failed job
    back to "queued" for a retry)."""
    if status not in _STATUSES:
        raise ValueError(f"unknown status {status!r}, must be one of {_STATUSES}")

    now = _now()
    row = {
        "imdb_id": imdb_id,
        "quality": quality,
        "magnet_link": magnet_link,
        "info_hash": info_hash,
        "file_id": None,
        "file_size": None,
        "status": status,
        "progress": 0.0,
        "downloaded_mb": 0.0,
        "total_mb": 0.0,
        "error_msg": None,
        "created_at": now,
        "updated_at": now,
    }
    client = _client()
    client.set(_key(imdb_id, quality), json.dumps(row))
    client.sadd(_qualities_key(imdb_id), quality)
    return row


def _update(imdb_id: str, quality: str, **changes: Any) -> dict:
    """Merges `changes` into the existing row and writes the whole thing
    back - Redis has no partial-JSON-update command, so read-modify-write
    it is. Falls back to creating the row if it's somehow missing (e.g.
    the worker crashed and restarted without the queued row surviving),
    so none of the status-transition helpers below can raise just because
    upsert_job() wasn't called first."""
    row = find(imdb_id, quality) or upsert_job(imdb_id, quality)
    row.update(changes)
    row["updated_at"] = _now()
    _client().set(_key(imdb_id, quality), json.dumps(row))
    return row


def update_progress(
    imdb_id: str,
    quality: str,
    *,
    downloaded_mb: float,
    total_mb: float,
) -> dict:
    """Called repeatedly by the Actions worker while aria2c downloads -
    progress is derived from the two MB figures rather than passed in
    separately, so the two numbers can never disagree with each other."""
    progress = round((downloaded_mb / total_mb) * 100, 1) if total_mb > 0 else 0.0
    return _update(
        imdb_id,
        quality,
        status="downloading",
        downloaded_mb=downloaded_mb,
        total_mb=total_mb,
        progress=progress,
    )


def mark_uploading(imdb_id: str, quality: str) -> dict:
    """Download finished, Telegram upload starting."""
    return _update(imdb_id, quality, status="uploading", progress=100.0)


def mark_cached(
    imdb_id: str,
    quality: str,
    *,
    file_id: str,
    file_size: int,
) -> dict:
    """The Telegram upload finished - this is what makes /cache/check
    start returning "instant" for this title+quality. Also records it in
    the recently-cached index (see module docstring)."""
    row = _update(
        imdb_id,
        quality,
        status="cached",
        file_id=file_id,
        file_size=file_size,
        progress=100.0,
    )
    _client().zadd(_INDEX_KEY, {f"{imdb_id}:{quality}": datetime.now(timezone.utc).timestamp()})
    return row


def mark_error(imdb_id: str, quality: str, error_msg: str) -> dict:
    return _update(imdb_id, quality, status="error", error_msg=error_msg)


def recently_cached(limit: int = 20) -> list[dict]:
    """Newest-first {"imdb_id", "quality"} pairs that have reached
    "cached" - for a future "Available Instantly" row. Returns just the
    identifiers, not the full rows (call find() for a specific one if
    the caller needs file_id/size too) so this stays cheap to call often."""
    members = _client().zrange(_INDEX_KEY, 0, limit - 1, rev=True)
    out = []
    for member in members:
        imdb_id, quality = member.rsplit(":", 1)
        out.append({"imdb_id": imdb_id, "quality": quality})
    return out
