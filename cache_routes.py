"""
cache_routes.py

FastAPI routes for the Telegram-cache system (storage lives in
upstash_cache.py - this file is just the HTTP layer on top of it). Mount
into your existing app with:

    from cache_routes import router as cache_router
    app.include_router(cache_router)

Three endpoints, called by the Flutter app's DebridService:
    GET  /cache/isthere   - is this title+quality already cached?
    POST /server/leech    - start caching it (queues the row, then
                             triggers the GitHub Actions worker)
    GET  /server/status   - poll progress while it's being cached

(isthere/status return the exact same shape today - kept as two routes
because the app calls them with different intent: a one-shot check before
showing the source picker, vs. a repeated poll while a leech is running.)

Environment variables this file needs (set in Render's dashboard):
    GITHUB_TOKEN        a token with access to fire repository_dispatch on
                         the WORKER repo (fine-grained: "Contents: write"
                         is enough - that scope covers dispatches)
    GITHUB_OWNER         e.g. "zsdev07"
    GITHUB_REPO          the worker repo's name - NOT this backend's repo
    GITHUB_EVENT_TYPE    the repository_dispatch event_type the worker's
                          workflow listens for (default "leech_magnet")
    APP_SHARED_SECRET    optional, but worth setting. If present,
                          /server/leech requires a matching `X-App-Secret`
                          header. Without this, anyone who finds this URL
                          can fire your GitHub Actions workflow for free -
                          burning your Actions minutes and filling the
                          Telegram channel with whatever they want cached.
                          A static shared secret baked into the APK isn't
                          bulletproof (anyone can pull it out of the app
                          binary) but it stops casual/automated abuse for
                          free; if that's not enough once this is public,
                          the next step up is a short-lived signed request
                          instead of a fixed string - worth revisiting then,
                          not before.
"""

from __future__ import annotations

import os
from typing import Optional

import httpx
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

import upstash_cache as cache

router = APIRouter()

_GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
_GITHUB_OWNER = os.environ.get("GITHUB_OWNER", "")
_GITHUB_REPO = os.environ.get("GITHUB_REPO", "")
_GITHUB_EVENT_TYPE = os.environ.get("GITHUB_EVENT_TYPE", "leech_magnet")
_APP_SHARED_SECRET = os.environ.get("APP_SHARED_SECRET")  # None/"" = disabled


# ── request / response shapes ─────────────────────────────────────────────

class LeechRequest(BaseModel):
    imdb_id: str
    quality: str
    magnet_link: str
    info_hash: Optional[str] = None


class CacheStatusResponse(BaseModel):
    exists: bool
    imdb_id: str
    quality: str
    status: Optional[str] = None
    progress: Optional[float] = None
    downloaded_mb: Optional[float] = None
    total_mb: Optional[float] = None
    file_id: Optional[str] = None
    file_size: Optional[int] = None
    error_msg: Optional[str] = None


def _to_response(imdb_id: str, quality: str, row: Optional[dict]) -> CacheStatusResponse:
    if row is None:
        return CacheStatusResponse(exists=False, imdb_id=imdb_id, quality=quality)
    return CacheStatusResponse(
        exists=True,
        imdb_id=imdb_id,
        quality=quality,
        status=row.get("status"),
        progress=row.get("progress"),
        downloaded_mb=row.get("downloaded_mb"),
        total_mb=row.get("total_mb"),
        file_id=row.get("file_id"),
        file_size=row.get("file_size"),
        error_msg=row.get("error_msg"),
    )


def _require_secret(x_app_secret: Optional[str]) -> None:
    if _APP_SHARED_SECRET and x_app_secret != _APP_SHARED_SECRET:
        raise HTTPException(status_code=401, detail="missing/invalid X-App-Secret")


# ── routes ─────────────────────────────────────────────────────────────────

@router.get("/cache/isthere", response_model=CacheStatusResponse)
async def cache_isthere(imdb_id: str, quality: str) -> CacheStatusResponse:
    """Quick existence+status check - the hot path, hit every time the app
    opens the source picker. Read-only, no side effects."""
    row = cache.find(imdb_id, quality)
    return _to_response(imdb_id, quality, row)


@router.post("/server/leech", response_model=CacheStatusResponse)
async def server_leech(
    body: LeechRequest,
    x_app_secret: Optional[str] = Header(default=None),
) -> CacheStatusResponse:
    """Starts caching a title: writes a "queued" row immediately (so an
    instant follow-up /server/status poll has something to show), then
    fires the GitHub Actions worker. If a job for this exact imdb_id+
    quality already exists and isn't in "error", returns that existing
    row instead of starting a duplicate - the app can retry a failed one
    simply by calling this again."""
    _require_secret(x_app_secret)

    existing = cache.find(body.imdb_id, body.quality)
    if existing is not None and existing["status"] != "error":
        return _to_response(body.imdb_id, body.quality, existing)

    row = cache.upsert_job(
        body.imdb_id,
        body.quality,
        magnet_link=body.magnet_link,
        info_hash=body.info_hash,
        status="queued",
    )

    if not (_GITHUB_TOKEN and _GITHUB_OWNER and _GITHUB_REPO):
        # A misconfiguration, not a client error - the row is queued but
        # nothing will ever pick it up. Surfaced as 500 instead of letting
        # the app poll /server/status forever against a job nobody's
        # working on.
        raise HTTPException(
            status_code=500,
            detail="GITHUB_TOKEN/GITHUB_OWNER/GITHUB_REPO not configured",
        )

    url = f"https://api.github.com/repos/{_GITHUB_OWNER}/{_GITHUB_REPO}/dispatches"
    payload = {
        "event_type": _GITHUB_EVENT_TYPE,
        "client_payload": {
            "imdb_id": body.imdb_id,
            "quality": body.quality,
            "magnet_link": body.magnet_link,
            "info_hash": body.info_hash,
        },
    }
    headers = {
        "Authorization": f"Bearer {_GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
    }

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload, headers=headers)
        resp.raise_for_status()
    except httpx.HTTPError as e:
        cache.mark_error(body.imdb_id, body.quality, f"could not start worker: {e}")
        raise HTTPException(status_code=502, detail=f"GitHub dispatch failed: {e}")

    return _to_response(body.imdb_id, body.quality, row)


@router.get("/server/status", response_model=CacheStatusResponse)
async def server_status(imdb_id: str, quality: str) -> CacheStatusResponse:
    """Polled by the app every few seconds while a leech is in progress."""
    row = cache.find(imdb_id, quality)
    return _to_response(imdb_id, quality, row)
