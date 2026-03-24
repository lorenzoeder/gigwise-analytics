"""Unified dlt ingestion for Ticketmaster, Setlist.fm, and MusicBrainz.

Supports two pipeline modes controlled by PIPELINE_MODE env var:
  - prototype: small dataset, fast (<5 min). Uses TRACKED_ARTISTS, 1 TM country, 5 setlist pages.
  - production: full dataset (<1 hr). All 5 markets (US,CA,GB,DE,IT), monthly date chunks with
    auto-pagination (fetches all pages per chunk), 80 setlist pages, live artist discovery.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
from urllib import error
from urllib import parse, request

import dlt

MUSICBRAINZ_URL = "https://musicbrainz.org/ws/2/artist/"
TICKETMASTER_EVENTS_URL = "https://app.ticketmaster.com/discovery/v2/events.json"
SETLISTFM_SEARCH_URL = "https://api.setlist.fm/rest/1.0/search/setlists"
SETLISTFM_ARTIST_URL = "https://api.setlist.fm/rest/1.0/artist/{mbid}/setlists"

# Setlist.fm historical cutoff — only ingest setlists from this year onward
SETLISTFM_MIN_YEAR = 2000

# MusicBrainz resolution cache: stored in GCS for persistence across environments
MB_CACHE_BLOB = "cache/mb_artist_cache.json"

# Default markets for production mode
DEFAULT_PRODUCTION_COUNTRIES = "US,CA,GB,DE,IT"

# MusicBrainz artist types considered genuine music artists
_VALID_ARTIST_TYPES = {"Person", "Group", "Orchestra", "Choir"}


def _pipeline_mode() -> str:
    """Return 'prototype' or 'production'."""
    mode = os.getenv("PIPELINE_MODE", "prototype").strip().lower()
    if mode not in ("prototype", "production"):
        raise ValueError(f"PIPELINE_MODE must be 'prototype' or 'production', got '{mode}'")
    return mode


def _status(message: str) -> None:
    timestamp = datetime.now(timezone.utc).isoformat()
    print(f"[{timestamp}] {message}", flush=True)


@dataclass
class ArtistResolution:
    artist_name: str
    mbid: str | None
    origin_country: str | None
    formation_year: int | None
    artist_type: str | None
    primary_genre: str | None
    genres: list[str]


def _normalize_artist_name(name: str | None) -> str:
    return (name or "").strip().lower()


# ── MusicBrainz cache (GCS-backed) ─────────────────────────────────────

def _gcs_bucket():
    """Return the GCS bucket object, or None if not configured."""
    bucket_name = os.getenv("DATA_LAKE_BUCKET", "").strip()
    if not bucket_name:
        return None
    try:
        from google.cloud import storage as gcs
        client = gcs.Client(project=os.getenv("GCP_PROJECT_ID", ""))
        return client.bucket(bucket_name)
    except Exception as exc:
        _status(f"GCS cache unavailable ({exc}); using empty cache")
        return None


def _load_mb_cache() -> dict[str, dict[str, Any]]:
    """Load the persistent MusicBrainz resolution cache from GCS."""
    bucket = _gcs_bucket()
    if bucket is None:
        return {}
    blob = bucket.blob(MB_CACHE_BLOB)
    try:
        if blob.exists():
            data = json.loads(blob.download_as_text(encoding="utf-8"))
            if isinstance(data, dict):
                _status(f"Loaded MB cache from gs://{bucket.name}/{MB_CACHE_BLOB} ({len(data)} entries)")
                return data
    except Exception as exc:
        _status(f"Failed to load MB cache from GCS ({exc}); starting fresh")
    return {}


def _save_mb_cache(cache: dict[str, dict[str, Any]]) -> None:
    """Persist cache to GCS with upsert semantics."""
    bucket = _gcs_bucket()
    if bucket is None:
        _status("GCS cache not configured; skipping cache save")
        return
    blob = bucket.blob(MB_CACHE_BLOB)
    blob.upload_from_string(
        json.dumps(cache, indent=2, ensure_ascii=False),
        content_type="application/json",
    )
    _status(f"Saved MB cache to gs://{bucket.name}/{MB_CACHE_BLOB} ({len(cache)} entries)")


def _cache_key(artist_name: str) -> str:
    return _normalize_artist_name(artist_name)


# ── HTTP helper ─────────────────────────────────────────────────────────

def _http_json(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    data: bytes | None = None,
) -> dict[str, Any]:
    if params:
        query = parse.urlencode({k: v for k, v in params.items() if v is not None})
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}{query}"

    max_retries = int(os.getenv("HTTP_MAX_RETRIES", "3"))
    backoff_seconds = float(os.getenv("HTTP_RETRY_BACKOFF_SECONDS", "1.5"))

    payload = ""
    for attempt in range(1, max_retries + 1):
        req = request.Request(url=url, headers=headers or {}, data=data)
        try:
            with request.urlopen(req, timeout=30) as response:
                payload = response.read().decode("utf-8")
            break
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")[:500]
            retryable = exc.code in {429, 500, 502, 503, 504}
            if retryable and attempt < max_retries:
                wait_for = backoff_seconds * attempt
                _status(f"Retryable HTTP {exc.code} for {url}; retry {attempt}/{max_retries} in {wait_for:.1f}s")
                time.sleep(wait_for)
                continue
            raise RuntimeError(f"HTTP {exc.code} calling {url}. Response: {body}") from exc
        except error.URLError as exc:
            if attempt < max_retries:
                wait_for = backoff_seconds * attempt
                _status(f"Network error for {url}; retry {attempt}/{max_retries} in {wait_for:.1f}s")
                time.sleep(wait_for)
                continue
            raise RuntimeError(f"Network error calling {url}: {exc.reason}") from exc

    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        preview = payload[:500]
        raise RuntimeError(f"Invalid JSON from {url}. Payload preview: {preview}") from exc


# ── MusicBrainz ─────────────────────────────────────────────────────────

def _musicbrainz_artist(artist_name: str, user_agent: str, api_key: str | None = None) -> dict[str, Any] | None:
    params = {"query": f"artist:{artist_name}", "fmt": "json", "limit": 1}
    if api_key:
        params["token"] = api_key
    data = _http_json(MUSICBRAINZ_URL, headers={"User-Agent": user_agent}, params=params)
    artists = data.get("artists", [])
    return artists[0] if artists else None


def _resolve_artist(artist_name: str, mb_user_agent: str, mb_api_key: str | None) -> ArtistResolution:
    mb_artist = _musicbrainz_artist(artist_name, mb_user_agent, mb_api_key) or {}

    mb_begin_date = mb_artist.get("life-span", {}).get("begin") or ""
    formation_year = int(mb_begin_date[:4]) if len(mb_begin_date) >= 4 and mb_begin_date[:4].isdigit() else None

    mb_tags = mb_artist.get("tags") or []
    top_tag = max(mb_tags, key=lambda t: t.get("count", 0), default=None)
    primary_genre = top_tag.get("name") if top_tag else None

    return ArtistResolution(
        artist_name=artist_name,
        mbid=mb_artist.get("id"),
        origin_country=mb_artist.get("country"),
        formation_year=formation_year,
        artist_type=mb_artist.get("type"),
        primary_genre=primary_genre,
        genres=[t.get("name") for t in sorted(mb_tags, key=lambda t: t.get("count", 0), reverse=True) if t.get("name")],
    )


def _resolve_artist_cached(
    artist_name: str,
    mb_user_agent: str,
    mb_api_key: str | None,
    cache: dict[str, dict[str, Any]],
) -> ArtistResolution:
    """Resolve artist with persistent cache. Cache hit avoids API call."""
    key = _cache_key(artist_name)
    if key in cache:
        c = cache[key]
        return ArtistResolution(
            artist_name=c.get("artist_name", artist_name),
            mbid=c.get("mbid"),
            origin_country=c.get("origin_country"),
            formation_year=c.get("formation_year"),
            artist_type=c.get("artist_type"),
            primary_genre=c.get("primary_genre"),
            genres=c.get("genres", []),
        )

    # Rate-limit MusicBrainz: 1 req/s
    time.sleep(1.0)
    resolved = _resolve_artist(artist_name, mb_user_agent, mb_api_key)

    # Upsert into cache
    cache[key] = {
        "artist_name": resolved.artist_name,
        "mbid": resolved.mbid,
        "origin_country": resolved.origin_country,
        "formation_year": resolved.formation_year,
        "artist_type": resolved.artist_type,
        "primary_genre": resolved.primary_genre,
        "genres": resolved.genres,
    }
    return resolved


# ── Ticketmaster ────────────────────────────────────────────────────────

def _ticketmaster_events() -> list[dict[str, Any]]:
    api_key = os.getenv("TICKETMASTER_API_KEY", "").strip()
    if not api_key:
        raise ValueError("TICKETMASTER_API_KEY is required for Ticketmaster ingestion.")

    mode = _pipeline_mode()
    page_size = int(os.getenv("TICKETMASTER_PAGE_SIZE", "200"))
    min_upcoming = int(os.getenv("TICKETMASTER_MIN_UPCOMING_EVENTS", "3"))

    # In prototype mode, cap pages to keep runs fast. In production mode,
    # auto-paginate until the API returns no more results (max_pages=0 means unlimited).
    max_pages_default = "5" if mode == "prototype" else "0"
    max_pages = int(os.getenv("TICKETMASTER_MAX_PAGES", max_pages_default))

    # Country codes: multi-country in production, single (first) in prototype
    all_countries_str = os.getenv("TICKETMASTER_COUNTRY_CODES", DEFAULT_PRODUCTION_COUNTRIES)
    all_countries = [c.strip() for c in all_countries_str.split(",") if c.strip()]
    if mode == "production":
        countries = all_countries
    else:
        countries = all_countries[:1]  # prototype: only first country for speed

    # Dynamic date range: today -> 1 year forward, chunked to stay under
    # the TM API's ~1000 results-per-query limit.
    #   Production: 12 monthly chunks — each chunk is small enough to fit
    #               within the API limit even for the busiest markets.
    #   Prototype:  2 semi-annual chunks for speed.
    now = datetime.now(timezone.utc)
    if mode == "production":
        chunk_days = 31  # ~monthly
        num_chunks = 12
        date_chunks = [
            (now + timedelta(days=i * chunk_days), now + timedelta(days=min((i + 1) * chunk_days, 365)))
            for i in range(num_chunks)
        ]
    else:
        date_chunks = [
            (now, now + timedelta(days=182)),
            (now + timedelta(days=182), now + timedelta(days=365)),
        ]

    extracted_at = now.isoformat()
    seen_event_ids: set[str] = set()
    rows: list[dict[str, Any]] = []
    skipped = 0

    range_start = now.strftime("%Y-%m-%d")
    range_end = (now + timedelta(days=365)).strftime("%Y-%m-%d")

    _status(
        f"Fetching Ticketmaster events countries={','.join(countries)}, "
        f"pages<={'unlimited' if max_pages == 0 else max_pages}/chunk, chunks={len(date_chunks)}, "
        f"page_size={page_size}, min_upcoming={min_upcoming}, "
        f"date_range={range_start}..{range_end}"
    )

    for country_code in countries:
        _status(f"Ticketmaster market: {country_code}")
        for chunk_idx, (chunk_start, chunk_end) in enumerate(date_chunks):
            start_dt = chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_dt = chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ")
            _status(f"  Chunk {chunk_idx + 1}/{len(date_chunks)}: {start_dt[:10]}..{end_dt[:10]}")

            page = 0
            while True:
                if max_pages > 0 and page >= max_pages:
                    break
                data = _http_json(
                    TICKETMASTER_EVENTS_URL,
                    params={
                        "apikey": api_key,
                        "classificationName": "music",
                        "size": page_size,
                        "page": page,
                        "countryCode": country_code,
                        "sort": "date,asc",
                        "startDateTime": start_dt,
                        "endDateTime": end_dt,
                    },
                )

                events = data.get("_embedded", {}).get("events", [])
                if not events:
                    break

                for event in events:
                    attractions = event.get("_embedded", {}).get("attractions", [])
                    if not attractions:
                        skipped += 1
                        continue
                    primary_attraction = attractions[0]

                    # Filter: require minimum upcoming events
                    upcoming_total = (primary_attraction.get("upcomingEvents") or {}).get("_total", 0)
                    if upcoming_total < min_upcoming:
                        skipped += 1
                        continue

                    # Filter: exclude non-artist attraction types
                    classifications = primary_attraction.get("classifications") or []
                    attraction_type = ((classifications[0] if classifications else {}).get("type") or {}).get("name", "")
                    if attraction_type in ("Event Style", "Venue Based"):
                        skipped += 1
                        continue

                    # Filter: exclude cancelled/postponed events
                    event_status = (event.get("dates", {}).get("status") or {}).get("code", "")
                    if event_status in ("cancelled", "postponed"):
                        skipped += 1
                        continue

                    venue = (event.get("_embedded", {}).get("venues") or [{}])[0]
                    city = venue.get("city") or {}
                    country = venue.get("country") or {}
                    start = event.get("dates", {}).get("start") or {}

                    event_id = event.get("id")
                    if not event_id or event_id in seen_event_ids:
                        continue
                    seen_event_ids.add(event_id)

                    rows.append(
                        {
                            "event_id": event_id,
                            "event_name": event.get("name"),
                            "event_date": start.get("localDate"),
                            "event_time": start.get("localTime"),
                            "event_datetime_utc": start.get("dateTime"),
                            "artist_name": primary_attraction.get("name"),
                            "artist_mbid": None,
                            "venue_id": venue.get("id"),
                            "venue_name": venue.get("name"),
                            "city": city.get("name"),
                            "country_code": country.get("countryCode"),
                            "country_name": country.get("name"),
                            "event_status": event_status,
                            "event_url": event.get("url"),
                            "extracted_at": extracted_at,
                        }
                    )

                page += 1

    _status(f"Prepared Ticketmaster rows: {len(rows)} (skipped {skipped})")
    return rows


def _ticketmaster_touring_artists(ticketmaster_rows: list[dict[str, Any]]) -> list[str]:
    max_artists = int(os.getenv("MAX_TOURING_ARTISTS", "100"))
    names = sorted({(row.get("artist_name") or "").strip() for row in ticketmaster_rows if row.get("artist_name")})
    return names[:max_artists]


# ── Setlist.fm ──────────────────────────────────────────────────────────

def _setlistfm_setlists(artist_mbids: dict[str, str]) -> list[dict[str, Any]]:
    """Fetch setlists by MBID (preferred) or name search (fallback).
    Only includes setlists from year 2000 onward.
    """
    api_key = os.getenv("SETLISTFM_API_KEY", "").strip()
    if not api_key:
        raise ValueError("SETLISTFM_API_KEY is required for Setlist.fm ingestion.")

    mode = _pipeline_mode()
    max_pages = int(os.getenv("SETLISTFM_MAX_PAGES", "80" if mode == "production" else "5"))
    user_agent = os.getenv("SETLISTFM_USER_AGENT", "gigwise-analytics/0.1")
    extracted_at = datetime.now(timezone.utc).isoformat()

    headers = {
        "x-api-key": api_key,
        "Accept": "application/json",
        "User-Agent": user_agent,
    }

    rows: list[dict[str, Any]] = []
    _status(f"Fetching Setlist.fm setlists for {len(artist_mbids)} artists (max_pages={max_pages}, min_year={SETLISTFM_MIN_YEAR})")

    for index, (artist_name, mbid) in enumerate(artist_mbids.items(), start=1):
        _status(f"Setlist.fm artist {index}/{len(artist_mbids)}: {artist_name} (mbid={'yes' if mbid else 'no'})")
        normalized_search = _normalize_artist_name(artist_name) if not mbid else None
        hit_cutoff = False

        for page in range(1, max_pages + 1):
            if hit_cutoff:
                break
            try:
                if mbid:
                    url = SETLISTFM_ARTIST_URL.format(mbid=mbid)
                    data = _http_json(url, headers=headers, params={"p": page})
                else:
                    data = _http_json(
                        SETLISTFM_SEARCH_URL,
                        headers=headers,
                        params={"artistName": artist_name, "p": page},
                    )
            except RuntimeError as exc:
                if "HTTP 404" in str(exc):
                    break
                raise
            setlists = data.get("setlist") or []
            if isinstance(setlists, dict):
                setlists = [setlists]

            if not setlists:
                break

            for setlist in setlists:
                # For name-search fallback, strict-match to skip cover bands
                if not mbid:
                    returned_name = (setlist.get("artist") or {}).get("name") or ""
                    if _normalize_artist_name(returned_name) != normalized_search:
                        continue

                # Parse event date and apply year cutoff
                raw_date = setlist.get("eventDate") or ""
                event_year = None
                if len(raw_date) >= 4:
                    # Format is DD-MM-YYYY
                    parts = raw_date.split("-")
                    if len(parts) == 3 and parts[2].isdigit():
                        event_year = int(parts[2])
                if event_year is not None and event_year < SETLISTFM_MIN_YEAR:
                    hit_cutoff = True
                    break

                sets = (setlist.get("sets") or {}).get("set") or []
                if isinstance(sets, dict):
                    sets = [sets]

                songs: list[str] = []
                for set_item in sets:
                    set_songs = set_item.get("song") or []
                    if isinstance(set_songs, dict):
                        set_songs = [set_songs]
                    songs.extend([(song.get("name") or "").strip() for song in set_songs if song.get("name")])

                venue = setlist.get("venue") or {}
                city = venue.get("city") or {}
                country = city.get("country") or {}
                artist = setlist.get("artist") or {}

                rows.append(
                    {
                        "setlist_id": setlist.get("id"),
                        "event_date": raw_date,
                        "artist_name": artist.get("name") or artist_name,
                        "artist_mbid": artist.get("mbid"),
                        "tour_name": (setlist.get("tour") or {}).get("name"),
                        "venue_name": venue.get("name"),
                        "city": city.get("name"),
                        "country_code": country.get("code"),
                        "country_name": country.get("name"),
                        "song_count": len(songs),
                        "songs_played": "; ".join(songs) if songs else None,
                        "setlistfm_version": setlist.get("versionId"),
                        "setlistfm_last_updated": setlist.get("lastUpdated"),
                        "setlistfm_url": setlist.get("url"),
                        "extracted_at": extracted_at,
                    }
                )

    rows = [row for row in rows if row.get("setlist_id")]

    _status(f"Prepared Setlist.fm rows: {len(rows)}")
    return rows


# ── Artist selection ────────────────────────────────────────────────────

def _selected_artists(ticketmaster_rows: list[dict[str, Any]]) -> list[str]:
    mode = _pipeline_mode()

    if mode == "production":
        # Production: derive from Ticketmaster events
        artists = _ticketmaster_touring_artists(ticketmaster_rows)
        if not artists:
            raise ValueError("No touring artists found from Ticketmaster. Check API key and filters.")
        _status(f"Production mode: {len(artists)} artists from Ticketmaster events")
        return artists

    # Prototype: use tracked artist list
    raw = os.getenv("TRACKED_ARTISTS", "Coldplay,Radiohead,Arctic Monkeys").strip('"').strip("'")
    artists = [name.strip() for name in raw.split(",") if name.strip()]
    if not artists:
        raise ValueError("TRACKED_ARTISTS is empty. Set at least one artist name or use PIPELINE_MODE=production.")
    _status(f"Prototype mode: {len(artists)} tracked artists")
    return artists


# ── Main build ──────────────────────────────────────────────────────────

def _build_rows() -> dict[str, list[dict[str, Any]]]:
    mode = _pipeline_mode()
    _status(f"Pipeline mode: {mode}")

    mb_user_agent = os.getenv(
        "MUSICBRAINZ_USER_AGENT",
        "gigwise-analytics/0.1 (contact: your-email@example.com)",
    )
    mb_api_key = os.getenv("MUSICBRAINZ_API_KEY", "").strip() or None

    # Load persistent MusicBrainz cache
    mb_cache = _load_mb_cache()
    cache_hits_before = len(mb_cache)

    ticketmaster_rows = _ticketmaster_events()
    artists = _selected_artists(ticketmaster_rows)
    _status(f"Selected {len(artists)} artists for ingestion")

    now = datetime.now(timezone.utc)
    run_date = now.date().isoformat()
    extracted_at = f"{run_date}T00:00:00+00:00"

    mb_rows: list[dict[str, Any]] = []
    mbid_by_artist_name: dict[str, str] = {}
    artist_type_by_name: dict[str, str | None] = {}
    artist_mbid_map: dict[str, str] = {}

    for idx, artist_name in enumerate(artists, start=1):
        _status(f"Resolving artist {idx}/{len(artists)}: {artist_name}")
        try:
            resolved = _resolve_artist_cached(artist_name, mb_user_agent, mb_api_key, mb_cache)
        except Exception as exc:
            raise RuntimeError(f"Failed resolving artist '{artist_name}': {exc}") from exc

        mb_rows.append(
            {
                "artist_name": resolved.artist_name,
                "mbid": resolved.mbid,
                "origin_country": resolved.origin_country,
                "formation_year": resolved.formation_year,
                "artist_type": resolved.artist_type,
                "primary_genre": resolved.primary_genre,
                "extracted_at": extracted_at,
            }
        )

        normalized_name = _normalize_artist_name(resolved.artist_name)
        if resolved.mbid and normalized_name:
            mbid_by_artist_name[normalized_name] = resolved.mbid
            artist_type_by_name[normalized_name] = resolved.artist_type

        artist_mbid_map[artist_name] = resolved.mbid or ""

    setlist_rows = _setlistfm_setlists(artist_mbid_map)

    # Resolve unique Ticketmaster artists via MusicBrainz (with cache)
    # Production: resolve all. Prototype: limited to keep first run under 5 min.
    default_limit = "0" if mode == "production" else "50"
    resolve_limit = int(os.getenv("TICKETMASTER_RESOLVE_ARTISTS_LIMIT", default_limit))
    unique_ticketmaster_artists = sorted(
        {
            (row.get("artist_name") or "").strip()
            for row in ticketmaster_rows
            if (row.get("artist_name") or "").strip()
        }
    )
    artists_to_resolve = unique_ticketmaster_artists if resolve_limit == 0 else unique_ticketmaster_artists[:resolve_limit]
    if artists_to_resolve:
        _status(
            f"Resolving Ticketmaster artists (MusicBrainz) "
            f"(limit={'all' if resolve_limit == 0 else resolve_limit}, unique={len(unique_ticketmaster_artists)})"
        )
        for artist_name in artists_to_resolve:
            artist_key = _normalize_artist_name(artist_name)
            if artist_key in mbid_by_artist_name:
                continue
            try:
                resolved = _resolve_artist_cached(artist_name, mb_user_agent, mb_api_key, mb_cache)
            except Exception:
                continue
            if resolved.mbid:
                mbid_by_artist_name[artist_key] = resolved.mbid
                artist_type_by_name[artist_key] = resolved.artist_type
            mb_rows.append(
                {
                    "artist_name": resolved.artist_name,
                    "mbid": resolved.mbid,
                    "origin_country": resolved.origin_country,
                    "formation_year": resolved.formation_year,
                    "artist_type": resolved.artist_type,
                    "primary_genre": resolved.primary_genre,
                    "extracted_at": extracted_at,
                }
            )

    # Persist updated cache
    _save_mb_cache(mb_cache)
    cache_hits_after = len(mb_cache)
    _status(f"MusicBrainz cache: {cache_hits_before} entries before, {cache_hits_after} after")

    # Filter Ticketmaster rows: only keep genuine music artists with a MusicBrainz match
    enriched_ticketmaster_rows: list[dict[str, Any]] = []
    no_mb_match = 0
    non_artist_type = 0
    for row in ticketmaster_rows:
        artist_key = _normalize_artist_name(str(row.get("artist_name") or ""))
        mbid = mbid_by_artist_name.get(artist_key)
        if not mbid:
            no_mb_match += 1
            continue
        # Only keep genuine music artist types (Person, Group, Orchestra, Choir)
        atype = artist_type_by_name.get(artist_key)
        if atype and atype not in _VALID_ARTIST_TYPES:
            non_artist_type += 1
            continue
        enriched_row = dict(row)
        enriched_row["artist_mbid"] = mbid
        enriched_ticketmaster_rows.append(enriched_row)

    _status(
        f"Ticketmaster: {len(enriched_ticketmaster_rows)} events with verified artist match, "
        f"{no_mb_match} dropped (no MB match), {non_artist_type} dropped (non-artist type)"
    )

    _status(
        "Prepared rows: "
        f"ticketmaster={len(enriched_ticketmaster_rows)}, "
        f"setlistfm={len(setlist_rows)}, "
        f"musicbrainz={len(mb_rows)}"
    )
    return {
        "ticketmaster_events": enriched_ticketmaster_rows,
        "setlistfm_setlists": setlist_rows,
        "musicbrainz_artists": mb_rows,
    }


def _bigquery_location(project_id: str, dataset_name: str) -> str:
    try:
        from google.api_core.exceptions import NotFound
        from google.cloud import bigquery
    except ImportError as exc:
        raise RuntimeError(
            "BigQuery dependencies are missing. Run: uv sync"
        ) from exc

    client = bigquery.Client(project=project_id)
    dataset_ref = f"{project_id}.{dataset_name}"

    try:
        dataset = client.get_dataset(dataset_ref)
    except NotFound as exc:
        raise ValueError(
            f"BigQuery dataset {dataset_ref} was not found. "
            "Run 'make setup-infra' or set DLT_DATASET to an existing dataset."
        ) from exc

    location = (dataset.location or "").strip()
    if not location:
        raise ValueError(f"BigQuery dataset {dataset_ref} has no location metadata.")

    _status(f"Verified BigQuery dataset {dataset_ref} in location={location}")
    return location


def _run_dlt(rows: dict[str, list[dict[str, Any]]]) -> None:
    destination = os.getenv("DLT_DESTINATION", "bigquery")
    dataset_name = os.getenv("DLT_DATASET", "raw")
    _status(f"Initializing dlt pipeline destination={destination} dataset={dataset_name}")

    destination_config: Any = destination
    if destination.lower() == "bigquery":
        project_id = os.getenv("GCP_PROJECT_ID", "").strip()
        if not project_id:
            raise ValueError("GCP_PROJECT_ID is required when DLT_DESTINATION=bigquery.")

        detected_location = _bigquery_location(project_id, dataset_name)
        configured_location = os.getenv("DLT_BIGQUERY_LOCATION", "").strip()
        if configured_location and configured_location.upper() != detected_location.upper():
            raise ValueError(
                f"DLT_BIGQUERY_LOCATION={configured_location} does not match dataset location {detected_location}."
            )

        effective_location = configured_location or detected_location
        destination_config = dlt.destinations.bigquery(location=effective_location)
        _status(f"Using BigQuery job location={effective_location}")

    pipeline = dlt.pipeline(
        pipeline_name="gigwise_dlt_pipeline",
        destination=destination_config,
        dataset_name=dataset_name,
        progress="log",
    )

    @dlt.resource(
        name="ticketmaster_events",
        write_disposition="merge",
        primary_key="event_id",
        columns={
            "event_id": {"data_type": "text", "nullable": False},
            "artist_name": {"data_type": "text", "nullable": True},
            "artist_mbid": {"data_type": "text", "nullable": True},
            "event_date": {"data_type": "text", "nullable": True},
            "country_code": {"data_type": "text", "nullable": True},
            "venue_id": {"data_type": "text", "nullable": True},
            "event_status": {"data_type": "text", "nullable": True},
        },
    )
    def ticketmaster_events_resource() -> list[dict[str, Any]]:
        return rows["ticketmaster_events"]

    @dlt.resource(
        name="setlistfm_setlists",
        write_disposition="merge",
        primary_key="setlist_id",
    )
    def setlistfm_setlists_resource() -> list[dict[str, Any]]:
        return rows["setlistfm_setlists"]

    @dlt.resource(
        name="musicbrainz_artists",
        write_disposition="merge",
        primary_key="mbid",
    )
    def musicbrainz_artists_resource() -> list[dict[str, Any]]:
        return rows["musicbrainz_artists"]

    _status("Running dlt load")
    load_info = pipeline.run(
        [
            ticketmaster_events_resource(),
            setlistfm_setlists_resource(),
            musicbrainz_artists_resource(),
        ]
    )
    _status("dlt load completed successfully")
    print(load_info, flush=True)

    # Clean up the transient staging dataset dlt creates for merge operations
    from google.cloud import bigquery as bq_client

    client = bq_client.Client(project=os.environ.get("GCP_PROJECT_ID"))
    staging_dataset = f"{dataset_name}_staging"
    try:
        client.delete_dataset(staging_dataset, delete_contents=True, not_found_ok=True)
        _status(f"Cleaned up staging dataset: {staging_dataset}")
    except Exception as exc:  # noqa: BLE001
        _status(f"Warning: could not drop staging dataset {staging_dataset}: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print counts without loading")
    args = parser.parse_args()

    _status("Starting dlt ingestion")
    try:
        rows = _build_rows()

        if args.dry_run:
            _status(
                "Dry run complete: "
                f"ticketmaster={len(rows['ticketmaster_events'])}, "
                f"setlistfm={len(rows['setlistfm_setlists'])}, "
                f"musicbrainz={len(rows['musicbrainz_artists'])}"
            )
            return

        _run_dlt(rows)
    except Exception as exc:
        print(f"[ERROR] Ingestion failed: {exc}", file=sys.stderr, flush=True)
        if os.getenv("DLT_DEBUG_TRACEBACK", "0") == "1":
            traceback.print_exc()
        raise SystemExit(1)


if __name__ == "__main__":
    main()
