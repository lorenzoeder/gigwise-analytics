"""Unified dlt ingestion for Ticketmaster, Setlist.fm, and MusicBrainz.

Supports two pipeline modes controlled by PIPELINE_MODE env var:
  - prototype: small dataset, fast (<5 min). Uses TRACKED_ARTISTS, 1 TM country, 5 setlist pages.
  - production: full dataset (<30 min first run, <15 min subsequent). All 5 markets
    (US,CA,GB,DE,IT), monthly date chunks, 10 setlist pages. Artist selection uses
    name-heuristic pre-filter + MusicBrainz type/disambiguation post-filter. Only the
    top ~100 touring candidates are resolved via MusicBrainz (not all unique TM artists).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import traceback
import unicodedata
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

# Name patterns that indicate non-genuine touring music artists.
# Applied as a cheap pre-filter BEFORE any API-based resolution.
_EXCLUDE_NAME_RE = re.compile(
    r'(?i)'
    r'(?:'
    r'\btribut\w*\b'                # "Tribute", "Tributes"
    r'|\bimpersonat\w*\b'            # "impersonator"
    r'|\bcirque\b'                   # Cirque du Soleil etc.
    r'|\bon\s+ice\b'                # "Disney on Ice" etc.
    r'|\bday\s+out\s+with\b'        # "Day Out with Thomas" etc.
    r'|LIVE\s+[\u2013\u2014-]'       # "MJ LIVE – ..." tribute format
    r'|\bhologram\b'                # hologram shows
    r'|\bthe\s+\w+\s+experience\b'  # "The Hendrix Experience" etc.
    r'|\bopry\b'                    # Grand Ole Opry etc.
    r'|\bcelebrat\w*\b'             # "A Celebration of ..."
    r'|\bmusical\b'                 # "The Musical", musicals
    r'|\bbroadway\b'               # Broadway shows
    r'|\bspectacular\b'            # "Christmas Spectacular" etc.
    r'|\bresidency\b'              # Vegas residencies
    r'|\blegacy\b'                 # "Michael Jackson - Legacy" etc.
    r'|\b(?:sounds?|songs?|music|echoes?|rumours?)\s+of\b'  # "Rumours of Fleetwood Mac" etc.
    r'|\b(?:salute|homage)\s+to\b'  # "A Salute to the Stars"
    r')')

# Keywords in MusicBrainz disambiguation that indicate tribute/cover acts
_TRIBUTE_DISAMBIG_KW = ("tribute", "cover band", "impersonat", "tribute band", "cover")


def _is_likely_non_artist(name: str) -> bool:
    """Heuristic check \u2014 True for names clearly not genuine touring artists."""
    return bool(_EXCLUDE_NAME_RE.search(name))


def _normalize_for_match(name: str) -> str:
    """Aggressively normalize a name for MB match comparison.

    Handles accented characters (e.g., León → leon), strips 'The' prefix,
    removes punctuation, and collapses whitespace.
    """
    s = (name or "").strip()
    # Decompose Unicode → strip combining marks (accents)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r'^the\s+', '', s)
    # Normalize "&" → "and" BEFORE stripping punctuation so that
    # "Jools Holland & His Orchestra" matches "... and His Orchestra"
    s = re.sub(r'\s*&\s*', ' and ', s)
    s = re.sub(r'[^a-z0-9\s]', '', s)  # strip punctuation
    return re.sub(r'\s+', ' ', s).strip()


def _strip_tm_qualifier(name: str) -> str:
    """Strip parenthetical location/tour qualifiers that Ticketmaster appends.

    Examples: 'Donny Osmond (Las Vegas)' → 'Donny Osmond'
              'Keith Urban (US Tour)'    → 'Keith Urban'
    """
    return re.sub(r'\s*\([^)]*\)\s*$', '', name).strip()


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
    disambiguation: str | None = None
    mb_name: str | None = None  # name returned by MB (for match validation)


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
    max_retries: int | None = None,
) -> dict[str, Any]:
    if params:
        query = parse.urlencode({k: v for k, v in params.items() if v is not None})
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}{query}"

    if max_retries is None:
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
                # Honour Retry-After header if present
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                if retry_after and retry_after.isdigit():
                    wait_for = int(retry_after) + 1
                else:
                    wait_for = backoff_seconds * (2 ** (attempt - 1))  # exponential backoff
                _status(f"Retryable HTTP {exc.code} for {url}; retry {attempt}/{max_retries} in {wait_for:.1f}s")
                time.sleep(wait_for)
                continue
            raise RuntimeError(f"HTTP {exc.code} calling {url}. Response: {body}") from exc
        except error.URLError as exc:
            if attempt < max_retries:
                wait_for = backoff_seconds * (2 ** (attempt - 1))
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
    """Search MB for an artist, returning the best name-matched result from top 5.

    MusicBrainz fuzzy search often returns wrong artists first (e.g., "J. Cole"
    returns "Nat King Cole" at rank 1). We fetch 5 candidates and pick the one
    whose name best matches the query.

    Shadow-artist check: if the matched candidate has very few tags but a
    much-more-established artist with a similar name also appears in the
    results, the match is likely a cover/tribute act (e.g., "Australian
    Bee Gees" shadowed by "Bee Gees"). In that case we return None.
    """
    clean_name = _strip_tm_qualifier(artist_name)
    params = {"query": f"artist:{clean_name}", "fmt": "json", "limit": 5}
    if api_key:
        params["token"] = api_key
    data = _http_json(MUSICBRAINZ_URL, headers={"User-Agent": user_agent}, params=params)
    candidates = data.get("artists", [])
    if not candidates:
        return None

    searched_norm = _normalize_for_match(clean_name)
    if not searched_norm:
        return None

    matched: dict[str, Any] | None = None

    # Pass 1: exact normalized match
    for c in candidates:
        if _normalize_for_match(c.get("name", "")) == searched_norm:
            matched = c
            break

    # Pass 2: prefix match — only when the candidate name is LONGER than or equal
    # to the searched name.  This prevents tribute acts like "Michael Jackson - Legacy"
    # from prefix-matching the real "Michael Jackson" (searched is longer → no match).
    if matched is None:
        for c in candidates:
            cand_norm = _normalize_for_match(c.get("name", ""))
            if cand_norm and cand_norm.startswith(searched_norm) and len(cand_norm) >= len(searched_norm):
                matched = c
                break

    if matched is None:
        _status(f"  MB no name match in top 5 for {artist_name!r}: "
                f"{[c.get('name') for c in candidates[:3]]}")
        return None

    # ── Shadow-artist check ─────────────────────────────────────────
    # If the matched MB entry has very few tags and another candidate in the
    # results is a well-established artist whose name is contained within
    # the matched name, the match is almost certainly a cover/tribute act.
    #   "Australian Bee Gees" (0 tags) + "Bee Gees" (15 tags) → cover act
    # Direction: only trigger when the famous name is a SUBSTRING of the
    # matched name (cover acts ADD qualifiers to the famous name).
    # Threshold: the famous artist must have ≥10 tags to be considered
    # "established enough" to cast a shadow.  This prevents false positives
    # like Sugar (6) vs Sugar Ray (14) or Freya Skye (1) vs Skye (4).
    matched_norm = _normalize_for_match(matched.get("name", ""))
    matched_tags = len(matched.get("tags") or [])
    for other in candidates:
        if other is matched:
            continue
        other_norm = _normalize_for_match(other.get("name", ""))
        other_tags = len(other.get("tags") or [])
        if not other_norm or len(other_norm) < 4:
            continue
        # Famous name must be contained IN the matched name (not the reverse)
        if other_norm not in matched_norm:
            continue
        # The shadow must be genuinely established (≥10 tags)
        if other_tags >= 10 and other_tags >= matched_tags + 3:
            _status(f"  MB shadow-artist detected: {matched.get('name')!r} ({matched_tags} tags) "
                    f"shadowed by {other.get('name')!r} ({other_tags} tags)")
            return None

    return matched


def _resolve_artist(artist_name: str, mb_user_agent: str, mb_api_key: str | None) -> ArtistResolution:
    mb_artist = _musicbrainz_artist(artist_name, mb_user_agent, mb_api_key)
    if mb_artist is None:
        return ArtistResolution(
            artist_name=artist_name, mbid=None, origin_country=None,
            formation_year=None, artist_type=None, primary_genre=None,
            genres=[], disambiguation=None, mb_name=None,
        )

    mb_returned_name = mb_artist.get("name") or ""
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
        disambiguation=mb_artist.get("disambiguation"),
        mb_name=mb_returned_name,
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
        # Invalidate stale cache entries that are missing newer fields
        # (e.g., 'mb_name' added for match-quality validation).
        if "mb_name" not in c:
            _status(f"  Cache entry for {artist_name!r} is stale (missing mb_name); re-resolving")
            del cache[key]
        else:
            return ArtistResolution(
                artist_name=c.get("artist_name", artist_name),
                mbid=c.get("mbid"),
                origin_country=c.get("origin_country"),
                formation_year=c.get("formation_year"),
                artist_type=c.get("artist_type"),
                primary_genre=c.get("primary_genre"),
                genres=c.get("genres", []),
                disambiguation=c.get("disambiguation"),
                mb_name=c.get("mb_name"),
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
        "disambiguation": resolved.disambiguation,
        "mb_name": resolved.mb_name,
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
                    first_class = classifications[0] if classifications else {}
                    attraction_type = (first_class.get("type") or {}).get("name", "")
                    if attraction_type in ("Event Style", "Venue Based"):
                        skipped += 1
                        continue

                    # Extract TM genre for downstream filtering
                    tm_genre = (first_class.get("genre") or {}).get("name", "")
                    tm_subgenre = (first_class.get("subGenre") or {}).get("name", "")

                    # Filter: exclude cancelled/postponed events
                    event_status = (event.get("dates", {}).get("status") or {}).get("code", "")
                    if event_status in ("cancelled", "postponed"):
                        skipped += 1
                        continue

                    venue = (event.get("_embedded", {}).get("venues") or [{}])[0]
                    city = venue.get("city") or {}
                    country = venue.get("country") or {}
                    start = event.get("dates", {}).get("start") or {}

                    # Clean artist name: strip TM qualifiers like "(Las Vegas)"
                    raw_artist = primary_attraction.get("name") or ""
                    artist_name = _strip_tm_qualifier(raw_artist)

                    # Filter: skip if attraction name matches or is contained
                    # in the venue name (indicates venue listed as the "artist").
                    # E.g., "Holiday Inn" artist at "Holiday Inn Resort" venue.
                    venue_name = venue.get("name") or ""
                    norm_artist = _normalize_for_match(artist_name)
                    norm_venue = _normalize_for_match(venue_name)
                    if norm_artist and norm_venue and len(norm_artist) > 3 and (
                        norm_artist in norm_venue or norm_venue in norm_artist
                    ):
                        skipped += 1
                        continue

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
                            "artist_name": artist_name,
                            "artist_mbid": None,
                            "venue_id": venue.get("id"),
                            "venue_name": venue_name,
                            "city": city.get("name"),
                            "country_code": country.get("countryCode"),
                            "country_name": country.get("name"),
                            "event_status": event_status,
                            "event_url": event.get("url"),
                            "tm_genre": tm_genre or None,
                            "tm_subgenre": tm_subgenre or None,
                            "extracted_at": extracted_at,
                        }
                    )

                page += 1

    _status(f"Prepared Ticketmaster rows: {len(rows)} (skipped {skipped})")
    return rows


def _ticketmaster_touring_artists(ticketmaster_rows: list[dict[str, Any]]) -> list[str]:
    """Return top artists ranked by event count (descending) across all markets."""
    max_artists = int(os.getenv("MAX_TOURING_ARTISTS", "120"))
    counts: dict[str, int] = {}
    for row in ticketmaster_rows:
        name = (row.get("artist_name") or "").strip()
        if name:
            counts[name] = counts.get(name, 0) + 1
    ranked = sorted(counts, key=lambda n: (-counts[n], n))
    _status(f"Top touring artists: {', '.join(f'{n} ({counts[n]})' for n in ranked[:5])}...")
    return ranked[:max_artists]


# ── Setlist.fm ──────────────────────────────────────────────────────────

def _setlistfm_setlists(artist_mbids: dict[str, str]) -> list[dict[str, Any]]:
    """Fetch setlists by MBID (preferred) or name search (fallback).
    Only includes setlists from year 2000 onward.
    """
    api_key = os.getenv("SETLISTFM_API_KEY", "").strip()
    if not api_key:
        raise ValueError("SETLISTFM_API_KEY is required for Setlist.fm ingestion.")

    mode = _pipeline_mode()
    max_pages = int(os.getenv("SETLISTFM_MAX_PAGES", "10" if mode == "production" else "5"))
    user_agent = os.getenv("SETLISTFM_USER_AGENT", "gigwise-analytics/0.1")
    extracted_at = datetime.now(timezone.utc).isoformat()

    headers = {
        "x-api-key": api_key,
        "Accept": "application/json",
        "User-Agent": user_agent,
    }

    # Setlist.fm hard rate limits (https://api.setlist.fm/docs):
    #   - max 2.0 requests/second
    #   - max 1440 requests/day
    # We enforce both. The per-second limit is handled by a minimum delay
    # (0.5s = 2 req/s). The daily limit is tracked with a counter.
    setlistfm_delay = max(float(os.getenv("SETLISTFM_REQUEST_DELAY", "1.0")), 0.5)
    setlistfm_retries = int(os.getenv("SETLISTFM_MAX_RETRIES", "5"))
    daily_limit = int(os.getenv("SETLISTFM_DAILY_LIMIT", "1400"))  # stay ~40 under the 1440 hard limit
    current_delay = setlistfm_delay  # adaptive: doubles on 429
    request_count = 0

    rows: list[dict[str, Any]] = []
    skipped_artists: list[str] = []
    _status(
        f"Fetching Setlist.fm setlists for {len(artist_mbids)} artists "
        f"(max_pages={max_pages}, min_year={SETLISTFM_MIN_YEAR}, "
        f"delay={setlistfm_delay}s, daily_limit={daily_limit})"
    )
    daily_limit_hit = False

    for index, (artist_name, mbid) in enumerate(artist_mbids.items(), start=1):
        if daily_limit_hit:
            skipped_artists.append(artist_name)
            continue
        _status(f"Setlist.fm artist {index}/{len(artist_mbids)}: {artist_name} (mbid={'yes' if mbid else 'no'})")
        if not mbid:
            _status(f"  Skipping {artist_name!r}: no MBID (Setlist.fm requires MBID)")
            skipped_artists.append(artist_name)
            continue
        hit_cutoff = False

        for page in range(1, max_pages + 1):
            if hit_cutoff:
                break
            if request_count >= daily_limit:
                _status(f"  Daily request limit reached ({request_count}/{daily_limit}); stopping Setlist.fm ingestion")
                daily_limit_hit = True
                break
            time.sleep(current_delay)
            request_count += 1
            try:
                url = SETLISTFM_ARTIST_URL.format(mbid=mbid)
                data = _http_json(url, headers=headers, params={"p": page}, max_retries=setlistfm_retries)
            except RuntimeError as exc:
                if "HTTP 404" in str(exc):
                    break
                if "HTTP 429" in str(exc):
                    current_delay = min(current_delay * 2, 10.0)
                    _status(f"  Rate-limited on {artist_name}; delay now {current_delay:.1f}s, skipping remaining pages")
                    skipped_artists.append(artist_name)
                    break
                raise
            setlists = data.get("setlist") or []
            if isinstance(setlists, dict):
                setlists = [setlists]

            if not setlists:
                break

            for setlist in setlists:
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

    if skipped_artists:
        _status(f"Setlist.fm: skipped {len(skipped_artists)} artists due to rate limits / daily cap: {', '.join(skipped_artists[:10])}")
    _status(f"Prepared Setlist.fm rows: {len(rows)} (API requests used: {request_count}/{daily_limit})")
    return rows



# ── Main build ──────────────────────────────────────────────────────────

def _build_rows() -> dict[str, list[dict[str, Any]]]:
    """Build all output rows using a multi-phase pipeline.

    Production pipeline (6 phases):
      1. Fetch Ticketmaster events
      2. Pre-filter: remove obvious non-artists via name heuristics (instant)
      3. Rank remaining artists by event count, select top N
      4. MB-resolve ONLY those ~100 candidates (not all 1000+ unique TM artists)
      5. Post-filter by MB artist_type + disambiguation
      6. Enrich TM rows & fetch Setlist.fm for verified artists

    This avoids the old bottleneck of resolving every unique TM artist via MB.
    """
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

    now = datetime.now(timezone.utc)
    run_date = now.date().isoformat()
    extracted_at = f"{run_date}T00:00:00+00:00"

    # ── Phase 1: Fetch Ticketmaster events ──────────────────────────────
    ticketmaster_rows = _ticketmaster_events()

    # ── Phase 2: Select candidate artists ───────────────────────────────
    if mode == "production":
        # Pre-filter: remove obvious non-artists using cheap name heuristics
        pre_filtered_rows = [
            row for row in ticketmaster_rows
            if not _is_likely_non_artist(row.get("artist_name") or "")
        ]
        excluded_count = len(ticketmaster_rows) - len(pre_filtered_rows)
        if excluded_count:
            _status(f"Pre-filter: excluded {excluded_count} events matching non-artist name patterns")

        # Rank remaining artists by event count, pick top N
        artists = _ticketmaster_touring_artists(pre_filtered_rows)
        if not artists:
            raise ValueError("No touring artists found from Ticketmaster. Check API key and filters.")
        _status(f"Production mode: {len(artists)} candidate artists from Ticketmaster events")
    else:
        pre_filtered_rows = ticketmaster_rows
        raw = os.getenv("TRACKED_ARTISTS", "Coldplay,Radiohead,Arctic Monkeys").strip('"').strip("'")
        artists = [name.strip() for name in raw.split(",") if name.strip()]
        if not artists:
            raise ValueError("TRACKED_ARTISTS is empty. Set at least one artist name or use PIPELINE_MODE=production.")
        _status(f"Prototype mode: {len(artists)} tracked artists")

    # ── Phase 3: MB-resolve selected artists only ───────────────────────
    # Only resolves ~100 selected candidates (not all 1000+ unique TM artists).
    # Cached artists resolve instantly; uncached require 1 API call per second.
    mb_rows: list[dict[str, Any]] = []
    mbid_by_artist: dict[str, str] = {}
    artist_type_by_name: dict[str, str | None] = {}
    disambig_by_name: dict[str, str] = {}

    _status(f"Resolving {len(artists)} artists via MusicBrainz (cache has {len(mb_cache)} entries)")
    for idx, artist_name in enumerate(artists, start=1):
        if idx % 25 == 0:
            _status(f"  Resolved {idx}/{len(artists)} artists...")
        key = _normalize_artist_name(artist_name)
        if key in mbid_by_artist:
            continue
        try:
            resolved = _resolve_artist_cached(artist_name, mb_user_agent, mb_api_key, mb_cache)
        except Exception:
            continue
        if resolved.mbid:
            mbid_by_artist[key] = resolved.mbid
            artist_type_by_name[key] = resolved.artist_type
            disambig_by_name[key] = resolved.disambiguation or ""
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

    # ── Phase 4: Post-filter by MB artist type + disambiguation ─────────
    verified_keys: set[str] = set()
    no_mbid = 0
    non_artist_type = 0
    tribute_disambig = 0
    for artist_name in artists:
        key = _normalize_artist_name(artist_name)
        mbid = mbid_by_artist.get(key)
        if not mbid:
            no_mbid += 1
            _status(f"  Rejected {artist_name!r}: no MB match")
            continue
        atype = artist_type_by_name.get(key)
        if atype and atype not in _VALID_ARTIST_TYPES:
            non_artist_type += 1
            _status(f"  Rejected {artist_name!r}: type={atype}")
            continue
        # Check disambiguation for tribute/cover indicators
        disambig = disambig_by_name.get(key, "").lower()
        if any(kw in disambig for kw in _TRIBUTE_DISAMBIG_KW):
            tribute_disambig += 1
            _status(f"  Rejected {artist_name!r}: disambiguation={disambig!r}")
            continue
        verified_keys.add(key)

    _status(
        f"Artist verification: {len(verified_keys)} verified, "
        f"{no_mbid} no MBID, {non_artist_type} non-artist type, "
        f"{tribute_disambig} tribute/cover (disambiguation)"
    )

    # ── Phase 5: Enrich TM rows with MBIDs (verified artists only) ─────
    enriched_ticketmaster_rows: list[dict[str, Any]] = []
    for row in pre_filtered_rows:
        key = _normalize_artist_name(row.get("artist_name") or "")
        if key not in verified_keys:
            continue
        enriched_row = dict(row)
        enriched_row["artist_mbid"] = mbid_by_artist.get(key)
        enriched_ticketmaster_rows.append(enriched_row)

    _status(f"Ticketmaster: {len(enriched_ticketmaster_rows)} events with verified artists")

    # ── Phase 6: Setlist.fm ingestion (MBID-only, no name fallback) ─────
    artist_mbid_map: dict[str, str] = {}
    for name in artists:
        key = _normalize_artist_name(name)
        mbid = mbid_by_artist.get(key, "")
        if key in verified_keys and mbid:
            artist_mbid_map[name] = mbid

    # Skip artists whose setlists are already in BigQuery
    existing_mbids = _existing_setlist_mbids()
    if existing_mbids:
        before = len(artist_mbid_map)
        artist_mbid_map = {n: m for n, m in artist_mbid_map.items() if m not in existing_mbids}
        skipped = before - len(artist_mbid_map)
        _status(f"Setlist.fm: skipping {skipped}/{before} artists already in dataset")

    setlist_rows = _setlistfm_setlists(artist_mbid_map)

    # Persist updated cache
    _save_mb_cache(mb_cache)
    _status(f"MusicBrainz cache: {cache_hits_before} entries before, {len(mb_cache)} after")

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


def _existing_setlist_mbids() -> set[str]:
    """Return MBIDs already present in the raw.setlistfm_setlists table."""
    destination = os.getenv("DLT_DESTINATION", "bigquery").lower()
    if destination != "bigquery":
        return set()
    project_id = os.getenv("GCP_PROJECT_ID", "").strip()
    dataset_name = os.getenv("DLT_DATASET", "raw")
    if not project_id:
        return set()
    try:
        from google.cloud import bigquery as bq
        client = bq.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_name}.setlistfm_setlists"
        query = f"SELECT DISTINCT artist_mbid FROM `{table_ref}` WHERE artist_mbid IS NOT NULL"
        rows = client.query(query).result()
        mbids = {row.artist_mbid for row in rows}
        _status(f"Found {len(mbids)} artists already in setlistfm_setlists")
        return mbids
    except Exception as exc:
        _status(f"Could not query existing setlist artists (table may not exist yet): {exc}")
        return set()


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
