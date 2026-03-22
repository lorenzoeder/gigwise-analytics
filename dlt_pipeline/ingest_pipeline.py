"""Unified dlt ingestion for Ticketmaster, Setlist.fm, MusicBrainz, and Spotify."""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib import error
from urllib import parse, request

import dlt

MUSICBRAINZ_URL = "https://musicbrainz.org/ws/2/artist/"
SPOTIFY_SEARCH_URL = "https://api.spotify.com/v1/search"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
TICKETMASTER_EVENTS_URL = "https://app.ticketmaster.com/discovery/v2/events.json"
SETLISTFM_SEARCH_URL = "https://api.setlist.fm/rest/1.0/search/setlists"


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
    spotify_id: str | None
    primary_genre: str | None
    genres: list[str]
    spotify_popularity: int | None
    spotify_followers: int | None


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


def _spotify_access_token(client_id: str, client_secret: str) -> str:
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    payload = parse.urlencode({"grant_type": "client_credentials"}).encode("utf-8")
    token_resp = _http_json(
        SPOTIFY_TOKEN_URL,
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data=payload,
    )
    return token_resp["access_token"]


def _musicbrainz_artist(artist_name: str, user_agent: str, api_key: str | None = None) -> dict[str, Any] | None:
    params = {"query": f"artist:{artist_name}", "fmt": "json", "limit": 1}
    if api_key:
        # Optional key passthrough for setups using a keyed gateway/proxy.
        params["token"] = api_key
    data = _http_json(MUSICBRAINZ_URL, headers={"User-Agent": user_agent}, params=params)
    artists = data.get("artists", [])
    return artists[0] if artists else None


def _spotify_artist(artist_name: str, token: str) -> dict[str, Any] | None:
    data = _http_json(
        SPOTIFY_SEARCH_URL,
        headers={"Authorization": f"Bearer {token}"},
        params={"q": artist_name, "type": "artist", "limit": 1},
    )
    items = data.get("artists", {}).get("items", [])
    return items[0] if items else None


def _ticketmaster_events() -> list[dict[str, Any]]:
    api_key = os.getenv("TICKETMASTER_API_KEY", "").strip()
    if not api_key:
        raise ValueError("TICKETMASTER_API_KEY is required for Ticketmaster ingestion.")

    max_pages = int(os.getenv("TICKETMASTER_MAX_PAGES", "3"))
    page_size = int(os.getenv("TICKETMASTER_PAGE_SIZE", "200"))
    country_code = os.getenv("TICKETMASTER_COUNTRY_CODE", "GB")
    extracted_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []

    _status(
        f"Fetching Ticketmaster events country={country_code}, pages<={max_pages}, page_size={page_size}"
    )

    for page in range(max_pages):
        _status(f"Ticketmaster page {page + 1}/{max_pages}")
        data = _http_json(
            TICKETMASTER_EVENTS_URL,
            params={
            "apikey": api_key,
            "classificationName": "music",
            "size": page_size,
            "page": page,
            "countryCode": country_code,
            "sort": "date,asc",
            },
        )

        events = data.get("_embedded", {}).get("events", [])
        if not events:
            break

        for event in events:
            attractions = event.get("_embedded", {}).get("attractions", [])
            primary_attraction = attractions[0] if attractions else {}
            venue = (event.get("_embedded", {}).get("venues") or [{}])[0]
            city = venue.get("city") or {}
            country = venue.get("country") or {}
            start = event.get("dates", {}).get("start") or {}
            price_ranges = event.get("priceRanges") or []
            first_price = price_ranges[0] if price_ranges else {}

            event_id = event.get("id")
            if not event_id:
                continue

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
                    "currency": first_price.get("currency"),
                    "min_price_gbp": first_price.get("min"),
                    "max_price_gbp": first_price.get("max"),
                    "event_status": (event.get("dates", {}).get("status") or {}).get("code"),
                    "event_url": event.get("url"),
                    "extracted_at": extracted_at,
                }
            )

    _status(f"Prepared Ticketmaster rows: {len(rows)}")
    return rows


def _ticketmaster_touring_artists(ticketmaster_rows: list[dict[str, Any]]) -> list[str]:
    max_artists = int(os.getenv("MAX_TOURING_ARTISTS", "100"))
    names = sorted({(row.get("artist_name") or "").strip() for row in ticketmaster_rows if row.get("artist_name")})
    return names[:max_artists]


def _setlistfm_setlists(artist_names: list[str]) -> list[dict[str, Any]]:
    api_key = os.getenv("SETLISTFM_API_KEY", "").strip()
    if not api_key:
        raise ValueError("SETLISTFM_API_KEY is required for Setlist.fm ingestion.")

    max_pages = int(os.getenv("SETLISTFM_MAX_PAGES", "1"))
    user_agent = os.getenv("SETLISTFM_USER_AGENT", "gigwise-analytics/0.1")
    extracted_at = datetime.now(timezone.utc).isoformat()

    headers = {
        "x-api-key": api_key,
        "Accept": "application/json",
        "User-Agent": user_agent,
    }

    rows: list[dict[str, Any]] = []
    _status(f"Fetching Setlist.fm setlists for {len(artist_names)} artists")

    for index, artist_name in enumerate(artist_names, start=1):
        _status(f"Setlist.fm artist {index}/{len(artist_names)}: {artist_name}")
        for page in range(1, max_pages + 1):
            data = _http_json(
                SETLISTFM_SEARCH_URL,
                headers=headers,
                params={"artistName": artist_name, "p": page},
            )
            setlists = data.get("setlist") or []
            if isinstance(setlists, dict):
                setlists = [setlists]

            if not setlists:
                break

            for setlist in setlists:
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
                        "event_date": setlist.get("eventDate"),
                        "artist_name": artist.get("name") or artist_name,
                        "artist_mbid": artist.get("mbid"),
                        "tour_name": (setlist.get("tour") or {}).get("name"),
                        "venue_name": venue.get("name"),
                        "city": city.get("name"),
                        "country_code": country.get("code"),
                        "country_name": country.get("name"),
                        "song_count": len(songs),
                        "songs": songs,
                        "setlistfm_version": setlist.get("versionId"),
                        "setlistfm_last_updated": setlist.get("lastUpdated"),
                        "setlistfm_url": setlist.get("url"),
                        "extracted_at": extracted_at,
                    }
                )

    rows = [row for row in rows if row.get("setlist_id")]

    _status(f"Prepared Setlist.fm rows: {len(rows)}")
    return rows


def _resolve_artist(artist_name: str, mb_user_agent: str, spotify_token: str, mb_api_key: str | None) -> ArtistResolution:
    mb_artist = _musicbrainz_artist(artist_name, mb_user_agent, mb_api_key) or {}
    sp_artist = _spotify_artist(artist_name, spotify_token) or {}

    mb_begin_date = mb_artist.get("life-span", {}).get("begin") or ""
    formation_year = int(mb_begin_date[:4]) if len(mb_begin_date) >= 4 and mb_begin_date[:4].isdigit() else None

    genres = sp_artist.get("genres") or []
    return ArtistResolution(
        artist_name=artist_name,
        mbid=mb_artist.get("id"),
        origin_country=mb_artist.get("country"),
        formation_year=formation_year,
        artist_type=mb_artist.get("type"),
        spotify_id=sp_artist.get("id"),
        primary_genre=genres[0] if genres else None,
        genres=genres,
        spotify_popularity=sp_artist.get("popularity"),
        spotify_followers=(sp_artist.get("followers") or {}).get("total"),
    )


def _selected_artists(ticketmaster_rows: list[dict[str, Any]]) -> list[str]:
    mode = os.getenv("ARTIST_SELECTION_MODE", "tracked").strip().lower()
    _status(f"Artist selection mode: {mode}")

    if mode == "ticketmaster_live":
        artists = _ticketmaster_touring_artists(ticketmaster_rows)
        if not artists:
            raise ValueError("No touring artists found from Ticketmaster. Check API key and filters.")
        return artists

    raw = os.getenv("TRACKED_ARTISTS", "Coldplay,Radiohead,Arctic Monkeys")
    artists = [name.strip() for name in raw.split(",") if name.strip()]
    if not artists:
        raise ValueError("TRACKED_ARTISTS is empty. Set at least one artist name or use ARTIST_SELECTION_MODE=ticketmaster_live.")
    return artists


def _build_rows() -> dict[str, list[dict[str, Any]]]:
    spotify_client_id = os.getenv("SPOTIFY_CLIENT_ID", "")
    spotify_client_secret = os.getenv("SPOTIFY_CLIENT_SECRET", "")
    if not spotify_client_id or not spotify_client_secret:
        raise ValueError("Missing SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET.")

    mb_user_agent = os.getenv(
        "MUSICBRAINZ_USER_AGENT",
        "gigwise-analytics/0.1 (contact: your-email@example.com)",
    )
    mb_api_key = os.getenv("MUSICBRAINZ_API_KEY", "").strip() or None

    _status("Requesting Spotify access token")
    token = _spotify_access_token(spotify_client_id, spotify_client_secret)
    _status("Spotify access token acquired")

    ticketmaster_rows = _ticketmaster_events()
    artists = _selected_artists(ticketmaster_rows)
    _status(f"Selected {len(artists)} artists for snapshot ingestion")
    setlist_rows = _setlistfm_setlists(artists)

    now = datetime.now(timezone.utc)
    snapshot_date = now.date().isoformat()
    # Keep a stable per-day timestamp so reruns on the same day upsert, not append.
    extracted_at = f"{snapshot_date}T00:00:00+00:00"

    mb_rows: list[dict[str, Any]] = []
    sp_rows: list[dict[str, Any]] = []

    for idx, artist_name in enumerate(artists, start=1):
        _status(f"Resolving artist {idx}/{len(artists)}: {artist_name}")
        try:
            resolved = _resolve_artist(artist_name, mb_user_agent, token, mb_api_key)
        except Exception as exc:
            raise RuntimeError(f"Failed resolving artist '{artist_name}': {exc}") from exc

        mb_rows.append(
            {
                "artist_name": resolved.artist_name,
                "mbid": resolved.mbid,
                "origin_country": resolved.origin_country,
                "formation_year": resolved.formation_year,
                "artist_type": resolved.artist_type,
                "spotify_id": resolved.spotify_id,
                "extracted_at": extracted_at,
            }
        )

        sp_rows.append(
            {
                "artist_name": resolved.artist_name,
                "artist_id": resolved.spotify_id,
                "mbid": resolved.mbid,
                "name": resolved.artist_name,
                "primary_genre": resolved.primary_genre,
                "genres": resolved.genres,
                "popularity": resolved.spotify_popularity,
                "followers": resolved.spotify_followers,
                "snapshot_date": snapshot_date,
                "extracted_at": extracted_at,
            }
        )

    _status(
        "Prepared rows: "
        f"ticketmaster={len(ticketmaster_rows)}, "
        f"setlistfm={len(setlist_rows)}, "
        f"musicbrainz={len(mb_rows)}, "
        f"spotify={len(sp_rows)}"
    )
    return {
        "ticketmaster_events": ticketmaster_rows,
        "setlistfm_setlists": setlist_rows,
        "musicbrainz_artists": mb_rows,
        "spotify_artists": sp_rows,
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
        pipeline_name="gigwise_dlt_snapshots",
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
            "currency": {"data_type": "text", "nullable": True},
            "min_price_gbp": {"data_type": "double", "nullable": True},
            "max_price_gbp": {"data_type": "double", "nullable": True},
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
        primary_key=["artist_name", "extracted_at"],
    )
    def musicbrainz_artists_resource() -> list[dict[str, Any]]:
        return rows["musicbrainz_artists"]

    @dlt.resource(
        name="spotify_artists",
        write_disposition="merge",
        primary_key=["artist_name", "snapshot_date"],
        columns={
            "primary_genre": {"data_type": "text", "nullable": True},
            "popularity": {"data_type": "bigint", "nullable": True},
            "followers": {"data_type": "bigint", "nullable": True},
        },
    )
    def spotify_artists_resource() -> list[dict[str, Any]]:
        return rows["spotify_artists"]

    _status("Running dlt load")
    load_info = pipeline.run(
        [
            ticketmaster_events_resource(),
            setlistfm_setlists_resource(),
            musicbrainz_artists_resource(),
            spotify_artists_resource(),
        ]
    )
    _status("dlt load completed successfully")
    print(load_info, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print counts without loading")
    args = parser.parse_args()

    _status("Starting dlt snapshot ingestion")
    try:
        rows = _build_rows()

        if args.dry_run:
            _status(
                "Dry run complete: "
                f"ticketmaster={len(rows['ticketmaster_events'])}, "
                f"setlistfm={len(rows['setlistfm_setlists'])}, "
                f"musicbrainz={len(rows['musicbrainz_artists'])}, "
                f"spotify={len(rows['spotify_artists'])}"
            )
            if rows["spotify_artists"]:
                print(f"Sample Spotify row keys: {sorted(rows['spotify_artists'][0].keys())}", flush=True)
            return

        _run_dlt(rows)
    except Exception as exc:
        print(f"[ERROR] Ingestion failed: {exc}", file=sys.stderr, flush=True)
        if os.getenv("DLT_DEBUG_TRACEBACK", "0") == "1":
            traceback.print_exc()
        raise SystemExit(1)


if __name__ == "__main__":
    main()
