# dlt Unified Ingestion

This folder contains the primary ingestion pipeline for all external APIs. It orchestrates API calls, quality filtering, and idempotent loading into BigQuery.

## Data sources

### Ticketmaster events -> `raw.ticketmaster_events`

- **What:** Upcoming and near-future concerts across US, CA, GB, DE, IT markets (1-year forward window)
- **Key fields:** `event_id` (merge key), `artist_name`, `artist_mbid`, `event_date`, `country_code`, `venue_name`, `event_status`
- **Quality filtering:** Events without an artist attraction are skipped. Artists with fewer than 3 upcoming events are filtered out (removes cover bands, tribute acts, unknowns). Cancelled/postponed events excluded.

### Setlist.fm setlists -> `raw.setlistfm_setlists`

- **What:** Historical performance data with song-level detail (year 2000 onward)
- **Key fields:** `setlist_id` (merge key), `artist_mbid`, `event_date`, `songs_played` (semicolon-delimited), `tour_name`
- **Quality filtering:** Strict artist name matching -- only setlists where the returned artist name exactly matches the search term (case-insensitive). Prevents cover/tribute band data from polluting results.
- **Idempotency:** Before fetching, the pipeline queries BigQuery for MBIDs already present in `raw.setlistfm_setlists` and skips those artists entirely.

### MusicBrainz artists -> `raw.musicbrainz_artists`

- **What:** Canonical artist metadata -- MBID, origin country, formation year, artist type, and genre via crowd-sourced tags
- **Key fields:** `mbid` (merge key), `artist_name`, `primary_genre` (highest-voted tag), `artist_type`
- **Caching:** Resolutions are cached in GCS (`gs://<DATA_LAKE_BUCKET>/cache/mb_artist_cache.json`). Subsequent runs skip API calls for already-resolved artists.

## Pipeline modes

Controlled by `PIPELINE_MODE`:

| Setting | Behaviour | Runtime |
|---|---|---|
| `prototype` (default) | Uses `TRACKED_ARTISTS`, 1 TM country, 2 date chunks, 5 setlist pages/artist, 5 TM pages/chunk | < 5 min |
| `production` | All 5 markets, 12 monthly date chunks, auto-pagination, 80 setlist pages, artists derived from TM events | < 1 hr |

## Ingestion phases

The pipeline executes in strict sequence:

1. **Ticketmaster event fetch** -- Multi-country, date-chunked, auto-paginated
2. **Artist name extraction** -- Build candidate list from fetched events
3. **MusicBrainz resolution** -- Resolve artist names to MBIDs, cache in GCS
4. **Artist quality filtering** -- Remove tribute acts, cover bands, venue-based entries, non-music types
5. **MBID enrichment** -- Attach resolved MBIDs back to Ticketmaster event rows
6. **Setlist.fm ingestion** -- Fetch historical setlists for resolved artists (skips already-ingested MBIDs)
7. **dlt load** -- Merge all resources into BigQuery `raw` tables

## Commands

Dry run (fetch + transform, no load):

```bash
make run-dlt-dry
```

Load into destination dataset (default: BigQuery `raw`):

```bash
make run-dlt
```

## Key design decisions

- **dlt merge semantics:** `write_disposition="merge"` with primary keys (`event_id`, `setlist_id`, `mbid`) ensures re-runs update existing rows instead of creating duplicates.
- **MusicBrainz cache:** Upsert semantics in GCS. Subsequent runs skip API calls for already-resolved artists.
- **Setlist.fm cutoff:** Only setlists from year 2000 onward. Older data is too sparse.
- **TM date range:** Dynamically set to today -> 1 year forward. Production uses 12 monthly chunks; prototype uses 2 semi-annual chunks. Monthly chunking keeps each window under the TM API's ~1,000 result limit.
- **TM auto-pagination:** In production mode (`TICKETMASTER_MAX_PAGES=0`), fetches all available pages per chunk. Prototype caps at 5.
- **TM event filtering:** Only genuine MusicBrainz artist types (Person, Group, Orchestra, Choir) are kept.
- **HTTP resilience:** Retry logic with exponential backoff for 429 and 5xx errors. Timestamped status logs for troubleshooting.
- **BigQuery location preflight:** Dataset location is validated before loading to avoid opaque location-mismatch errors.
- **Orchestration:** `PIPELINE_MODE` is a standard env var that any orchestrator (Kestra, Airflow, Bruin) can pass.

## Required environment variables

- `TICKETMASTER_API_KEY`
- `SETLISTFM_API_KEY`
- `SETLISTFM_USER_AGENT` (recommended)
- `PIPELINE_MODE` (`prototype` or `production`)
- `TRACKED_ARTISTS` (comma-separated, used in prototype mode)
- `MUSICBRAINZ_USER_AGENT` (recommended)
- `MUSICBRAINZ_API_KEY` (optional)
- `DLT_DESTINATION` (default `bigquery`)
- `DLT_DATASET` (default `raw`)

## Optional tuning

- `TICKETMASTER_MAX_PAGES` (default `5` prototype, `0` = unlimited production)
- `TICKETMASTER_PAGE_SIZE` (default `200`)
- `TICKETMASTER_COUNTRY_CODES` (default `US,CA,GB,DE,IT` in production, first country only in prototype)
- `SETLISTFM_MAX_PAGES` (default `80` production, `5` prototype)
- `TICKETMASTER_RESOLVE_ARTISTS_LIMIT` (default `0` (all) in production, `50` in prototype)
- `MAX_TOURING_ARTISTS` (default `100`)
- `DATA_LAKE_BUCKET` (GCS bucket for MusicBrainz cache)
- `HTTP_MAX_RETRIES` (default `3`)
- `HTTP_RETRY_BACKOFF_SECONDS` (default `1.5`)

## Limitations

- MusicBrainz query quality depends on name quality and API availability -- not all artists resolve
- Setlist.fm rate-limits at ~2 req/s; the pipeline retries with backoff but cannot guarantee completeness under heavy throttling
- Songs stored as semicolon-delimited string (dlt drops nested lists; parsing happens in dbt)
- Artist matching in prototype mode (tracked artists) may not overlap with live Ticketmaster artists

For the full project design rationale, see [docs/design_choices.md](../docs/design_choices.md).
