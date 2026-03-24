# dlt Unified Ingestion

This folder contains the primary ingestion pipeline for all external APIs:

- Ticketmaster events (upcoming concerts across US, CA, GB, DE, IT)
- Setlist.fm setlists (historical setlists from year 2000 onward)
- MusicBrainz artist metadata (with persistent GCS-backed cache)

Bruin orchestrates this pipeline as part of its DAG (ingestion → staging SQL → quality checks). Kestra schedules Bruin runs on a daily cron.

## Pipeline Modes

The pipeline supports two modes, controlled by `PIPELINE_MODE`:

| Setting | Behaviour | Runtime target |
|---|---|---|
| `prototype` (default) | Uses `TRACKED_ARTISTS`, 1 TM country, 2 date chunks, 5 setlist pages/artist | < 5 min |
| `production` | All 5 markets (US,CA,GB,DE,IT), 4 quarterly date chunks, 80 setlist pages, artists derived from TM events | < 1 hr |

Switch mode by setting `PIPELINE_MODE=production` in your `.env`.

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

- **MusicBrainz cache**: Artist resolutions are cached in GCS (`gs://<DATA_LAKE_BUCKET>/cache/mb_artist_cache.json`). Subsequent runs skip API calls for already-resolved artists. The cache uses upsert semantics — no duplicates.
- **Setlist.fm cutoff**: Only setlists from year 2000 onward are ingested. Older data is filtered in both the pipeline and the dbt staging model.
- **TM date range**: Dynamically set to today → 1 year forward (not hardcoded). The year is chunked into quarterly windows to overcome TM's ~1000 result limit per query.
- **TM event filtering**: Cancelled/postponed events are excluded. Events without a MusicBrainz artist match are dropped. Additionally, only genuine music artist types (Person, Group, Orchestra, Choir) are kept — this filters out non-artist entries like charity events.
- **dlt merge**: All three resources use `write_disposition="merge"` with primary keys (`event_id`, `setlist_id`, `mbid`). Re-running the pipeline does NOT create duplicate rows.
- **Orchestration**: `PIPELINE_MODE` is a standard env var that any orchestrator (Kestra, Airflow, Bruin) can pass. See `kestra/flows/concert_pipeline_daily.yml` for an example.

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

- `TICKETMASTER_MAX_PAGES` (default `5`)
- `TICKETMASTER_PAGE_SIZE` (default `200`)
- `TICKETMASTER_COUNTRY_CODES` (default `US,CA,GB,DE,IT` in production, first country only in prototype)
- `SETLISTFM_MAX_PAGES` (default `80` production, `5` prototype)
- `TICKETMASTER_RESOLVE_ARTISTS_LIMIT` (default `0` (all) in production, `50` in prototype)
- `MAX_TOURING_ARTISTS` (default `100`)
- `DATA_LAKE_BUCKET` (GCS bucket for MusicBrainz cache)
- `HTTP_MAX_RETRIES` (default `3`)
- `HTTP_RETRY_BACKOFF_SECONDS` (default `1.5`)
