# dlt Unified Ingestion

This folder contains the primary ingestion pipeline for all external APIs:

- Ticketmaster events
- Setlist.fm setlists
- MusicBrainz artist metadata
- Spotify artist popularity snapshots

Bruin remains in the project as an orchestration layer and for SQL/quality assets, while API ingestion ownership is now fully in dlt.

## Commands

Dry run (fetch + transform, no load):

```bash
make run-dlt-snapshots-dry
```

Load into destination dataset (default: BigQuery `raw`):

```bash
make run-dlt-snapshots
```

## Required environment variables

- `SPOTIFY_CLIENT_ID`
- `SPOTIFY_CLIENT_SECRET`
- `TICKETMASTER_API_KEY`
- `SETLISTFM_API_KEY`
- `SETLISTFM_USER_AGENT` (recommended)
- `ARTIST_SELECTION_MODE` (`tracked` or `ticketmaster_live`)
- `TRACKED_ARTISTS` (comma-separated, used when mode is `tracked`)
- `MUSICBRAINZ_USER_AGENT` (recommended)
- `MUSICBRAINZ_API_KEY` (optional)
- `DLT_DESTINATION` (default `bigquery`)
- `DLT_DATASET` (default `raw`)

## Artist scope modes

- `tracked` (dev/test): use a small static list from `TRACKED_ARTISTS`
- `ticketmaster_live` (production-style): derive artists dynamically from Ticketmaster events API

Optional tuning for `ticketmaster_live`:

- `MAX_TOURING_ARTISTS` (default `100`)
- `TICKETMASTER_MAX_PAGES` (default `3`)
- `TICKETMASTER_PAGE_SIZE` (default `200`)
- `TICKETMASTER_COUNTRY_CODE` (default `GB`)
- `SETLISTFM_MAX_PAGES` (default `1`)

HTTP resilience knobs:

- `HTTP_MAX_RETRIES` (default `3`)
- `HTTP_RETRY_BACKOFF_SECONDS` (default `1.5`)

## Write behavior

- `ticketmaster_events`: merge on `event_id`
- `setlistfm_setlists`: merge on `setlist_id`
- `musicbrainz_artists`: merge on `artist_name + extracted_at`
- `spotify_artists`: merge on `artist_name + snapshot_date`

This makes ingestion idempotent: rerunning the same extraction (direct dlt run or via Bruin orchestration) will not create duplicate rows for the same source identifiers.
