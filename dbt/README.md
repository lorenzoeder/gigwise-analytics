# dbt Transformation Layer

dbt models transform raw API data from BigQuery `raw` into a star schema in `analytics`, powering the Streamlit dashboard and Parquet exports.

## Layer overview

| Layer | Materialization | Purpose |
|---|---|---|
| **staging** | view | Normalize raw API data into typed, stable columns |
| **intermediate** | view | Unify events from multiple sources into a single spine |
| **core** | incremental | Establish reusable semantic entities (facts and dimensions) |
| **marts** | view | Shape curated outputs for the dashboard |

## Sources

Defined in `models/sources.yml`:

- `raw.ticketmaster_events` — upcoming concert events
- `raw.setlistfm_setlists` — historical setlists with song detail
- `raw.musicbrainz_artists` — canonical artist metadata and genre tags

Source freshness is configured to warn after 3 days and error after 7 days, using `extracted_at` as the loaded-at timestamp.

## Model reference

### Staging

**`stg_ticketmaster__events`** — Normalizes Ticketmaster events. Left-joins `musicbrainz_artists` on normalized name to attach MBID when not already enriched at ingestion. Uses `QUALIFY ROW_NUMBER()` to enforce one row per event and avoid fanout from ambiguous name matches. Excludes cancelled/postponed events.

**`stg_setlistfm__setlists`** — Parses setlist records with safe date handling (`COALESCE(SAFE.PARSE_DATE('%d-%m-%Y', ...), SAFE_CAST(...))`) for mixed formats. Filters to year 2000 onward. Exposes `country`, `city`, `venue_name` for geographic analysis.

### Intermediate

**`int_concerts_unified`** — UNION ALL of Ticketmaster (future events) and Setlist.fm (historical events). Treats both as complementary event sources. A LEFT JOIN approach was rejected because future Ticketmaster events have no matching historical setlists — the join would yield near-zero matches. A `source` column tracks data provenance.

### Core (star schema)

**`dim_artist`** — One row per artist, keyed on MBID. Built from MusicBrainz data with a Ticketmaster fallback for artists not yet in MusicBrainz. Genre sourced from MusicBrainz crowd-sourced tags (highest-voted tag as `primary_genre`). Incremental with merge on `artist_id`.

**`fact_concert`** — Central event fact table. Carries `artist_name`, `city`, `venue_name`, `source` (ticketmaster/setlistfm), and optional setlist fields. Joined to `dim_artist` via MBID for `artist_id` linkage. Incremental with merge on `concert_id`. Partitioned by `event_date` (monthly) and clustered by `artist_id`.

### Marts

**`mart_artist_touring_intensity`** — Concert count by artist, genre, and country. Filtered to Ticketmaster source and `event_date >= CURRENT_DATE()`. Powers the Touring Intensity and Genre Intensity dashboard charts.

**`mart_artist_yearly_repertoire`** — Unique songs per artist per year. Uses `CROSS JOIN UNNEST(SPLIT(songs_played, '; '))` to explode the semicolon-delimited song list. Filtered to Setlist.fm source only.

**`mart_artist_setlist_freshness`** — Percentage of first-time songs per artist per year. High freshness = evolving repertoire; low freshness = predictable setlist.

### Exposure

**`streamlit_dashboard`** — Declared in `models/_exposures.yml`. Formalizes lineage from `fact_concert` and all three marts to the Streamlit dashboard at [gigwise.streamlit.app](https://gigwise.streamlit.app).

## Tests

### Schema tests (`models/core/schema.yml`, `models/staging/schema.yml`)

- `not_null` + `unique` on `dim_artist.artist_id`, `dim_artist.mbid`, `fact_concert.concert_id`, staging keys
- `not_null` on `dim_artist.artist_name`, `fact_concert.event_date`, staging dates and names
- `accepted_values` on `fact_concert.source` (ticketmaster, setlistfm)
- `relationships`: `fact_concert.artist_id` → `dim_artist.artist_id` (with null filter)

### Singular tests (`tests/`)

- **`assert_concert_date_not_future`** — Flags events more than 3 years in the future (catches parse errors, not valid future concerts)
- **`assert_valid_artist_types_only`** — Ensures no artists with invalid MusicBrainz types (e.g. festivals, brands) slip through ingestion filters

## Known limitations

- ~31% of artists lack MusicBrainz genre tags
- `artist_id` is always MBID — when MBID cannot be resolved, `artist_id` is null
- UNION ALL could theoretically produce duplicate concerts if both sources cover the same event (unlikely given Ticketmaster is forward-looking and Setlist.fm is historical)

## Commands

```bash
make run-dbt                    # Incremental build (deps + build)
make run-dbt DBT_BUILD_FLAGS='--full-refresh --fail-fast'  # Full refresh
make run-dbt DBT_BUILD_FLAGS='--select mart_artist_touring_intensity'  # Build specific model
make generate-dbt-docs          # Regenerate docs site into dbt/docs/
```

## Generated docs

The `docs/` subdirectory contains a self-contained dbt docs site (`index.html` + `manifest.json` + `catalog.json`). Open `dbt/docs/index.html` in a browser to browse model lineage, column descriptions, and test coverage. Regenerate with `make generate-dbt-docs` after model changes.
