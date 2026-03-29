# GigWise Analytics — Design Choices

A detailed walkthrough of every design decision in the project: what each component does, why it was chosen, and known limitations.

---

## Architecture Overview

```
Kestra (daily scheduler)
  └── Bruin (inner orchestration DAG)
        ├── dlt ingestion (APIs → BigQuery raw)
        ├── SQL staging view
        ├── Quality checks
        ├── dbt build (raw → analytics)
        └── Spark enrichment (production only)

Kafka/Redpanda (streaming add-on)
  ├── Producer → polls Ticketmaster API every 5 min
  └── Consumer → writes to BigQuery streaming dataset

Streamlit dashboard → reads from BigQuery analytics + streaming
```

The pipeline follows a layered design: each component has a single responsibility, failures are isolated, and steps can be re-run independently.

---

## Why a Makefile?

All pipeline operations are wrapped in `Makefile` targets (`make run-bruin`, `make run-dlt`, `make run-dashboard`, etc.). This was a deliberate choice:

- **Single entry point:** Every command chain is discoverable via `make help`. No need to remember long CLI invocations, environment-sourcing steps, or directory changes.
- **Environment handling:** Each target automatically sources `.env` before execution, eliminating a common class of "variable not set" errors.
- **Composability:** Targets can depend on each other (e.g., `run-dbt` first runs `run-dbt-debug` to validate the connection).
- **Safety guards:** Destructive operations like `wipe-all` require `CONFIRM_WIPE=1`, preventing accidental data loss.
- **Portability:** Make is available on virtually every Unix system. No additional tooling required.

---

## Data Sources

### Ticketmaster Discovery API

- **What it provides:** Upcoming concert events across US, CA, GB, DE, IT markets (1-year forward window)
- **Why Ticketmaster:** Largest global ticketing platform with a well-documented, **free API** for upcoming event data
- **Key decisions:**
  - Events are fetched with monthly date chunking in production (12 chunks), keeping each window under the API's ~1,000-result limit
  - Auto-pagination in production mode (`TICKETMASTER_MAX_PAGES=0`) ensures complete event capture per market
  - Cancelled/postponed events are excluded
  - Events without an artist `attraction` are skipped
  - Artists with fewer than 3 upcoming events are filtered (removes one-off, cover band, and tribute act listings)
  - Only genuine MusicBrainz artist types (Person, Group, Orchestra, Choir) are kept — filtering out venue-based entries and event styles

### Setlist.fm API

- **What it provides:** Historical setlists with song-level detail (only recent data included due to free API constraints)
- **Why Setlist.fm:** The only public, **free API** with structured setlist data (songs played at each concert)
- **Key decisions:**
  - Uses MBID-based endpoint (`artist/{mbid}/setlists`) for exact matching when MBID (MusicBrainz ID) is available
  - Falls back to name search with strict matching for unresolved artists
  - Year 2000 cutoff applied at both ingestion and dbt staging (older data is too sparse to be reliable)
  - Songs stored as semicolon-delimited string (dlt drops nested lists; parsing happens in dbt)
  - Already-ingested artists are skipped on re-runs (idempotency check against BigQuery)

### MusicBrainz API

- **What it provides:** Canonical artist metadata — MBID, origin country, formation year, artist type, and genre via crowd-sourced tags
- **Why MusicBrainz:** **Open-source**, community-maintained canonical artist database.
- **Key decisions:**
  - Artist resolutions are cached in GCS (`gs://<bucket>/cache/mb_artist_cache.json`). Subsequent runs skip API calls for already-resolved artists
  - The highest-voted MusicBrainz tag is used as `primary_genre`
  - 0.5s delay between resolutions to respect MusicBrainz rate limits
  - ~69% genre coverage — depends on community tagging quality

### Canonical Key Strategy

**MusicBrainz ID (MBID) is the cross-source join key.** Artist names are inconsistent across platforms (spacing, punctuation, aliases, casing). MBID provides a stable canonical identifier. When MBID is unavailable, the system falls back to name normalization, but this is treated as enrichment rather than guaranteed truth.

---

## Ingestion Layer (dlt)

### Why dlt?

dlt is a lightweight Python framework for API-to-warehouse data loading. It was chosen for:
- **Merge semantics:** `write_disposition="merge"` with primary keys (`event_id`, `setlist_id`, `mbid`) ensures re-runs update existing rows instead of creating duplicates — true idempotency
- **Schema inference:** Automatically handles JSON-to-table mapping
- **BigQuery native:** Built-in BigQuery destination with location awareness

### Pipeline Modes

| Mode | Scope | Runtime | Purpose |
|---|---|---|---|
| `prototype` | 3 tracked artists, 1 country, 5 pages | ~5 min | Development and testing |
| `production` | All TM artists, 5 countries, 80 setlist pages, auto-pagination | ~1 hr | Full analytics dataset |

Controlled by `PIPELINE_MODE` environment variable, which any orchestrator can override.

### Ingestion Phases

The pipeline executes in strict sequence:

1. **Ticketmaster event fetch** — Multi-country, date-chunked, auto-paginated
2. **Artist name extraction** — Build candidate list from fetched events
3. **MusicBrainz resolution** — Resolve artist names to MBIDs, cache in GCS
4. **Artist quality filtering** — Remove tribute acts, cover bands, venue-based entries, non-music types
5. **MBID enrichment** — Attach resolved MBIDs back to Ticketmaster event rows
6. **Setlist.fm ingestion** — Fetch historical setlists for resolved artists (skips already-ingested MBIDs)
7. **dlt load** — Merge all resources into BigQuery `raw` tables

### HTTP Resilience

All API calls use retry logic with exponential backoff for 429 (rate limit) and 5xx errors. Configurable via `HTTP_MAX_RETRIES` and `HTTP_RETRY_BACKOFF_SECONDS`.

### Limitations

- MusicBrainz query quality depends on name quality and API response — not all artists resolve
- Setlist.fm rate-limits at ~2 req/s; the pipeline retries with backoff but cannot guarantee completeness under heavy throttling
- Artist matching in prototype mode (tracked artists) may not overlap with live Ticketmaster artists

---

## Orchestration: Bruin + Kestra

### Why two layers?

- **Bruin** is the *inner orchestrator*. It manages the DAG (directed acyclic graph) of pipeline steps with explicit dependency ordering: ingestion → staging SQL → quality checks → dbt build → Spark enrichment. Bruin ensures each step only runs after its dependencies succeed.
- **Kestra** is the *outer scheduler*. It triggers `make run-bruin` on a daily cron (06:00 UTC) and provides a web UI for monitoring and manual triggers.

This separation keeps scheduling concerns (when to run) separate from execution concerns (what to run and in what order).

### Bruin DAG Assets

| Asset | Type | Purpose |
|---|---|---|
| `run_dlt_ingestion` | Python | Execute the dlt pipeline |
| `stg_concerts_union` | BigQuery SQL view | Staging checkpoint joining raw tables |
| `check_event_dates` | BigQuery SQL | Quality check for suspicious future dates |
| `run_dbt_build` | Python | Run `dbt deps && dbt build` |
| `run_spark_enrichment` | Python | Submit PySpark job (production only) |

### Kestra Flow

Defined in `kestra/flows/concert_pipeline_daily.yml`. Accepts a `pipeline_mode` input (defaults to `production`). Runs the full Bruin pipeline inside a Docker container.

---

## Transformation Layer (dbt)

### Layer Design

| Layer | Materialization | Purpose |
|---|---|---|
| **staging** | view | Normalize raw API data into typed, stable columns |
| **intermediate** | view | Unify events from multiple sources into a single spine |
| **core** | incremental | Establish reusable semantic entities (facts and dimensions) |
| **marts** | view | Shape curated outputs for the dashboard |

### Staging Models

**`stg_ticketmaster__events`** — Normalizes Ticketmaster events. Left-joins `musicbrainz_artists` on normalized name to attach MBID when not already enriched at ingestion. Uses `QUALIFY ROW_NUMBER()` to enforce one row per event and avoid fanout from ambiguous name matches.

**`stg_setlistfm__setlists`** — Parses setlist records with safe date parsing (`COALESCE(SAFE.PARSE_DATE('%d-%m-%Y', ...), SAFE_CAST(...))` handles mixed formats). Exposes `country_code`, `city`, `venue_name` for geographic analysis.

### Intermediate Model

**`int_concerts_unified`** — UNION ALL of Ticketmaster (future events) and Setlist.fm (historical events). Treats both as complementary event sources rather than using Setlist.fm as optional enrichment. A LEFT JOIN approach was rejected because future Ticketmaster events have no matching historical setlists — the join would yield near-zero matches.

### Core Models (star schema)

**`fact_concert`** — Central event fact table. Carries `artist_name`, `city`, `venue_name`, `source` (ticketmaster/setlistfm), and optional setlist fields. Joined to `dim_artist` via MBID for `artist_id` linkage.

**`dim_artist`** — One row per artist, keyed on MBID. Genre sourced from MusicBrainz tags (highest-voted tag as `primary_genre`).

### Mart Models

**`mart_artist_touring_intensity`** — Concert count by artist, genre, and country. Filtered to Ticketmaster source only (upcoming concerts) and `event_date >= CURRENT_DATE()` to exclude past events. Powers the Touring Intensity and Genre Intensity charts.

**`mart_artist_yearly_repertoire`** — Unique songs per artist per year. Uses `CROSS JOIN UNNEST(SPLIT(songs_played, '; '))` to explode the semicolon-delimited song list. Filtered to Setlist.fm source (only historical data has song details).

**`mart_artist_setlist_freshness`** — Percentage of first-time songs per artist per year. High freshness = evolving repertoire; low freshness = predictable setlist.

### dbt Tests

- Schema tests: `not_null` and `unique` on `dim_artist.artist_id`, `dim_artist.mbid`, `fact_concert.concert_id`, `fact_concert.event_date`
- `accepted_values` on `fact_concert.source`
- Referential integrity: `fact_concert.artist_id` → `dim_artist.artist_id`
- Singular test: events not more than 3 years in the future (catches parse errors, not valid future concerts)

### Limitations

- ~31% of artists lack MusicBrainz genre tags
- `artist_id` is always MBID — when MBID cannot be resolved, `artist_id` is null
- UNION ALL could theoretically produce duplicate concerts if both sources cover the same event (unlikely given different temporal coverage)

---

## Batch Processing (PySpark)

### What it computes

PySpark calculates **venue-based Jaccard similarity** between artists: for any two artists, similarity = (shared venues) ÷ (combined unique venues). This identifies which artists play at the same venues, suggesting they draw similar audiences.

### Why Spark?

The computation is a self-join on all `(artist_name, venue_name)` pairs from `fact_concert` — a quadratic operation that benefits from distributed processing. Runs on Google Cloud Dataproc Serverless, so no cluster management is needed.

### Output

Table `analytics.spark_artist_similarity` with columns: `artist_a`, `artist_b`, `shared_venues`, `jaccard_similarity`, `shared_venues_detail` (list of shared venue names).

### Limitation

Runs as a production-only enrichment step. Requires Dataproc API enabled and appropriate IAM permissions.

---

## Streaming (Kafka/Redpanda)

### Architecture

- **Producer** (`kafka/producer.py`): Polls Ticketmaster Discovery API every 5 minutes for upcoming music events. Publishes each event as a JSON message to the `ticket_events` Kafka topic.
- **Consumer** (`kafka/consumer.py`): Reads from `ticket_events`, batches messages (50 at a time), and writes to BigQuery `streaming.live_event_updates` table.
- **Broker**: Redpanda (Kafka-compatible), runs via Docker Compose.

### Why Redpanda instead of Apache Kafka?

Redpanda is a drop-in Kafka-compatible broker that requires far less memory and configuration. A single container with 1 GB RAM is sufficient for this use case.

### Design Decisions

- Consumer uses `auto_offset_reset="earliest"` to never miss messages, even if it restarts after the producer has already published
- Each process sources `.env` independently via `bash -c` wrapper in the Makefile, ensuring environment variables survive `nohup` backgrounding
- Streaming is fully toggleable — `make run-streaming` / `make stop-streaming` — and does not affect the core batch pipeline

### Limitation

Streaming is an add-on demonstration layer. The data it writes is not integrated into the dbt model lineage — it goes directly to a separate BigQuery dataset (`streaming`).

---

## Dashboard (Streamlit)

### Dual-Mode Data Loading

The dashboard supports two modes via `DASHBOARD_MODE`:

| Mode | Behaviour |
|---|---|
| `local` (default) | Uses Parquet snapshots if fresh (exported today), otherwise queries BigQuery live |
| `cloud` | Parquet snapshots only — no BigQuery dependency (for Streamlit Cloud deployment) |

Parquet snapshots are generated with `make export-dashboard-data`, which exports all 6 dashboard queries to `streamlit/data/*.parquet` plus a `meta.json` with the export date.

### Dashboard Sections

1. **Artist Touring Intensity** (Tile 1) — Bar chart of top 15 artists by upcoming concert count, broken down by country. Only concerts from today onward are included.
2. **Genre Intensity** (Tile 1) — Top 10 genres by upcoming concert count.
3. **Live Event Stream** (Tile 1, optional) — Real-time metrics, event status breakdown, and recent updates from Kafka streaming. Shows LIVE/OFFLINE indicator.
4. **Setlist Evolution** (Tile 2) — Bar chart of unique songs per year for a selected artist, with a Freshness Index line overlay.
5. **Similar Artists** (Tile 2, optional) — Horizontal bar chart of top 10 similar artists by Jaccard score, based on shared venues.

---

## Infrastructure (Terraform)

### What is provisioned

| Resource | Purpose |
|---|---|
| GCS bucket | Data lake — stores MusicBrainz artist cache and Spark temp files |
| BigQuery datasets (`raw`, `staging`, `analytics`, `streaming`) | Logical separation of pipeline stages |
| Service account (optional) | Dedicated identity for pipeline execution |
| Dataproc API | Enabled for Spark batch jobs |

### Key Terraform decisions

- `force_destroy` defaults to `false` (safety against accidental bucket deletion)
- GCS versioning is enabled
- Lifecycle rules auto-delete archived versions after 7 days and Spark temp files after 1 day
- Service account creation is conditional — supports reusing an existing SA via `EXISTING_PIPELINE_SA_EMAIL`

---

## Data Quality Controls

### At Ingestion

- Ticketmaster events without an artist attraction are skipped
- Artists with < 3 upcoming events are filtered (removes cover/tribute acts)
- Attraction types "Event Style" and "Venue Based" are excluded
- Only genuine MusicBrainz artist types (Person, Group, Orchestra, Choir) are kept
- Setlist.fm strict name matching prevents cover/tribute band pollution
- Artist name normalization handles `&` → `and`, Unicode characters, and other inconsistencies

### At Transformation

- dbt staging models use safe date parsing with `COALESCE` fallbacks
- `QUALIFY ROW_NUMBER()` prevents duplicate events
- Singular tests catch events > 3 years in future (parse errors)
- Schema tests enforce not-null on all key columns

### At Orchestration

- Bruin DAG dependencies ensure dbt only runs after quality checks pass
- Quality SQL asset checks for suspicious date ranges

---

## Known Limitations

| Area | Limitation |
|---|---|
| Ticketmaster GB pricing | Does not expose `priceRanges` via Discovery API — prices are out of scope |
| MusicBrainz genre coverage | ~69% of artists have genre tags. Depends on community tagging |
| Setlist.fm rate limits | API returns 429 on rapid requests. Pipeline retries with backoff but cannot guarantee completeness |
| Setlist.fm historical cutoff | Only year 2000 onward — older data is too sparse |
| Artist identity | MBID resolution is best-effort. ~31% of dim_artist rows may lack genre. Name-based fallback is probabilistic |
| Streaming scope | Kafka data lands in a separate dataset, not integrated into dbt models |
