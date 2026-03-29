# Bruin Orchestration and Quality Layer

Bruin is used as the inner orchestrator for the GigWise Analytics pipeline. It manages a five-asset DAG with explicit dependency ordering, ensuring each step only runs after its dependencies succeed.

## Pipeline definition

File: `pipeline.yml`

- **Name:** `gigwise_analytics`
- **Schedule:** `@daily`
- **Connection:** BigQuery (default)

Bruin runs manually via Makefile in development. In production, Kestra triggers `make run-bruin` on a daily cron.

## DAG assets

### Asset 1: `ingestion.run_dlt_ingestion` (Python)

**File:** `assets/ingestion/run_dlt_ingestion.py`

Executes the dlt ingestion pipeline (Ticketmaster, Setlist.fm, MusicBrainz) from within the Bruin asset flow. Keeps orchestration self-contained so downstream SQL assets can depend on ingestion completion explicitly.

### Asset 2: `staging.stg_concerts_union` (BigQuery SQL view)

**File:** `assets/staging/stg_concerts_union.sql`

Creates a lightweight staging view in BigQuery joining raw Ticketmaster and MusicBrainz data. Provides an orchestration-visible checkpoint independent of dbt.

### Asset 3: `quality.check_event_dates` (BigQuery SQL)

**File:** `assets/quality/check_event_dates.sql`

Counts event rows with future dates in raw Ticketmaster data. This is a quality gate — dbt only runs after this check passes.

**Note:** For concert data, future events are expected and desirable. The check's primary value is as a DAG dependency gate rather than a strict validation. A threshold-based check (e.g., events > 3 years in future) would be more semantically accurate.

### Asset 4: `transformation.run_dbt_build` (Python)

**File:** `assets/transformation/run_dbt_build.py`

Runs `dbt deps && dbt build` as the final transformation step. Depends on `quality.check_event_dates`, ensuring dbt only runs after quality checks pass. Includes a post-run ownership fix (`chmod -R a+rwX dbt_packages/`) when running as root inside Docker, preventing permission conflicts between Kestra Docker runs and local development.

### Asset 5: `transformation.run_spark_enrichment` (Python)

**File:** `assets/transformation/run_spark_enrichment.py`

Submits the PySpark venue-similarity job to Dataproc Serverless. Production-only — skipped in prototype mode.

## Execution order

Bruin `depends` clauses enforce this sequence:

1. `run_dlt_ingestion`
2. `stg_concerts_union`
3. `check_event_dates`
4. `run_dbt_build`
5. `run_spark_enrichment`

## Relationship with dbt

Bruin orchestrates dbt as one step in a larger DAG. dbt remains the semantic modeling engine. Bruin manages the full pipeline: ingestion → staging → quality → dbt build → Spark enrichment.

dbt is also available standalone via `make run-dbt` for ad-hoc reruns without running the full DAG.

## Commands

```bash
make run-bruin          # Execute full Bruin pipeline
make run-bruin-dry      # Validate pipeline configuration (no execution)
```
