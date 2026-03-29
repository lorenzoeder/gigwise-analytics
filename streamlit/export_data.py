"""Export dashboard data from BigQuery to Parquet snapshots.

Usage:
    uv run python streamlit/export_data.py

Creates streamlit/data/*.parquet files that the dashboard can load
instead of querying BigQuery live. A metadata file records the export
date so the dashboard knows when to treat snapshots as stale.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

DATA_DIR = Path(__file__).resolve().parent / "data"


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Convert BigQuery-specific types to standard pandas types for Parquet."""
    for col in df.columns:
        dtype_name = str(df[col].dtype)
        if "dbdate" in dtype_name:
            df[col] = pd.to_datetime(df[col])
        elif "dbtimestamp" in dtype_name or "dbtime" in dtype_name:
            df[col] = pd.to_datetime(df[col], utc=True)
    return df


def main() -> None:
    project_id = os.getenv("GCP_PROJECT_ID", "").strip()
    dataset = os.getenv("BQ_DATASET_ANALYTICS", "analytics")
    streaming_dataset = os.getenv("BQ_DATASET_STREAMING", "streaming")

    if not project_id:
        print("GCP_PROJECT_ID not set", file=sys.stderr)
        raise SystemExit(1)

    client = bigquery.Client(project=project_id)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    queries = {
        "date_range": f"""
            SELECT MIN(event_date) AS min_date, MAX(event_date) AS max_date
            FROM `{project_id}.{dataset}.fact_concert`
            WHERE source = 'ticketmaster'
              AND event_date >= CURRENT_DATE()
        """,
        "touring_intensity": f"""
            SELECT artist_name, primary_genre, country, concert_count
            FROM `{project_id}.{dataset}.mart_artist_touring_intensity`
            ORDER BY concert_count DESC
            LIMIT 500
        """,
        "repertoire": f"""
            SELECT artist_name, primary_genre, concert_year, concert_count,
                   unique_songs, total_song_performances
            FROM `{project_id}.{dataset}.mart_artist_yearly_repertoire`
            WHERE artist_name IS NOT NULL
            ORDER BY artist_name, concert_year
            LIMIT 5000
        """,
        "freshness": f"""
            SELECT artist_name, primary_genre, concert_year, concert_count,
                   unique_songs, first_time_songs, repeated_songs, freshness_pct
            FROM `{project_id}.{dataset}.mart_artist_setlist_freshness`
            WHERE artist_name IS NOT NULL
            ORDER BY artist_name, concert_year
            LIMIT 5000
        """,
        "similarity": f"""
            SELECT artist_a, artist_b, shared_venues, jaccard_similarity
            FROM `{project_id}.{dataset}.spark_artist_similarity`
            ORDER BY jaccard_similarity DESC
            LIMIT 5000
        """,
        "live_events": f"""
            SELECT event_id, event_name, artist_name, event_date, event_status,
                   venue_name, city, country_code, observed_at, is_new
            FROM `{project_id}.{streaming_dataset}.live_event_updates`
            ORDER BY observed_at DESC
            LIMIT 500
        """,
    }

    for name, query in queries.items():
        path = DATA_DIR / f"{name}.parquet"
        try:
            df = client.query(query).to_dataframe()
            df = _normalize_df(df)
            df.to_parquet(path, index=False)
            print(f"  {name}: {len(df)} rows -> {path.name}")
        except Exception as exc:
            print(f"  {name}: skipped ({exc})")
            # Remove stale file if the table no longer exists
            path.unlink(missing_ok=True)

    meta = {"exported_date": date.today().isoformat()}
    (DATA_DIR / "meta.json").write_text(json.dumps(meta))
    print(f"Export complete: {meta['exported_date']}")


if __name__ == "__main__":
    main()
