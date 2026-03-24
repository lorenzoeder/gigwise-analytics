"""Standalone Spark runner: export from BQ → run PySpark → load results back.

Works with any pipeline mode (prototype or production). Requires:
- GCP_PROJECT_ID set in environment
- fact_concert table to exist in BigQuery (run the batch pipeline first)
- Java 21 on JAVA_HOME (set automatically by `make run-spark`)
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


def main() -> None:
    project_id = os.environ.get("GCP_PROJECT_ID", "").strip()
    if not project_id:
        print("GCP_PROJECT_ID not set", file=sys.stderr)
        raise SystemExit(1)

    dataset = os.getenv("BQ_DATASET_ANALYTICS", "analytics")
    repo_root = Path(__file__).resolve().parent.parent
    spark_dir = repo_root / "spark_jobs"
    data_dir = spark_dir / "data"
    input_dir = data_dir / "input"
    output_dir = data_dir / "output"

    # Clean slate
    if data_dir.exists():
        shutil.rmtree(data_dir)
    input_dir.mkdir(parents=True)
    output_dir.mkdir(parents=True)

    # Step 1: Export fact_concert from BigQuery
    print("=== Exporting fact_concert from BigQuery ===", flush=True)
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    df = client.query(
        f"SELECT * FROM `{project_id}.{dataset}.fact_concert`"
    ).to_dataframe()
    df.to_parquet(input_dir / "fact_concert.parquet", index=False)
    print(f"Exported {len(df)} rows", flush=True)

    if len(df) == 0:
        print("No data in fact_concert — nothing to compute", flush=True)
        shutil.rmtree(data_dir, ignore_errors=True)
        return

    # Step 2: Run PySpark
    print("=== Running PySpark artist similarity ===", flush=True)
    result = subprocess.run(
        ["uv", "run", "python", str(spark_dir / "compute_artist_similarity.py")],
        cwd=str(repo_root),
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    if result.returncode != 0:
        raise SystemExit(result.returncode)

    # Step 3: Load results back to BigQuery
    print("=== Loading results to BigQuery ===", flush=True)
    import pandas as pd

    result_df = pd.read_parquet(output_dir)
    print(f"{len(result_df)} similarity pairs computed", flush=True)

    if not result_df.empty:
        table_ref = f"{project_id}.{dataset}.spark_artist_similarity"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(
            result_df, table_ref, job_config=job_config
        ).result()
        print(f"Loaded to {table_ref}", flush=True)

    # Cleanup
    shutil.rmtree(data_dir, ignore_errors=True)
    print("Cleaned up temp files", flush=True)


if __name__ == "__main__":
    main()
