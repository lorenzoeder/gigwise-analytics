"""@bruin
name: transformation.run_spark_enrichment
type: python
depends:
  - transformation.run_dbt_build
@bruin"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


def _status(msg: str) -> None:
    print(f"[spark-enrichment] {msg}", flush=True)


def main() -> None:
    mode = os.getenv("PIPELINE_MODE", "prototype").strip().lower()
    if mode != "production":
        _status("Skipping Spark enrichment (prototype mode)")
        return

    repo_root = Path(__file__).resolve().parents[3]
    spark_dir = repo_root / "spark_jobs"
    data_dir = spark_dir / "data"
    input_dir = data_dir / "input"
    output_dir = data_dir / "output"

    # Prepare directories
    if input_dir.exists():
        shutil.rmtree(input_dir)
    if output_dir.exists():
        shutil.rmtree(output_dir)
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    project_id = os.environ["GCP_PROJECT_ID"]
    dataset = os.getenv("BQ_DATASET_ANALYTICS", "analytics")

    # Step 1: Export fact_concert from BigQuery to local parquet
    _status("Exporting fact_concert from BigQuery to local parquet")
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    query = f"SELECT * FROM `{project_id}.{dataset}.fact_concert`"
    df = client.query(query).to_dataframe()
    parquet_path = input_dir / "fact_concert.parquet"
    df.to_parquet(parquet_path, index=False)
    _status(f"Exported {len(df)} rows to {parquet_path}")

    if len(df) == 0:
        _status("No data in fact_concert — skipping Spark job")
        return

    # Step 2: Run Spark job locally via PySpark
    _status("Running PySpark artist similarity computation")
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    env["JAVA_HOME"] = "/usr/local/sdkman/candidates/java/21.0.10-ms"

    spark_script = spark_dir / "compute_artist_similarity.py"
    result = subprocess.run(
        ["uv", "run", "python", str(spark_script)],
        cwd=str(repo_root),
        env=env,
        capture_output=True,
        text=True,
    )
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"Spark job failed with exit code {result.returncode}")

    # Step 3: Load result back into BigQuery
    output_parquet = output_dir / "artist_similarity.parquet"
    if not output_parquet.exists():
        # Spark writes partitioned output — look for part files
        part_files = list(output_dir.glob("*.parquet"))
        if not part_files:
            _status("No output parquet found — Spark may have written empty result")
            return

    _status("Loading Spark output into BigQuery")
    import pandas as pd

    result_df = pd.read_parquet(output_dir)
    if result_df.empty:
        _status("Empty similarity result — no overlapping repertoires found")
        return

    table_ref = f"{project_id}.{dataset}.spark_artist_similarity"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    load_job = client.load_table_from_dataframe(result_df, table_ref, job_config=job_config)
    load_job.result()
    _status(f"Loaded {len(result_df)} rows into {table_ref}")

    # Clean up temp data
    shutil.rmtree(data_dir, ignore_errors=True)
    _status("Spark enrichment complete")


if __name__ == "__main__":
    main()
