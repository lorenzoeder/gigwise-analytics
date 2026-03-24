"""Standalone Spark runner: submit artist similarity job to Dataproc Serverless.

The PySpark job reads from BigQuery and writes back directly — no local temp files.

Requires:
- GCP_PROJECT_ID set in environment
- DATA_LAKE_BUCKET set in environment (GCS bucket for Spark staging)
- fact_concert table to exist in BigQuery (run the batch pipeline first)
- gcloud CLI authenticated and Dataproc API enabled
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def main() -> None:
    project_id = os.environ.get("GCP_PROJECT_ID", "").strip()
    if not project_id:
        print("GCP_PROJECT_ID not set", file=sys.stderr)
        raise SystemExit(1)

    region = os.environ.get("GCP_REGION", "europe-west2")
    gcs_bucket = os.environ.get("DATA_LAKE_BUCKET", "").strip()
    dataset = os.getenv("BQ_DATASET_ANALYTICS", "analytics")

    if not gcs_bucket:
        print("DATA_LAKE_BUCKET not set", file=sys.stderr)
        raise SystemExit(1)

    spark_script = Path(__file__).resolve().parent / "compute_artist_similarity.py"
    gcs_script = f"gs://{gcs_bucket}/spark_jobs/compute_artist_similarity.py"

    # Upload PySpark script to GCS
    print("=== Uploading Spark job to GCS ===", flush=True)
    result = subprocess.run(
        ["gcloud", "storage", "cp", str(spark_script), gcs_script],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        raise SystemExit(1)

    # Submit Dataproc Serverless batch job
    print("=== Submitting Dataproc Serverless batch job ===", flush=True)
    env_vars = {
        "GCP_PROJECT_ID": project_id,
        "BQ_DATASET_ANALYTICS": dataset,
        "DATA_LAKE_BUCKET": gcs_bucket,
        "DATAPROC_SERVERLESS": "1",
    }

    cmd = [
        "gcloud", "dataproc", "batches", "submit", "pyspark",
        gcs_script,
        f"--project={project_id}",
        f"--region={region}",
        f"--deps-bucket=gs://{gcs_bucket}",
        "--properties=spark.executor.instances=2",
        "--properties=spark.dynamicAllocation.maxExecutors=2",
        f"--properties=spark.hadoop.fs.gs.project.id={project_id}",
    ]

    for key, val in env_vars.items():
        cmd.append(f"--properties=spark.executorEnv.{key}={val}")
        cmd.append(f"--properties=spark.driverEnv.{key}={val}")

    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise SystemExit(result.returncode)

    print("=== Spark job complete (Dataproc Serverless) ===", flush=True)


if __name__ == "__main__":
    main()
