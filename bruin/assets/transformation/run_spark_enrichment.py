"""@bruin
name: transformation.run_spark_enrichment
type: python
depends:
  - transformation.run_dbt_build
@bruin"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _status(msg: str) -> None:
    print(f"[spark-enrichment] {msg}", flush=True)


def main() -> None:
    project_id = os.environ["GCP_PROJECT_ID"]
    region = os.environ.get("GCP_REGION", "europe-west2")
    gcs_bucket = os.environ.get("DATA_LAKE_BUCKET", "")
    dataset = os.getenv("BQ_DATASET_ANALYTICS", "analytics")

    repo_root = Path(__file__).resolve().parents[3]
    spark_script = repo_root / "spark_jobs" / "compute_artist_similarity.py"
    gcs_script = f"gs://{gcs_bucket}/spark_jobs/compute_artist_similarity.py"

    # Upload PySpark script to GCS
    _status("Uploading Spark job to GCS")
    result = subprocess.run(
        ["gcloud", "storage", "cp", str(spark_script), gcs_script],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        raise RuntimeError("Failed to upload Spark script to GCS")

    # Submit Dataproc Serverless batch job
    _status("Submitting Dataproc Serverless batch job")
    props = ",".join([
        "spark.executor.instances=2",
        "spark.dynamicAllocation.maxExecutors=2",
        f"spark.hadoop.fs.gs.project.id={project_id}",
        f"spark.gigwise.projectId={project_id}",
        f"spark.gigwise.dataset={dataset}",
        f"spark.gigwise.gcsBucket={gcs_bucket}",
    ])
    cmd = [
        "gcloud", "dataproc", "batches", "submit", "pyspark",
        gcs_script,
        f"--project={project_id}",
        f"--region={region}",
        f"--deps-bucket=gs://{gcs_bucket}",
        f"--properties={props}",
    ]

    env = dict(os.environ)

    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"Dataproc batch job failed with exit code {result.returncode}")

    _status("Spark enrichment complete (Dataproc Serverless)")


if __name__ == "__main__":
    main()
