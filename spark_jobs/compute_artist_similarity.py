"""Spark job: compute pairwise artist venue similarity.

Reads fact_concert from BigQuery via the Spark BigQuery connector,
collects distinct venues per artist, and computes the Jaccard similarity
index between every pair of artists that share at least one venue.

Writes results directly back to BigQuery (spark_artist_similarity table).

Designed to run on Dataproc Serverless — no local temp files needed.
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _conf_or_env(spark: SparkSession | None, prop: str, env_key: str, default: str = "") -> str:
    """Read from Spark conf (Dataproc) with env var fallback (local)."""
    if spark:
        val = spark.conf.get(prop, "").strip()
        if val:
            return val
    return os.environ.get(env_key, default).strip()


def main() -> None:
    is_serverless = os.environ.get("DATAPROC_SERVERLESS") or False

    builder = (
        SparkSession.builder
        .appName("GigwiseArtistSimilarity")
    )

    # When running on Dataproc Serverless, master and BQ connector are
    # pre-configured. For local testing, set them explicitly.
    if not is_serverless:
        builder = (
            builder
            .master("local[*]")
            .config("spark.driver.memory", "2g")
            .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1")
        )

    spark = builder.getOrCreate()

    project_id = _conf_or_env(spark, "spark.gigwise.projectId", "GCP_PROJECT_ID")
    dataset = _conf_or_env(spark, "spark.gigwise.dataset", "BQ_DATASET_ANALYTICS", "analytics")
    gcs_bucket = _conf_or_env(spark, "spark.gigwise.gcsBucket", "DATA_LAKE_BUCKET")

    if not project_id:
        print("GCP_PROJECT_ID not set", file=sys.stderr)
        spark.stop()
        raise SystemExit(1)

    if gcs_bucket:
        spark.conf.set("temporaryGcsBucket", gcs_bucket)

    spark.sparkContext.setLogLevel("WARN")

    # Read fact_concert directly from BigQuery
    source_table = f"{project_id}.{dataset}.fact_concert"
    print(f"Reading from BigQuery: {source_table}", flush=True)
    concerts = spark.read.format("bigquery").option("table", source_table).load()

    # Use all concerts that have a venue name (both TM and Setlist.fm)
    concerts_with_venue = concerts.filter(
        F.col("venue_name").isNotNull() & (F.trim(F.col("venue_name")) != "")
    )

    dest_table = f"{project_id}.{dataset}.spark_artist_similarity"

    if concerts_with_venue.count() == 0:
        print("[WARN] No concert data with venues — writing empty result")
        empty = spark.createDataFrame(
            [],
            "artist_a STRING, artist_b STRING, shared_venues LONG, jaccard_similarity DOUBLE, shared_venues_detail STRING",
        )
        empty.write.format("bigquery").option("table", dest_table).option("writeMethod", "direct").mode("overwrite").save()
        spark.stop()
        return

    # Distinct (artist_name, venue_name) pairs
    venues = (
        concerts_with_venue
        .select("artist_name", F.trim(F.col("venue_name")).alias("venue_name"))
        .dropDuplicates(["artist_name", "venue_name"])
    )

    # Count unique venues per artist (for Jaccard denominator)
    artist_venue_counts = venues.groupBy("artist_name").agg(
        F.countDistinct("venue_name").alias("venue_count")
    )

    # Self-join on shared venues
    a = venues.alias("a")
    b = venues.alias("b")
    pairs = (
        a.join(b, (F.col("a.venue_name") == F.col("b.venue_name")) & (F.col("a.artist_name") < F.col("b.artist_name")))
        .select(
            F.col("a.artist_name").alias("artist_a"),
            F.col("b.artist_name").alias("artist_b"),
            F.col("a.venue_name").alias("venue_name"),
        )
        .dropDuplicates()
    )

    shared = pairs.groupBy("artist_a", "artist_b").agg(
        F.count("venue_name").alias("shared_venues"),
        F.concat_ws("; ", F.sort_array(F.collect_list("venue_name"))).alias("shared_venues_detail"),
    )

    # Jaccard = |A ∩ B| / |A ∪ B| = shared / (count_a + count_b - shared)
    result = (
        shared
        .join(artist_venue_counts.alias("ca"), F.col("artist_a") == F.col("ca.artist_name"))
        .join(artist_venue_counts.alias("cb"), F.col("artist_b") == F.col("cb.artist_name"))
        .withColumn(
            "jaccard_similarity",
            F.round(
                F.col("shared_venues") / (F.col("ca.venue_count") + F.col("cb.venue_count") - F.col("shared_venues")),
                4,
            ),
        )
        .select("artist_a", "artist_b", "shared_venues", "jaccard_similarity", "shared_venues_detail")
        .orderBy(F.col("jaccard_similarity").desc())
    )

    # Write directly to BigQuery
    print(f"Writing results to BigQuery: {dest_table}", flush=True)
    result.write.format("bigquery").option("table", dest_table).option("writeMethod", "direct").mode("overwrite").save()

    row_count = result.count()
    print(f"[OK] Wrote {row_count} artist-pair similarities to {dest_table}")
    spark.stop()


if __name__ == "__main__":
    main()
