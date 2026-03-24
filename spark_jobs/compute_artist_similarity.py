"""Spark job: compute pairwise artist repertoire similarity.

Reads fact_concert parquet (exported from BigQuery by the Bruin wrapper),
explodes semicolon-delimited song lists, and computes the Jaccard similarity
index between every pair of artists that share at least one song.

Input:  spark_jobs/data/input/fact_concert.parquet
Output: spark_jobs/data/output/artist_similarity.parquet
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    data_dir = Path(__file__).resolve().parent / "data"
    input_path = str(data_dir / "input" / "fact_concert.parquet")
    output_path = str(data_dir / "output" / "artist_similarity.parquet")

    spark = (
        SparkSession.builder
        .appName("GigwiseArtistSimilarity")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        concerts = spark.read.parquet(input_path)
    except Exception as exc:
        print(f"[ERROR] Cannot read {input_path}: {exc}", file=sys.stderr)
        raise SystemExit(1)

    # Only setlistfm rows have song data
    setlist_concerts = concerts.filter(
        (F.col("source") == "setlistfm") & F.col("songs_played").isNotNull()
    )

    if setlist_concerts.count() == 0:
        print("[WARN] No setlist data with songs — writing empty output")
        spark.createDataFrame([], "artist_a STRING, artist_b STRING, shared_songs LONG, jaccard_similarity DOUBLE, shared_songs_detail STRING").write.mode("overwrite").parquet(output_path)
        spark.stop()
        return

    # Explode songs: one row per (artist_name, song)
    songs = (
        setlist_concerts
        .select("artist_name", F.explode(F.split("songs_played", "; ")).alias("song"))
        .withColumn("song", F.trim(F.col("song")))
        .filter(F.col("song") != "")
        .dropDuplicates(["artist_name", "song"])
    )

    # Count unique songs per artist (for Jaccard denominator)
    artist_song_counts = songs.groupBy("artist_name").agg(
        F.countDistinct("song").alias("song_count")
    )

    # Self-join on shared songs
    a = songs.alias("a")
    b = songs.alias("b")
    pairs = (
        a.join(b, (F.col("a.song") == F.col("b.song")) & (F.col("a.artist_name") < F.col("b.artist_name")))
        .select(
            F.col("a.artist_name").alias("artist_a"),
            F.col("b.artist_name").alias("artist_b"),
            F.col("a.song").alias("song"),
        )
        .dropDuplicates()
    )

    shared = pairs.groupBy("artist_a", "artist_b").agg(
        F.count("song").alias("shared_songs"),
        F.concat_ws("; ", F.sort_array(F.collect_list("song"))).alias("shared_songs_detail"),
    )

    # Jaccard = |A ∩ B| / |A ∪ B| = shared / (count_a + count_b - shared)
    result = (
        shared
        .join(artist_song_counts.alias("ca"), F.col("artist_a") == F.col("ca.artist_name"))
        .join(artist_song_counts.alias("cb"), F.col("artist_b") == F.col("cb.artist_name"))
        .withColumn(
            "jaccard_similarity",
            F.round(
                F.col("shared_songs") / (F.col("ca.song_count") + F.col("cb.song_count") - F.col("shared_songs")),
                4,
            ),
        )
        .select("artist_a", "artist_b", "shared_songs", "jaccard_similarity", "shared_songs_detail")
        .orderBy(F.col("jaccard_similarity").desc())
    )

    result.write.mode("overwrite").parquet(output_path)
    row_count = result.count()
    print(f"[OK] Wrote {row_count} artist-pair similarities to {output_path}")
    spark.stop()


if __name__ == "__main__":
    main()
