"""
Creator-events ETL — a PySpark batch pipeline (bronze → silver → gold).

What it does:
  1. BRONZE: read raw semi-structured events (JSONL) with an explicit schema.
  2. SILVER: clean & conform — parse timestamps, drop bad/duplicate rows,
             enrich by joining the artist dimension.
  3. GOLD:   aggregate to daily per-artist metrics + a 7-day rolling
             "momentum" metric (window function), then write partitioned Parquet.

Runs locally in Spark local mode, or against S3 (s3a://) on a cluster/EMR —
the only thing that changes is the --input/--output paths (see README).

Usage (local):
    python src/etl.py --input data/raw/events.jsonl \
                      --artists data/dim/artists.csv \
                      --output data/warehouse

Usage (S3):
    spark-submit src/etl.py \
        --input  s3a://my-bucket/raw/events/ \
        --artists s3a://my-bucket/dim/artists.csv \
        --output s3a://my-bucket/warehouse/daily_artist_metrics/
"""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
)

# Explicit schema for the raw events — never infer schema on a real pipeline.
RAW_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("artist_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("country", StringType(), True),
    StructField("ts", StringType(), True),
    StructField("value", IntegerType(), True),
])

VALID_EVENT_TYPES = ["stream", "save", "follow", "share", "goal_progress"]


def build_spark(app_name="creator-events-etl"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")  # small for local; raise on a cluster
        .getOrCreate()
    )


def read_bronze(spark, input_path):
    """Raw events, exactly as landed — schema enforced, nothing cleaned yet."""
    return spark.read.schema(RAW_SCHEMA).json(input_path)


def to_silver(bronze, artists):
    """Clean, conform, and enrich into an analytics-ready fact table."""
    cleaned = (
        bronze
        # parse the timestamp; bad strings become null and get filtered out
        .withColumn("event_ts", F.to_timestamp("ts"))
        # data-quality gates
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("artist_id").isNotNull())
        .filter(F.col("event_ts").isNotNull())
        .filter(F.col("event_type").isin(VALID_EVENT_TYPES))
        # exactly-once: collapse duplicate event_ids, keeping the latest
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("event_id").orderBy(F.col("event_ts").desc())
            ),
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("event_date", F.to_date("event_ts"))
    )

    # Enrich with the artist dimension (broadcast — the dim is small).
    return cleaned.join(F.broadcast(artists), on="artist_id", how="left")


def to_gold(silver):
    """Daily per-artist metrics + a 7-day rolling momentum metric."""
    daily = (
        silver.groupBy("event_date", "artist_id", "archetype")
        .agg(
            F.sum(F.when(F.col("event_type") == "stream", 1).otherwise(0)).alias("streams"),
            F.sum(F.when(F.col("event_type") == "save", 1).otherwise(0)).alias("saves"),
            F.sum(F.when(F.col("event_type") == "follow", 1).otherwise(0)).alias("follows"),
            F.sum(F.when(F.col("event_type") == "share", 1).otherwise(0)).alias("shares"),
            F.countDistinct("user_id").alias("unique_listeners"),
            F.sum(F.coalesce(F.col("value"), F.lit(0))).alias("goal_progress"),
        )
        # a simple weighted engagement score
        .withColumn(
            "engagement_score",
            F.col("streams") + F.col("saves") * 3 + F.col("follows") * 5 + F.col("shares") * 4,
        )
    )

    # 7-day rolling streams per artist (window function over the last 7 days).
    w = (
        Window.partitionBy("artist_id")
        .orderBy(F.col("event_date").cast("timestamp").cast("long"))
        .rangeBetween(-6 * 86400, 0)  # inclusive 7-day window, in seconds
    )
    return daily.withColumn("streams_7d", F.sum("streams").over(w))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="raw events path (local dir/file or s3a://)")
    ap.add_argument("--artists", required=True, help="artist dimension CSV (local or s3a://)")
    ap.add_argument("--output", required=True, help="output warehouse path (local or s3a://)")
    args = ap.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze = read_bronze(spark, args.input)
    artists = spark.read.option("header", True).csv(args.artists)

    silver = to_silver(bronze, artists).cache()
    gold = to_gold(silver)

    # Write the gold table partitioned by date — the standard warehouse layout
    # that lets downstream queries prune partitions.
    (
        gold.repartition("event_date")
        .write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(args.output)
    )

    # --- Quick data-quality / demo report (also shows Spark SQL) ---
    silver.createOrReplaceTempView("events")
    top = spark.sql(
        """
        SELECT archetype,
               COUNT(*)                        AS events,
               COUNT(DISTINCT artist_id)       AS artists,
               COUNT(DISTINCT user_id)         AS listeners
        FROM events
        GROUP BY archetype
        ORDER BY events DESC
        """
    )
    print("\n=== Events by archetype (silver) ===")
    top.show(truncate=False)

    print(f"Bronze rows read : {bronze.count():,}")
    print(f"Silver rows kept : {silver.count():,}")
    print(f"Gold rows written: {gold.count():,} -> {args.output}")

    spark.stop()


if __name__ == "__main__":
    main()
