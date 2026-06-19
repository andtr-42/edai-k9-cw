import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from src.config import (
    DELTA_VERSION,
    HADOOP_VERSION,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    GOLD_BUCKET,
)

def extract_from_lakehouse(spark: SparkSession, bucket_name: str, topic: str) -> SparkDataFrame:
    """Extracts tables from the lakehouse layer using Spark."""
    minio_path = f"s3a://{bucket_name}/topics/{topic}"
    return spark.read.format("delta").load(minio_path)

# =============================================================================
# FEATURE STORE TRANSFORMATIONS
# =============================================================================

def transform_feat_user_90d(
    obt_playback_df: SparkDataFrame,
    fact_payment_df: SparkDataFrame,
    dim_user_df: SparkDataFrame,
    target_date_str: str
) -> SparkDataFrame:
    """
    Calculates the 90-day rolling window features for users up to the target_date.
    Applies Spark optimizations for skewness, cardinality, and outliers.
    """
    # 1. Define the 90-day rolling window boundaries
    end_date = F.to_date(F.lit(target_date_str))
    start_date = F.date_sub(end_date, 90)

    # 2. Playback Features (Using OBT for pre-joined genre & dates)
    playbacks_90d = obt_playback_df.filter(
        (F.col("calendar_date") > start_date) & 
        (F.col("calendar_date") <= end_date)
    ).groupBy("user_id").agg(
        # AQE handles skewness for basic counts automatically
        F.count("playback_id").cast("int").alias("f_user_total_playbacks_90d"),
        
        # Percentile Approx (Median) to ignore users leaving videos paused for days
        F.expr("percentile_approx(duration_watched_seconds, 0.5)").alias("f_user_avg_duration_watched_seconds_90d"),
        F.expr("percentile_approx(completion_rate, 0.5)").alias("f_user_avg_completion_rate_90d"),
        
        # HyperLogLog for high cardinality distinct counts (5% allowed error)
        F.approx_count_distinct("genre", 0.05).cast("int").alias("f_user_distinct_genre_90d")
    )

    # 3. Payment Features (Using Fact + Dim User to map keys to user_id)
    payment_base = fact_payment_df.join(
        dim_user_df.select("user_key", "user_id"), 
        on="user_key", 
        how="inner"
    )
    
    payments_90d = payment_base.withColumn(
        "payment_date", 
        F.to_date(F.col("payment_date_key").cast("string"), "yyyyMMdd")
    ).filter(
        (F.col("payment_date") > start_date) & 
        (F.col("payment_date") <= end_date)
    ).groupBy("user_id").agg(
        # Two-Phase Aggregation: Native math allows Spark to partial-sum on workers first
        (F.sum("is_payment_failed") / F.count("payment_attempt_id")).cast("double").alias("f_user_payment_fail_rate_90d")
    )

    # 4. Combine All Features
    # Full outer join ensures users who only viewed OR only paid are included
    combined_features = playbacks_90d.join(payments_90d, on="user_id", how="full_outer")

    # 5. Impute Nulls and Append Required Metadata Columns
    final_features = combined_features.fillna({
        "f_user_total_playbacks_90d": 0,
        "f_user_avg_duration_watched_seconds_90d": 0.0,
        "f_user_avg_completion_rate_90d": 0.0,
        "f_user_distinct_genre_90d": 0,
        "f_user_payment_fail_rate_90d": 0.0
    }).withColumn(
        "event_timestamp", F.to_timestamp(F.lit(target_date_str))
    ).withColumn(
        "created_ts", F.current_timestamp()
    )

    return final_features

# =============================================================================
# EXECUTABLE RUNTIME DRIVER
# =============================================================================

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("compile_feat_user_90d")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.jars.packages", f"io.delta:delta-spark_2.12:{DELTA_VERSION},org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        
        # --- EXPLICIT OPTIMIZATIONS ---
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("--- Loading Base Tables For Feature Generation ---")
    obt_playback = extract_from_lakehouse(spark, GOLD_BUCKET, "obt_playback_performance")
    fact_payment = extract_from_lakehouse(spark, GOLD_BUCKET, "fact_payment_attempt")
    dim_user = extract_from_lakehouse(spark, GOLD_BUCKET, "dim_user")

    # In a production environment like Airflow, this date is passed dynamically 
    # (e.g., {{ ds }}). For local backfilling, you can iterate over a list of dates.
    end_date = datetime.strptime("2026-06-07", "%Y-%m-%d")
    target_dates_to_compute = [
        (end_date - timedelta(days=i)).strftime("%Y-%m-%d") 
        for i in range(29, -1, -1)
    ]

    target_path = f"s3a://{GOLD_BUCKET}/features/feat_user_90d"

    print("--- Starting Feature Compilation ---")
    for process_date in target_dates_to_compute:
        print(f"Computing features for anchor date: {process_date}...")
        
        daily_features_df = transform_feat_user_90d(
            obt_playback, fact_payment, dim_user, process_date
        )
        
        # Partition by event_timestamp so Feast can efficiently prune dates when retrieving history
        daily_features_df.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("event_timestamp") \
            .save(target_path)
            
        print(f"Successfully appended features for {process_date}.")

    print("--- Feature Store Compilation Finished Safely ---")