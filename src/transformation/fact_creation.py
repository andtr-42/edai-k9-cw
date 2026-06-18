import hashlib
import numpy as np
import pandas as pd
from deltalake import write_deltalake
from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
from pyspark.sql import functions as F
from src.config import (
    DELTA_VERSION,
    HADOOP_VERSION,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    SILVER_BUCKET,
    GOLD_BUCKET,
)


def extract_from_lakehouse(spark: SparkSession, bucket_name: str, topic: str) -> SparkDataFrame:
    """Extracts tables from the lakehouse layer using Spark."""
    minio_path = f"s3a://{bucket_name}/topics/{topic}"
    return spark.read.format("delta").load(minio_path)


# =============================================================================
# GOLD LAYER FACT TABLE TRANSFORMATIONS (REFACTORED WITH PARAMETERS)
# =============================================================================

def transform_fact_playback(
    silver_playback_df: SparkDataFrame, 
    dim_user_df: SparkDataFrame, 
    dim_movie_df: SparkDataFrame
) -> SparkDataFrame:
    """
    Assembles fact_playback using parameterized dimensions for Point-in-Time joins.
    """
    user_pit_condition = (
        (F.col("silver.user_id") == F.col("du.user_id")) &
        (F.col("silver.start_ts") >= F.col("du.valid_from_ts")) &
        (F.col("silver.start_ts") < F.col("du.valid_to_ts"))
    )
    movie_pit_condition = (
        (F.col("silver.movie_id") == F.col("dm.movie_id")) &
        (F.col("silver.start_ts") >= F.col("dm.valid_from_ts")) &
        (F.col("silver.start_ts") < F.col("dm.valid_to_ts"))
    )
    
    return silver_playback_df.alias("silver") \
        .join(dim_user_df.alias("du"), user_pit_condition, "inner") \
        .join(dim_movie_df.alias("dm"), movie_pit_condition, "inner") \
        .select(
            F.col("silver.playback_id"),
            F.col("du.user_key"),
            F.col("dm.movie_key"),
            
            F.date_format(F.col("silver.click_ts"), "yyyyMMdd").cast("int").alias("click_date_key"),
            F.date_format(F.col("silver.start_ts"), "yyyyMMdd").cast("int").alias("start_date_key"),
            
            F.date_format(F.col("silver.end_ts"), "yyyyMMdd").cast("int").alias("stop_date_key"), 
            
            F.hour(F.col("silver.click_ts")).alias("click_hour"),
            F.hour(F.col("silver.start_ts")).alias("start_hour"),
            
            # --- FIX 2: Change stop_ts to end_ts ---
            F.hour(F.col("silver.end_ts")).alias("stop_hour"), 
            
            F.col("silver.click_ts"),
            F.col("silver.start_ts"),
            
            # --- FIX 3: Change stop_ts to end_ts ---
            F.col("silver.end_ts").alias("stop_ts"), # Aliased to keep your target DBML design clean!
            
            F.col("silver.duration_watched_seconds"),
            F.col("silver.completion_rate")
        )


def transform_fact_rating(
    silver_rating_df: SparkDataFrame, 
    dim_user_df: SparkDataFrame, 
    dim_movie_df: SparkDataFrame
) -> SparkDataFrame:
    """
    Assembles fact_rating using parameterized dimensions for Point-in-Time joins.
    """
    user_pit_condition = (
        (F.col("silver.user_id") == F.col("du.user_id")) &
        (F.col("silver.rating_ts") >= F.col("du.valid_from_ts")) &
        (F.col("silver.rating_ts") < F.col("du.valid_to_ts"))
    )
    movie_pit_condition = (
        (F.col("silver.movie_id") == F.col("dm.movie_id")) &
        (F.col("silver.rating_ts") >= F.col("dm.valid_from_ts")) &
        (F.col("silver.rating_ts") < F.col("dm.valid_to_ts"))
    )
    
    return silver_rating_df.alias("silver") \
        .join(dim_user_df.alias("du"), user_pit_condition, "inner") \
        .join(dim_movie_df.alias("dm"), movie_pit_condition, "inner") \
        .select(
            F.col("silver.rating_id"),
            F.col("du.user_key"),
            F.col("dm.movie_key"),
            F.date_format(F.col("silver.rating_ts"), "yyyyMMdd").cast("int").alias("rating_date_key"),
            F.hour(F.col("silver.rating_ts")).alias("rating_hour"),
            F.col("silver.rating_ts"),
            F.col("silver.rating")
        )


def transform_fact_payment_attempt(
    silver_payment_df: SparkDataFrame, 
    dim_user_df: SparkDataFrame
) -> SparkDataFrame:
    """
    Assembles fact_payment_attempt using a Point-in-Time join against 
    the dim_user SCD2 dimension based on payment transaction timestamps.
    """
    # Formulate PIT condition to map the historical user_key correctly
    user_pit_condition = (
        (F.col("silver.user_id") == F.col("du.user_id")) &
        (F.col("silver.payment_ts") >= F.col("du.valid_from_ts")) &
        (F.col("silver.payment_ts") < F.col("du.valid_to_ts"))
    )
    
    return silver_payment_df.alias("silver") \
        .join(dim_user_df.alias("du"), user_pit_condition, "inner") \
        .select(
            # Read payment_id from silver, alias to target payment_attempt_id
            F.col("silver.payment_id").alias("payment_attempt_id"),
            
            # Resolved Gold Surrogate Key from the SCD2 dimension
            F.col("du.user_key"),
            
            # Map calendar date key directly as an integer (yyyymmdd)
            F.date_format(F.col("silver.payment_ts"), "yyyyMMdd").cast("int").alias("payment_date_key"),
            
            # Compute the surrogate key inline from raw text value to match dim_payment_method
            F.md5(F.col("silver.payment_method")).alias("payment_method_key"),
            
            # Measures
            F.col("silver.amount").cast("decimal(10,2)"),
            
            # Derive binary metrics from the raw payment_status string
            F.when(F.col("silver.payment_status") == "Success", 1).otherwise(0).alias("is_payment_success"),
            F.when(F.col("silver.payment_status") == "Failed", 1).otherwise(0).alias("is_payment_failed")
        )


# =============================================================================
# EXECUTABLE RUNTIME DRIVER
# =============================================================================

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("compile_gold_fact_warehouse")
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
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("--- Loading Gold Reference Dimensions Into Engine Memory ---")
    # Read the available Gold dimensions once to pass them into the transformations
    shared_dim_user = extract_from_lakehouse(spark, GOLD_BUCKET, "dim_user")
    shared_dim_movie = extract_from_lakehouse(spark, GOLD_BUCKET, "dim_movie")

    print("--- Starting Gold Layer Fact Table Batch Compilation ---")

    # Fact: Playback Events
    print("Compiling fact_playback...")
    silver_playbacks = extract_from_lakehouse(spark, SILVER_BUCKET, "stg_playbacks")
    gold_playback_fact = transform_fact_playback(silver_playbacks, shared_dim_user, shared_dim_movie)
    print(f"Number of rows in fact table: {gold_playback_fact.count()}")
    gold_playback_fact.write.format("delta").mode("append").save(f"s3a://{GOLD_BUCKET}/topics/fact_playback")

    # Fact: Historical Content Ratings
    print("Compiling fact_rating...")
    silver_ratings = extract_from_lakehouse(spark, SILVER_BUCKET, "stg_ratings")
    gold_ratings_fact = transform_fact_rating(silver_ratings, shared_dim_user, shared_dim_movie)
    print(f"Number of rows in fact table: {gold_ratings_fact.count()}")
    gold_ratings_fact.write.format("delta").mode("append").save(f"s3a://{GOLD_BUCKET}/topics/fact_rating")

    # Fact: Transaction Financial Actions
    print("Compiling fact_payment_attempt...")
    silver_payments = extract_from_lakehouse(spark, SILVER_BUCKET, "stg_payment_attempts")
    gold_payments_fact = transform_fact_payment_attempt(silver_payments, shared_dim_user)
    print(f"Number of rows in fact table: {gold_payments_fact.count()}")
    gold_payments_fact.write.format("delta").mode("append").save(f"s3a://{GOLD_BUCKET}/topics/fact_payment_attempt")

    print("--- Warehouse Gold Fact Compilation Finished Safely ---")