from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
from pyspark.sql import functions as F
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
# OBT TABLE TRANSFORMATIONS
# =============================================================================

def transform_obt_playback_performance(
    fact_playback_df: SparkDataFrame,
    dim_user_df: SparkDataFrame,
    dim_movie_df: SparkDataFrame,
    dim_date_df: SparkDataFrame
) -> SparkDataFrame:
    """
    Assembles the One Big Table (OBT) for playback performance.
    Joins the fact table with its corresponding dimensions using Gold surrogate keys.
    """
    return fact_playback_df.alias("fp") \
        .join(dim_user_df.alias("du"), F.col("fp.user_key") == F.col("du.user_key"), "inner") \
        .join(dim_movie_df.alias("dm"), F.col("fp.movie_key") == F.col("dm.movie_key"), "inner") \
        .join(dim_date_df.alias("dd"), F.col("fp.start_date_key") == F.col("dd.date_key"), "inner") \
        .select(
            # Core Identifiers
            F.col("fp.playback_id"),
            F.col("du.user_id"),
            F.col("dm.movie_id"),
            
            # Temporal Context (Driven by playback start time)
            F.col("dd.calendar_date"),
            F.col("dd.day_of_week"),
            F.col("dd.is_weekend"),
            F.col("fp.start_hour"),
            
            # User Demographics
            F.col("du.age"),
            F.col("du.gender"),
            F.col("du.subscription_type"),
            F.col("du.signup_ts"),
            
            # Content Attributes
            F.col("dm.genre"),
            F.col("dm.country"),
            F.col("dm.runtime_seconds"),
            F.col("dm.language"),
            F.col("dm.release_year"),
            
            # Performance Measures
            F.col("fp.duration_watched_seconds"),
            F.col("fp.completion_rate")
        )

# =============================================================================
# EXECUTABLE RUNTIME DRIVER
# =============================================================================

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("compile_obt_playback_performance")
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

    print("--- Loading Gold Tables Into Engine Memory ---")
    
    # Load Facts
    gold_fact_playback = extract_from_lakehouse(spark, GOLD_BUCKET, "fact_playback")
    
    # Load Dimensions
    gold_dim_user = extract_from_lakehouse(spark, GOLD_BUCKET, "dim_user")
    gold_dim_movie = extract_from_lakehouse(spark, GOLD_BUCKET, "dim_movie")
    gold_dim_date = extract_from_lakehouse(spark, GOLD_BUCKET, "dim_date")

    # Run this right before the OBT inner join logic
    orphaned_dates = gold_fact_playback.join(
        gold_dim_date,
        gold_fact_playback["start_date_key"] == gold_dim_date["date_key"],
        "left_anti"
    )

    print(f"Dropped rows: {orphaned_dates.count()}")
    orphaned_dates.select("start_date_key", "click_ts", "start_ts").show(10)

    
    print("--- Starting OBT Compilation ---")

    print("Compiling obt_playback_performance...")
    obt_playback = transform_obt_playback_performance(
        gold_fact_playback, 
        gold_dim_user, 
        gold_dim_movie, 
        gold_dim_date
    )
    
    print(f"Number of rows in OBT table: {obt_playback.count()}")
    
    # Write to Gold Bucket (Overwrite is often used for OBTs if fully rebuilt, otherwise append/merge)
    target_path = f"s3a://{GOLD_BUCKET}/topics/obt_playback_performance"
    obt_playback.write.format("delta").mode("overwrite").save(target_path)

    print("--- OBT Compilation Finished Safely ---")
    