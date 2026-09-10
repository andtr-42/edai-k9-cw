"""
Silver Layer Processing Pipeline

This script extracts data from the Delta Bronze layer, applies deduplication 
and schema cleaning, and writes the refined datasets to the Silver layer.

python3 -m src.data_processing.process_to_silver
"""

import time
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from src.config import (
    DELTA_VERSION,
    HADOOP_VERSION,
    LAKEHOUSE_ENDPOINT,
    LAKEHOUSE_ACCESS_KEY,
    LAKEHOUSE_SECRET_KEY,
    BRONZE_BUCKET,
    SILVER_BUCKET,
)

def extract_and_clean_bronze(
    spark: SparkSession,
    bucket_name: str, 
    topic: str, 
    dedup_columns: list | None = None
) -> DataFrame:
    """Extracts data from the Delta bronze layer, deduplicates, and cleans schema."""
    minio_path = f"s3a://{bucket_name}/topics/{topic}"

    # Delta format handles headers and schema natively
    df = spark.read.format("delta").load(minio_path)

    # Apply deduplication if columns are provided
    if dedup_columns:
        df = df.dropDuplicates(dedup_columns)

    # Drop Bronze-specific lineage column if present
    if "raw_id" in df.columns:
        df = df.drop("raw_id")
    
    return df

def upload_to_silver(
    spark: SparkSession, 
    df: DataFrame, 
    silver_bucket: str, 
    topic: str
) -> None:
    """Writes the DataFrame to the Delta silver layer."""
    target_path = f"s3a://{silver_bucket}/topics/{topic}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true") # Replaced mergeSchema with overwriteSchema for clean state
        .save(target_path)
    )

    print(f"✔ Uploaded {target_path}.")

if __name__ == "__main__":
    # ---- SPARK SESSION INITIALIZATION ----
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("silver_layer_processing")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        
        # ---- DELTA LAKE DEPENDENCIES & EXTENSIONS ----
        .config("spark.jars.packages", f"io.delta:delta-spark_2.12:{DELTA_VERSION},org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # ---- S3A / MINIO STORAGE CONFIGURATION ----
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{LAKEHOUSE_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", LAKEHOUSE_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", LAKEHOUSE_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")                
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Processing Manifest mapped to target Silver topics and dedup rules
    datasets = {
        "raw_users":            {"silver_topic": "stg_users",            "dedup_cols": None},
        "raw_movies":           {"silver_topic": "stg_movies",           "dedup_cols": None},
        "raw_playbacks":        {"silver_topic": "stg_playbacks",        "dedup_cols": ["user_id", "movie_id", "click_ts"]},
        "raw_ratings":          {"silver_topic": "stg_ratings",          "dedup_cols": None},
        "raw_payment_attempts": {"silver_topic": "stg_payment_attempts", "dedup_cols": None}
    }

    # Execute batch processing across the manifest sequentially
    for bronze_topic, config in datasets.items():
        silver_topic = config["silver_topic"]
        print(f"\n==================== Processing Topic: {bronze_topic} -> {silver_topic} ====================")
        
        spark.sparkContext.setJobGroup(groupId=silver_topic, description=f"Processing {silver_topic}", interruptOnCancel=True)
        
        # Extract and transform
        df = extract_and_clean_bronze(
            spark, 
            bucket_name=BRONZE_BUCKET, 
            topic=bronze_topic, 
            dedup_columns=config["dedup_cols"]
        )
        
        # Load
        upload_to_silver(
            spark, 
            df=df, 
            silver_bucket=SILVER_BUCKET, 
            topic=silver_topic
        )
        
        spark.sparkContext.setJobGroup(None, None)

    print("\n🚀 All Silver layer topics processed successfully!")
    
    # Optional keep-alive block for Spark UI inspection
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down Spark Session gracefully...")
        spark.stop()