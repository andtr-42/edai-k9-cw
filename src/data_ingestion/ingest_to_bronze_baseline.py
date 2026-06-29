"""
Bronze Layer Ingestion Pipeline

This script extracts raw data from a MinIO source bucket (DATA_SOURCE_BUCKET)
using source-specific credentials, appends structural metadata, 
and writes the enriched datasets into a target bronze bucket (BRONZE_BUCKET)
on a separate lakehouse cluster using target-specific credentials in Delta format.
"""

import time
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from src.config import (
    DELTA_VERSION,
    HADOOP_VERSION,
    DATA_SOURCE_BUCKET,
    DATA_SOURCE_ENDPOINT,
    DATA_SOURCE_ACCESS_KEY,
    DATA_SOURCE_SECRET_KEY,
    LAKEHOUSE_ENDPOINT,
    LAKEHOUSE_ACCESS_KEY,
    LAKEHOUSE_SECRET_KEY,
    BRONZE_BUCKET,
)

def add_metadata_columns(df: DataFrame) -> DataFrame:
    """Appends audit and data lineage metadata columns to the DataFrame."""
    return df.withColumn("raw_id", F.monotonically_increasing_id()) \
             .withColumn("_ingested_at", F.current_timestamp()) \
             .select("raw_id", *df.columns, "_ingested_at")

def extract_data(spark: SparkSession, bucket_name: str, data_path: str) -> DataFrame:
    """Reads Parquet data directly from the S3/MinIO source bucket with schema merging."""
    full_path = f"s3a://{bucket_name}/{data_path}"
    
    df = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(full_path)
    )
    return add_metadata_columns(df)

def upload_delta(
    df: DataFrame, 
    bucket_name: str, 
    topic: str, 
    partition_cols: list[str] | None = None
) -> None:
    """Writes a Spark DataFrame directly to the designated Lakehouse bucket in Delta format."""
    target_path = f"s3a://{bucket_name}/topics/{topic}"
    
    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("overwriteSchema", "true")
    )
    
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
      
    writer.save(target_path)
    print(f"✔ Successfully uploaded {target_path} to Delta Lake house.")
   
if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("bronze_layer_ingestion")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        
        # ---- DELTA LAKE DEPENDENCIES & EXTENSIONS ----
        .config("spark.jars.packages", f"io.delta:delta-spark_2.12:{DELTA_VERSION},org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # ---- GLOBAL S3A CORE STORAGE CONFIGURATION ----
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        
        # ---- DUAL CREDENTIAL PER-BUCKET ROUTING ----
        # 1. Source Credentials mapped explicitly to DATA_SOURCE_BUCKET
        .config(f"spark.hadoop.fs.s3a.bucket.{DATA_SOURCE_BUCKET}.endpoint", f"http://{DATA_SOURCE_ENDPOINT}")
        .config(f"spark.hadoop.fs.s3a.bucket.{DATA_SOURCE_BUCKET}.access.key", DATA_SOURCE_ACCESS_KEY)
        .config(f"spark.hadoop.fs.s3a.bucket.{DATA_SOURCE_BUCKET}.secret.key", DATA_SOURCE_SECRET_KEY)
        
        # 2. Target Lakehouse Credentials mapped explicitly to BRONZE_BUCKET
        .config(f"spark.hadoop.fs.s3a.bucket.{BRONZE_BUCKET}.endpoint", f"http://{LAKEHOUSE_ENDPOINT}")
        .config(f"spark.hadoop.fs.s3a.bucket.{BRONZE_BUCKET}.access.key", LAKEHOUSE_ACCESS_KEY)
        .config(f"spark.hadoop.fs.s3a.bucket.{BRONZE_BUCKET}.secret.key", LAKEHOUSE_SECRET_KEY)
        
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Ingestion Manifest
    datasets = {
        "raw_users": {"path": "users.parquet", "partition_cols": None},
        "raw_movies": {"path": "movies", "partition_cols": None},
        "raw_playbacks": {"path": "playbacks", "partition_cols": None}, 
        "raw_ratings": {"path": "ratings", "partition_cols": None}, 
        "raw_payment_attempts": {"path": "payments", "partition_cols": None} 
    }

    # Execute batch processing across the manifest sequentially
    for topic, config in datasets.items():
        print(f"\n==================== Processing Topic: {topic} ====================")
        spark.sparkContext.setJobGroup(groupId=topic, description=f"Ingesting {topic}", interruptOnCancel=True)
        
        # Read using Data Source Bucket configurations
        df = extract_data(spark, bucket_name=DATA_SOURCE_BUCKET, data_path=config["path"])
        
        # Write using Bronze Bucket configurations
        upload_delta(
            df=df, 
            bucket_name=BRONZE_BUCKET, 
            topic=topic, 
            partition_cols=config["partition_cols"]
        )
        spark.sparkContext.setJobGroup(None, None)

    # ---- KEEP-ALIVE BLOCK (Moved out of loop to run at the very end) ----
    print("\n🚀 All ingestion topics processed successfully!")
    print("Spark UI is live at http://localhost:4040 to inspect metrics and execution DAGs.")
    print("Press Ctrl+C to stop the session and exit.")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down Spark Session gracefully...")
        spark.stop()