"""
Bronze Layer Ingestion Pipeline

This script extracts raw data from a MinIO source bucket (DATA_SOURCE_BUCKET)
using source-specific credentials, appends structural metadata, 
and writes the enriched datasets into a target bronze bucket (BRONZE_BUCKET)
on a separate lakehouse cluster using target-specific credentials in Delta format.

python3 -m src.data_ingestion.ingest_to_bronze
"""

import time
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType
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

USER_SCHEMA = StructType([
    StructField("user_id",           LongType(),      nullable=False),
    StructField("gender",            StringType(),    nullable=True),  # Must be nullable to apply fillna
    StructField("age",               LongType(),      nullable=True),
    StructField("subscription_type", StringType(),    nullable=True),
    StructField("signup_ts",         TimestampType(), nullable=True),
])

def add_metadata_columns(df: DataFrame) -> DataFrame:
    """Appends audit and data lineage metadata columns to the DataFrame."""
    return df.withColumn("raw_id", F.monotonically_increasing_id()) \
             .withColumn("_ingested_at", F.current_timestamp()) \
             .select("raw_id", *df.columns, "_ingested_at")

def extract_data(
    spark: SparkSession, 
    bucket_name: str, 
    data_path: str, 
    explicit_schema: StructType | None = None,
    fill_na_dict: dict | None = None
) -> DataFrame:
    """Reads Parquet data using explicit schema and applies fillna handling if provided."""
    full_path = f"s3a://{bucket_name}/{data_path}"
    
    reader = spark.read
    if explicit_schema:
        # Enforcing schema directly eliminates the costly mergeSchema job overhead
        # reader = reader.option("mergeSchema", "True")  # Allow schema merging if needed
        reader = reader.schema(explicit_schema)
        
    df = reader.parquet(full_path)
    
    if fill_na_dict:
        df = df.fillna(fill_na_dict)
        
    return add_metadata_columns(df)

def extract_data_with_schema_merging(spark: SparkSession, bucket_name: str, data_path: str) -> DataFrame:
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
        
        .config("spark.sql.adaptive.enabled", "false") 
        .config("spark.sql.shuffle.partitions", "8")  # Optimized for small datasets               
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Ingestion Manifest
    datasets = {
        "raw_users": {"path": "users", "partition_cols": None},
        "raw_movies": {"path": "movies.parquet", "partition_cols": None},
        "raw_playbacks": {"path": "playbacks.parquet", "partition_cols": None}, 
        "raw_ratings": {"path": "ratings.parquet", "partition_cols": None}, 
        "raw_payment_attempts": {"path": "payments.parquet", "partition_cols": None} 
    }

    # Execute batch processing across the manifest sequentially
    datasets = {
        "raw_users": {
            "path": "users", 
            "partition_cols": None, 
            "schema": USER_SCHEMA, 
            "fillna": {"gender": "UNKNOWN"} # Fills missing gender values smoothly
        },
        "raw_movies": {"path": "movies.parquet", "partition_cols": None, "schema": None, "fillna": None},
        "raw_playbacks": {"path": "playbacks.parquet", "partition_cols": None, "schema": None, "fillna": None}, 
        "raw_ratings": {"path": "ratings.parquet", "partition_cols": None, "schema": None, "fillna": None}, 
        "raw_payment_attempts": {"path": "payments.parquet", "partition_cols": None, "schema": None, "fillna": None} 
    }

    # Sequence execution loop
    for topic, config in datasets.items():
        print(f"\n==================== Processing Topic: {topic} ====================")
        spark.sparkContext.setJobGroup(groupId=topic, description=f"Ingesting {topic}", interruptOnCancel=True)
        
        df = extract_data(
            spark, 
            bucket_name=DATA_SOURCE_BUCKET, 
            data_path=config["path"], 
            explicit_schema=config["schema"],
            fill_na_dict=config["fillna"]
        )
        
        upload_delta(df=df, bucket_name=BRONZE_BUCKET, topic=topic, partition_cols=config["partition_cols"])
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