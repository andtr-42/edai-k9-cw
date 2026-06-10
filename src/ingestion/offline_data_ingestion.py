from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from src.config import (
    DELTA_VERSION,
    HADOOP_VERSION,
    OFFLINE_DATA_PATH,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    BRONZE_BUCKET,
)

def add_metadata_columns(df: DataFrame) -> DataFrame:
    """Appends lineage metadata to the DataFrame."""
    return df.withColumn("raw_id", F.monotonically_increasing_id()) \
             .withColumn("_ingested_at", F.current_timestamp()) \
             .select("raw_id", *df.columns, "_ingested_at")

def extract_data(spark: SparkSession, data_path: str) -> DataFrame:
    """Reads Parquet data with schema merging enabled and appends metadata."""
    full_path = f"{OFFLINE_DATA_PATH}/{data_path}"
    
    df = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(full_path)
    )
    
    df = add_metadata_columns(df)
    
    df.show(5)
    df.printSchema()
    return df

def upload_delta(
    df: DataFrame, 
    bucket_name: str, 
    topic: str, 
    partition_cols: list[str] | None = None
) -> None:
    """Write a Delta table directly to a specified MinIO bucket using native PySpark, 

    optionally partitioning by specified columns.
    """
    target_path = f"s3a://{bucket_name}/topics/{topic}"
    
    # Initialize the writer base configuration
    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("overwriteSchema", "true")
    )
    
    # Dynamically inject the partition columns if they are provided
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
      
    # Execute the write to MinIO/S3
    writer.save(target_path)
      
    print(f"  Uploaded {target_path} ({df.count()} rows) with partition column {partition_cols}")
   
if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("ingest_offline_data")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")

        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        
        # ---- DELTA LAKE DEPENDENCIES & EXTENSIONS ----
        .config("spark.jars.packages", f"io.delta:delta-spark_2.12:{DELTA_VERSION},org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # ---- S3A / MINIO STORAGE CONFIGURATION ----
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Ingestion Manifest
    datasets = {
        "raw_users": {"path": "users.parquet", "partition_cols": None},
        "raw_movies": {"path": "movies", "partition_cols": None},
        "raw_playbacks": {"path": "playbacks", "partition_cols": None}, # change from ["playback_date"] for flat table
        "raw_ratings": {"path": "ratings", "partition_cols": None}, # change from ["rating_date"] for flat table
        "raw_payment_attempts": {"path": "payments", "partition_cols": None} # change from ["rating_date"] for flat table
    }

    for topic, config in datasets.items():
        print(f"Processing {topic}...")
        df = extract_data(spark, config["path"])
        
        # Passing BRONZE_BUCKET and the partition columns explicitly here
        upload_delta(
            df=df, 
            bucket_name=BRONZE_BUCKET, 
            topic=topic, 
            partition_cols=config["partition_cols"]
        )