from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from minio import Minio

OFFLINE_DATA_PATH = "../data/raw/offline"
BUCKET = "bronze-bucket"

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
DELTA_VERSION = "3.2.0"       # Matches PySpark 3.5.x
HADOOP_VERSION = "3.3.4"      # Matches the Hadoop version Spark was compiled with

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("ingest_offline_data")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    
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

def extract_data(data_path: str):
    
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .parquet(f"{OFFLINE_DATA_PATH}/{data_path}")
    )

    # add two columns raw_id (auto increment id) and _ingested_at (timestamp)
    # with raw_id, other columns and _ingested_at 
    df = df.withColumn("raw_id", F.monotonically_increasing_id()) \
        .withColumn("_ingested_at", F.current_timestamp()) \
        .select("raw_id", *[col for col in df.columns], "_ingested_at")
    
    df.show(5)
    df.printSchema()
        
    return df

def extract_data_with_merge_schema(data_path: str):
    
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mergeSchema", "true")
        .parquet(f"{OFFLINE_DATA_PATH}/{data_path}")
    )

    # add two columns raw_id (auto increment id) and _ingested_at (timestamp)
    # with raw_id, other columns and _ingested_at 
    df = df.withColumn("raw_id", F.monotonically_increasing_id()) \
        .withColumn("_ingested_at", F.current_timestamp()) \
        .select("raw_id", *[col for col in df.columns], "_ingested_at")
    
    df.show(5)
    df.printSchema()
    
    return df

def upload_delta(df: DataFrame, topic: str):
    """Write a Delta table directly to MinIO using native PySpark DataFrame writers."""
    
    # Define target path using the s3a:// protocol
    target_path = f"s3a://{BUCKET}/topics/{topic}"
    
    # Native Spark save operation
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("mergeSchema", "true") \
      .save(target_path)
      
    print(f"  Uploaded {target_path}  ({df.count()} rows)")

def main():
    # ingest users data
    user_df = extract_data("users.parquet")
    upload_delta(user_df, "raw_users")

    # ingest movies data with mergeSchema to handle evolving schema
    movies_df = extract_data_with_merge_schema("movies_0.6_drama_20260304")
    upload_delta(movies_df, "raw_movies")

    # ingest playback data 
    playback_df = extract_data("playbacks/*")
    upload_delta(playback_df, "raw_playbacks")

    # ingest ratings data
    ratings_df = extract_data("ratings_20260304/*")
    upload_delta(ratings_df, "raw_ratings")

    # ingest payment attempts data 
    payment_attempts_df = extract_data("payments/*")
    upload_delta(payment_attempts_df, "raw_payment_attempts")

if __name__ == "__main__":
    main()



