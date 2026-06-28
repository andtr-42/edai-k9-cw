from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from src.config import (
    DELTA_VERSION,
    HADOOP_VERSION,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    BRONZE_BUCKET,
    SILVER_BUCKET,  # Added missing import
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
    dedup_df = df.dropDuplicates(dedup_columns) if dedup_columns else df

    if "raw_id" in dedup_df.columns:
        dedup_df = dedup_df.drop("raw_id")

    # Debugging output
    dedup_df.show(5)
    dedup_df.printSchema()
    
    return dedup_df


def upload_to_silver(
    spark: SparkSession, 
    df: DataFrame, 
    silver_bucket: str, 
    topic: str
) -> None:
    """Writes the DataFrame to the Delta silver layer."""
    target_path = f"s3a://{silver_bucket}/topics/{topic}"

    # Cache/persist temporarily if you must count, to avoid double execution
    df.persist() 

    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
        target_path
    )

    print(f"Uploaded {target_path} ({df.count()} rows)")
    df.unpersist()


if __name__ == "__main__":
    # ---- SPARK SESSION INITIALIZATION ----
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

    # 1. Users
    users_df = extract_and_clean_bronze(spark, BRONZE_BUCKET, "raw_users")
    upload_to_silver(spark, users_df, SILVER_BUCKET, "stg_users")

    # 2. Movies
    movies_df = extract_and_clean_bronze(spark, BRONZE_BUCKET, "raw_movies")
    upload_to_silver(spark, movies_df, SILVER_BUCKET, "stg_movies")

    # 3. Playbacks (with deduplication)
    playbacks_df = extract_and_clean_bronze(
        spark, BRONZE_BUCKET, "raw_playbacks", ["user_id", "movie_id", "click_ts"]
    )
    upload_to_silver(spark, playbacks_df, SILVER_BUCKET, "stg_playbacks")

    # 4. Ratings
    ratings_df = extract_and_clean_bronze(spark, BRONZE_BUCKET, "raw_ratings")
    upload_to_silver(spark, ratings_df, SILVER_BUCKET, "stg_ratings")

    # 5. Payment Attempts
    payments_df = extract_and_clean_bronze(spark, BRONZE_BUCKET, "raw_payment_attempts")
    upload_to_silver(spark, payments_df, SILVER_BUCKET, "stg_payment_attempts")