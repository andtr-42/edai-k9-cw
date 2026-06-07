from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from minio import Minio
from create_bucket import BRONZE_BUCKET_NAME, SILVER_BUCKET_NAME, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

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

def extract_data_without_duplicates(bucket_name: str, topic: str) -> DataFrame:

    minio_path = f"s3a://{bucket_name}/topics/{topic}"

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .format("delta") \
        .load(minio_path)
    
    # drop raw_id column if exists 
    if "raw_id" in df.columns:
        df = df.drop("raw_id")
    
    df.show(5)
    df.printSchema()

    return df

def extract_data_with_duplicates(bucket_name: str, topic: str, dedup_columns: list) -> DataFrame:

    minio_path = f"s3a://{bucket_name}/topics/{topic}"

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .format("delta") \
        .load(minio_path)
        
    dedup_df = df.dropDuplicates(dedup_columns)

    # drop raw_id column if exists
    if "raw_id" in dedup_df.columns:
        dedup_df = dedup_df.drop("raw_id")
    
    dedup_df.show(5)
    dedup_df.printSchema()

    print(f" Row count before deduplication: {df.count()}")
    print(f" Row count after deduplication: {dedup_df.count()}")

    return dedup_df

def upload_delta(df: DataFrame, topic: str):
    
    target_path = f"s3a://{SILVER_BUCKET_NAME}/topics/{topic}"
    
    df.write \
       .format("delta") \
       .mode("overwrite") \
       .option("mergeSchema", "true") \
       .save(target_path)
    
    print(f"  Uploaded {target_path}  ({df.count()} rows)")

spark.sparkContext.setLogLevel("WARN")

def main():
    
    # extract users data from bronze bucket 
    users_df = extract_data_without_duplicates(BRONZE_BUCKET_NAME, "raw_users")
    upload_delta(users_df, "stg_users")
    
    # extract movies data from bronze bucket
    movies_df = extract_data_without_duplicates(BRONZE_BUCKET_NAME, "raw_movies")
    upload_delta(movies_df, "stg_movies")

    # extract playback data from bronze bucket with deduplication on user_id, movie_id and start_ts
    playbacks_df = extract_data_with_duplicates(BRONZE_BUCKET_NAME, "raw_playbacks", ["user_id", "movie_id", "click_ts"])
    upload_delta(playbacks_df, "stg_playbacks")

    # extract ratings data from bronze bucket
    ratings_df = extract_data_without_duplicates(BRONZE_BUCKET_NAME, "raw_ratings")
    upload_delta(ratings_df, "stg_ratings")

    # extract payment attempts data from bronze bucket
    payments_df = extract_data_without_duplicates(BRONZE_BUCKET_NAME, "raw_payment_attempts")
    upload_delta(payments_df, "stg_payment_attempts")

if __name__ == "__main__":
    main()

    

