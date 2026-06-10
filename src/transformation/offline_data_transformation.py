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

# ---- GLOBAL TIME DIMENSION CONSTANTS ----
HOURS_IN_DAY = 24
MINUTES_IN_HOUR = 60
SECONDS_IN_MINUTE = 60


def extract_from_silver(spark: SparkSession, bucket_name: str, topic: str) -> SparkDataFrame:
    """Extracts staging data from the Delta silver layer using Spark."""
    minio_path = f"s3a://{bucket_name}/topics/{topic}"
    return spark.read.format("delta").load(minio_path)


def process_scd2_dimension(
    spark: SparkSession, 
    silver_df: SparkDataFrame, 
    gold_bucket: str, 
    topic: str, 
    bk_col: str, 
    sk_col: str, 
    attribute_cols: list
) -> None:
    """
    Implements production-grade SCD Type 2 tracking using Delta Lake's native MERGE.
    This modifies data in-place without rewriting unaffected records.
    """
    from delta.tables import DeltaTable
    
    target_path = f"s3a://{gold_bucket}/topics/{topic}"
    high_date = "9999-12-31 23:59:59"

    # --- PHASE 1: BOOTSTRAP INITIAL LOADING ---
    if not DeltaTable.isDeltaTable(spark, target_path):
        print(f"Target dimension {topic} not found. Running initial bootstrap seeding...")
        initial_df = silver_df.select([bk_col] + attribute_cols + ["_ingested_at"]) \
            .withColumn(sk_col, F.md5(F.concat(F.col(bk_col).cast("string"), F.col("_ingested_at").cast("string")))) \
            .withColumn("valid_from_ts", F.col("_ingested_at")) \
            .withColumn("valid_to_ts", F.to_timestamp(F.lit(high_date))) \
            .withColumn("is_current", F.lit(True)) \
            .select([sk_col, bk_col] + attribute_cols + ["valid_from_ts", "valid_to_ts", "is_current"])
            
        initial_df.write.format("delta").mode("overwrite").save(target_path)
        print(f"Successfully initialized {topic}.")
        return

    # --- PHASE 2: TARGET & SOURCE PREPARATION ---
    target_table = DeltaTable.forPath(spark, target_path)
    change_condition = " OR ".join([f"COALESCE(src.{c}, '') != COALESCE(tgt.{c}, '')" for c in attribute_cols])

    # --- PHASE 3: SOURCE EXPANSION FOR MERGE MATRIX ---
    changed_bks_df = silver_df.alias("src").join(
        target_table.toDF().alias("tgt"),
        on=(F.col(f"src.{bk_col}") == F.col(f"tgt.{bk_col}")) & (F.col("tgt.is_current") == True)
    ).filter(F.expr(change_condition)).select(f"src.{bk_col}").distinct()
    
    changed_bks = [row[bk_col] for row in changed_bks_df.collect()]

    staged_updates = silver_df.filter(F.col(bk_col).isin(changed_bks)) \
        .withColumn("merge_key", F.lit(None).cast("string")) \
        .unionByName(
            silver_df.withColumn("merge_key", F.col(bk_col))
        )

    # --- PHASE 4: NATIVE ATOMIC DELTA LAKE MERGE ---
    print(f"Executing atomic Delta Lake SCD2 Merge mutation for {topic}...")
    
    target_table.alias("tgt").merge(
        source=staged_updates.alias("src"),  # Fixed the Pylance keyword argument issue here
        condition=f"tgt.{bk_col} = src.merge_key AND tgt.is_current = true"
    ).whenMatchedUpdate(
        condition=F.expr(change_condition),
        set={
            "valid_to_ts": "src._ingested_at",
            "is_current": "false"
        }
    ).whenNotMatchedInsert(
        values={
            sk_col: F.expr(f"md5(concat(cast(src.{bk_col} as string), cast(src._ingested_at as string)))"),
            bk_col: f"src.{bk_col}",
            **{c: f"src.{c}" for c in attribute_cols},
            "valid_from_ts": "src._ingested_at",
            "valid_to_ts": F.to_timestamp(F.lit(high_date)),
            "is_current": F.lit(True)
        }
    ).execute()

    print(f"SCD2 Merge operations successfully completed for {topic}.")

# =============================================================================
# PANDAS & NUMPY GENERATION FUNCTIONS (NATIVE DELTA WRITES)
# =============================================================================

def get_storage_options() -> dict:
    """Configures storage variables for native delta-rs interaction via S3 API."""
    return {
        "AWS_ENDPOINT_URL": f"http://{MINIO_ENDPOINT}",
        "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_PROVIDER_API_WINDOWS": "true"
    }


def generate_dim_date(base_date: pd.Timestamp, days_history: int, gold_bucket: str) -> None:
    """Generates and writes a date dimension using Pandas and NumPy backward from a anchor point."""
    print(f"Generating dim_date with Pandas: {days_history} days back from {base_date.strftime('%Y-%m-%d')}...")
    
    start_date = base_date - pd.Timedelta(days=days_history - 1)
    dates = pd.date_range(start=start_date, end=base_date, freq='D')
    
    df = pd.DataFrame({"calendar_date": dates})
    df["date_key"] = df["calendar_date"].dt.strftime("%Y%m%d").astype(np.int32)
    df["day_of_week"] = df["calendar_date"].dt.strftime("%a")
    df["month"] = df["calendar_date"].dt.month.astype(np.int32)
    df["year"] = df["calendar_date"].dt.year.astype(np.int32)
    df["is_weekend"] = df["day_of_week"].isin(["Sat", "Sun"])
    
    df["calendar_date"] = df["calendar_date"].dt.date
    
    target_path = f"s3a://{gold_bucket}/topics/dim_date"
    write_deltalake(target_path, df, mode="overwrite", storage_options=get_storage_options())
    print(f"Successfully wrote dim_date to {target_path} using Pandas.")


def generate_dim_time(gold_bucket: str) -> None:
    """Generates and writes second-by-second lookup data using Pandas, NumPy and global dimensions."""
    print("Generating dim_time with Pandas & NumPy globals...")
    
    total_seconds = HOURS_IN_DAY * MINUTES_IN_HOUR * SECONDS_IN_MINUTE
    seconds_array = np.arange(total_seconds)
    
    td_series = pd.to_timedelta(seconds_array, unit='s')
    base_time = pd.to_datetime("00:00:00", format="%H:%M:%S")
    time_series = base_time + td_series
    
    df = pd.DataFrame()
    # Added .dt accessor to fix pandas series extraction errors
    df["time_key"] = time_series.strftime("%H%M%S")
    df["time_of_day"] = time_series.strftime("%H:%M:%S")
    df["hour"] = time_series.hour.astype(np.int32)
    df["minute"] = time_series.minute.astype(np.int32)
    df["second"] = time_series.second.astype(np.int32)
    df["am_pm"] = time_series.strftime("%p")
    
    df["day_part"] = "Night"
    df.loc[(df["hour"] >= 5) & (df["hour"] < 12), "day_part"] = "Morning"
    df.loc[(df["hour"] >= 12) & (df["hour"] < 17), "day_part"] = "Afternoon"
    df.loc[(df["hour"] >= 17) & (df["hour"] < 21), "day_part"] = "Evening"
    
    target_path = f"s3a://{gold_bucket}/topics/dim_time"
    write_deltalake(target_path, df, mode="overwrite", storage_options=get_storage_options())
    print(f"Successfully wrote dim_time to {target_path} using Pandas.")


def generate_dim_rating(gold_bucket: str) -> None:
    """Generates and writes a rating lookup scale from levels 1 to 5 using Pandas."""
    print("Generating dim_rating with Pandas...")
    
    ratings = np.arange(1, 6, dtype=np.int32)
    df = pd.DataFrame({"rating": ratings})
    
    df["rating_key"] = df["rating"].apply(lambda x: hashlib.md5(str(x).encode('utf-8')).hexdigest())
    df = df[["rating_key", "rating"]]
    
    target_path = f"s3a://{gold_bucket}/topics/dim_rating"
    write_deltalake(target_path, df, mode="overwrite", storage_options=get_storage_options())
    print(f"Successfully wrote dim_rating to {target_path} using Pandas.")


def generate_dim_payment_method(gold_bucket: str) -> None:
    """Generates and writes a fixed value payment method lookup collection using Pandas."""
    print("Generating dim_payment_method with Pandas...")
    
    methods = ["Credit Card", "Debit Card", "Gift Card"]
    df = pd.DataFrame({"payment_method": methods})
    
    df["payment_method_key"] = df["payment_method"].apply(lambda x: hashlib.md5(str(x).encode('utf-8')).hexdigest())
    df = df[["payment_method_key", "payment_method"]]
    
    target_path = f"s3a://{gold_bucket}/topics/dim_payment_method"
    write_deltalake(target_path, df, mode="overwrite", storage_options=get_storage_options())
    print(f"Successfully wrote dim_payment_method to {target_path} using Pandas.")


if __name__ == "__main__":
    # ---- SPARK SESSION INITIALIZATION ----
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("generate_gold_dimensions")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
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

    # -------------------------------------------------------------------------
    # 1. SCD TYPE 2 DIMENSIONS
    # -------------------------------------------------------------------------
    
    # Process dim_user
    silver_users = extract_from_silver(spark, SILVER_BUCKET, "stg_users")
    process_scd2_dimension(
        spark=spark, silver_df=silver_users, gold_bucket=GOLD_BUCKET, topic="dim_user",
        bk_col="user_id", sk_col="user_key", attribute_cols=["gender", "age", "subscription_type", "signup_ts"]
    )

    # Process dim_movie (with necessary renaming mapped for schema evaluation)
    silver_movies = extract_from_silver(spark, SILVER_BUCKET, "stg_movies")
    process_scd2_dimension(
        spark=spark, silver_df=silver_movies, gold_bucket=GOLD_BUCKET, topic="dim_movie",
        bk_col="movie_id", sk_col="movie_key", attribute_cols=["genre", "country", "runtime_seconds", "language", "release_year", "created_at"]
    )

    # -------------------------------------------------------------------------
    # 2. PANDAS & NUMPY DRIVEN DIMENSIONS
    # -------------------------------------------------------------------------
    
    # Process dim_date
    generate_dim_date(base_date=pd.Timestamp("2026-06-07"), days_history=180, gold_bucket=GOLD_BUCKET)

    # Process dim_time
    generate_dim_time(gold_bucket=GOLD_BUCKET)

    # Process dim_rating
    generate_dim_rating(gold_bucket=GOLD_BUCKET)

    # Process dim_payment_method
    generate_dim_payment_method(gold_bucket=GOLD_BUCKET)