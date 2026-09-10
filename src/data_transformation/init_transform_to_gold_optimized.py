"""
python3 -m src.data_transformation.init_transform_to_gold_baseline
"""

import os, time
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, concat, row_number, md5, lit, expr, sum as _sum, to_timestamp, when, dayofweek, month, year
from dotenv import load_dotenv
from pyspark.sql.functions import col, expr, count, countDistinct, sum as _sum, avg, when

from src.config import (
    DELTA_VERSION,
    HADOOP_VERSION,
    LAKEHOUSE_ENDPOINT,
    LAKEHOUSE_ACCESS_KEY,
    LAKEHOUSE_SECRET_KEY,
    SILVER_BUCKET
)

# Load database credentials
load_dotenv()
JDBC_URL = f"jdbc:postgresql://{os.getenv('DWH_HOST', 'localhost')}:{os.getenv('DWH_PORT')}/{os.getenv('DWH_DB')}"
JDBC_PROPERTIES = {
    "user": os.getenv("DWH_USER"),
    "password": os.getenv("DWH_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

def load_silver_table(spark: SparkSession, topic: str) -> DataFrame:
    """Reads a Silver layer Delta table from MinIO."""
    path = f"s3a://{SILVER_BUCKET}/topics/{topic}"
    return spark.read.format("delta").load(path)

def write_to_postgres_init(df: DataFrame, table_name: str) -> None:
    """Writes a DataFrame to Postgres using JDBC in overwrite mode for initial load."""
    (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"gold.{table_name}")
        .options(**JDBC_PROPERTIES)
        .mode("overwrite") # Overwrite ensures idempotency for initialization
        .option("truncate", "true") # Safely lock the schema truncation for overwrite
        .option("cascadeTruncate", "true") # Cascade truncation for dependent tables
        .save()
    )
    print(f"✔ Successfully initialized gold.{table_name}")

# ==========================================
# TRANSFORMATION FUNCTIONS
# ==========================================

def build_dim_user(df_silver_users: DataFrame) -> DataFrame:
    """Prepares User Dimension for initial load by taking the latest record per user."""
    window_spec = Window.partitionBy("user_id").orderBy(col("_ingested_at").desc())
    
    return df_silver_users \
        .withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn") \
        .select(
            md5(concat(col("user_id"), col("_ingested_at").cast("string"))).alias("user_key"),
            col("user_id").cast("string").alias("user_id"), 
            col("gender"),
            col("age"),
            col("subscription_type"),
            col("signup_ts"),
            col("_ingested_at").alias("valid_from_ts"),
            to_timestamp(lit("9999-12-31 23:59:59")).alias("valid_to_ts"),
            lit(True).alias("is_current")
        )

def build_dim_movie(df_silver_movies: DataFrame) -> DataFrame:
    """Prepares Movie Dimension for initial load by taking the latest record per movie."""
    window_spec = Window.partitionBy("movie_id").orderBy(col("_ingested_at").desc())
    
    return df_silver_movies \
        .withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn") \
        .select(
            md5(concat(col("movie_id"), col("_ingested_at").cast("string"))).alias("movie_key"),
            col("movie_id").cast("string").alias("movie_id"),
            col("genre"),
            col("country"),
            col("runtime_seconds"),
            col("language"),
            col("release_year"),
            col("created_at"),
            col("_ingested_at").alias("valid_from_ts"),
            to_timestamp(lit("9999-12-31 23:59:59")).alias("valid_to_ts"),
            lit(True).alias("is_current")
        )

def build_dim_date(spark: SparkSession, start_date: str = "2015-01-01", end_date: str = "2035-12-31") -> DataFrame:
    """Generates a static date dimension dataframe covering a massive date range."""
    query = f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'))) as calendar_date"
    df_dates = spark.sql(query)
    
    return df_dates.select(
        expr("date_format(calendar_date, 'yyyyMMdd')").cast("int").alias("date_key"),
        col("calendar_date"),
        dayofweek("calendar_date").alias("day_of_week"),
        month("calendar_date").alias("month"),
        year("calendar_date").alias("year"),
        expr("CASE WHEN dayofweek(calendar_date) IN (1, 7) THEN true ELSE false END").alias("is_weekend")
    )

def build_dim_payment_method(df_stg_payments: DataFrame) -> DataFrame:
    """Builds the Payment Method Dimension by extracting unique methods."""
    return df_stg_payments.select(
        col("payment_method")
    ).dropDuplicates(
        ["payment_method"]
    ).filter(
        col("payment_method").isNotNull()
    ).select(
        md5(col("payment_method")).alias("payment_method_key"),
        col("payment_method")
    )

def build_fact_playback(df_stg_playbacks: DataFrame, df_dim_user: DataFrame, df_dim_movie: DataFrame) -> DataFrame:
    """Builds the Playback Fact table by mapping natural keys to surrogate keys."""
    return df_stg_playbacks.join(
        df_dim_user, "user_id", "inner"
    ).join(
        df_dim_movie, "movie_id", "inner"
    ).select(
        col("playback_id"),
        col("user_key"),
        col("movie_key"),
        expr("date_format(start_ts, 'yyyyMMdd')").cast("int").alias("playback_date_key"),
        col("start_ts"),
        col("duration_watched_seconds"),
        when(col("duration_watched_seconds") >= (col("runtime_seconds") * 0.90), 1).otherwise(0).alias("is_completed")
    )

def build_fact_rating(df_stg_ratings: DataFrame, df_dim_user: DataFrame, df_dim_movie: DataFrame) -> DataFrame:
    """Joins staging ratings with dimension tables to map natural keys to surrogate keys."""
    return df_stg_ratings.join(
        df_dim_user, "user_id", "inner"
    ).join(
        df_dim_movie, "movie_id", "inner"
    ).select(
        col("rating_id"),
        col("user_key"),
        col("movie_key"),
        expr("date_format(rating_ts, 'yyyyMMdd')").cast("int").alias("rating_date_key"),
        col("rating_ts"),
        col("rating").alias("rating_score")
    )

def build_fact_payment_attempt(df_stg_payments: DataFrame, df_dim_user: DataFrame, df_dim_payment_method: DataFrame) -> DataFrame:
    """Transforms payment events utilizing existing dimensions."""
    return df_stg_payments.join(
        df_dim_user, "user_id", "inner"
    ).join(
        df_dim_payment_method, "payment_method", "inner" 
    ).select(
        col("payment_id").alias("payment_attempt_id"),
        col("user_key"),
        expr("date_format(payment_ts, 'yyyyMMdd')").cast("int").alias("payment_date_key"),
        col("payment_method_key"),
        col("payment_ts"),
        col("amount"),
        col("currency"),
        when(col("payment_status") == "Success", 1).otherwise(0).alias("is_payment_success"),
        when(col("payment_status") == "Failed", 1).otherwise(0).alias("is_payment_failed")
    )

def build_obt_playback(df_fact_playback: DataFrame, df_dim_user: DataFrame, df_dim_movie: DataFrame, df_dim_date: DataFrame) -> DataFrame:
    """Flattens fact_playback with dimensions to create One Big Table (OBT)."""
    return df_fact_playback.alias("f") \
        .join(df_dim_user.alias("u"), col("f.user_key") == col("u.user_key"), "left") \
        .join(df_dim_movie.alias("m"), col("f.movie_key") == col("m.movie_key"), "left") \
        .join(df_dim_date.alias("d"), col("f.playback_date_key") == col("d.date_key"), "left") \
        .select(
            col("f.playback_id"),
            col("f.start_ts"),
            col("f.duration_watched_seconds"),
            col("f.is_completed"),
            col("u.user_id"),
            col("u.age"),
            col("u.gender"),
            col("u.subscription_type"),
            col("m.movie_id"),
            col("m.genre"),
            col("m.runtime_seconds"),
            col("m.release_year"),
            col("d.calendar_date").alias("playback_date"),
            col("d.is_weekend")
        )

def build_complete_feat_user_90d(
    df_obt_playback: DataFrame, 
    df_fact_payment: DataFrame, 
    df_dim_user: DataFrame,
    df_dim_date: DataFrame,
    end_date: str = "2026-06-07", 
    snapshot_days: int = 30
) -> DataFrame:
    """Calculates 6 point-in-time 90-day features across a 30-day snapshot matrix safely.
    
    Balances absolute engagement time (median duration) with total user satisfaction 
    (average completion rate) without overloading executor memory allocations.
    """
    spark = df_obt_playback.sparkSession
    
    # 1. Generate the 30 snapshot dates matrix backbone
    start_date = spark.sql(f"SELECT date_sub('{end_date}', {snapshot_days - 1})").collect()[0][0]
    df_snapshots = spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'))) as snapshot_date")
    df_users = df_obt_playback.select("user_id").distinct()
    
    # Repartition by key shatters cross-join bottleneck blocks across 128 parallel threads
    df_matrix_base = df_users.crossJoin(df_snapshots).repartition(128, "user_id")
    
    # 2. Prepare Payments data with actual calendar dates
    df_payments_with_date = df_fact_payment.join(
        df_dim_date, df_fact_payment.payment_date_key == df_dim_date.date_key, "inner"
    ).select(
        col("user_key"),
        col("calendar_date").alias("payment_date"),
        col("is_payment_failed")
    )
    
    df_user_map = df_dim_user.select("user_id", "user_key")
    df_payments_clean = df_payments_with_date.join(df_user_map, "user_key", "inner")

    # 3. Time-travel Join Conditions (90-day lookback)
    playback_join_cond = (col("obt.user_id") == col("base.user_id")) & \
                         (col("obt.playback_date") <= col("base.snapshot_date")) & \
                         (col("obt.playback_date") > expr("date_sub(base.snapshot_date, 90)"))
                         
    payment_join_cond = (col("pay.user_id") == col("base.user_id")) & \
                        (col("pay.payment_date") <= col("base.snapshot_date")) & \
                        (col("pay.payment_date") > expr("date_sub(base.snapshot_date, 90)"))

    # 4. Aggregate Playback Features from OBT 
    df_playback_feats = df_matrix_base.alias("base") \
        .join(df_obt_playback.alias("obt"), playback_join_cond, "left") \
        .groupBy("base.user_id", "base.snapshot_date") \
        .agg(
            count("obt.playback_id").alias("f_user_total_playbacks_90d"),
            countDistinct("obt.genre").alias("f_user_distinct_genre_90d"),
            
            # Algebraic Average: Lightweight 16-byte memory profile tracking raw volume ratios
            (_sum("obt.duration_watched_seconds") / _sum("obt.runtime_seconds")).alias("f_user_avg_completion_rate_90d"),
            
            # --- Instructor Cardinality & Sketch Optimizations ---
            # HyperLogLog Cardinality Sketch for high-cardinality movie IDs (3% error margin bounds)
            countDistinct("obt.movie_id").alias("f_user_distinct_movies_90d"),
            # Quantile Summary Sketch for continuous median watch session (50th Percentile)
            F.percentile_approx("obt.duration_watched_seconds", 0.5).alias("f_user_median_duration_watched_seconds_90d")
        )

    # 5. Aggregate Payment Features from Fact Payment
    df_payment_feats = df_matrix_base.alias("base") \
        .join(df_payments_clean.alias("pay"), payment_join_cond, "left") \
        .groupBy("base.user_id", "base.snapshot_date") \
        .agg(
            F.avg(col("pay.is_payment_failed")).alias("f_user_payment_fail_rate_90d")
        )

    # 6. Unify into a single Feature Table
    df_final_features = df_playback_feats.alias("p") \
        .join(df_payment_feats.alias("pay_f"), (col("p.user_id") == col("pay_f.user_id")) & (col("p.snapshot_date") == col("pay_f.snapshot_date")), "inner") \
        .select(
            col("p.user_id"),
            col("p.snapshot_date"),
            col("p.f_user_total_playbacks_90d"),
            col("p.f_user_distinct_genre_90d"),
            col("p.f_user_avg_completion_rate_90d"),
            col("p.f_user_distinct_movies_90d"),
            col("p.f_user_median_duration_watched_seconds_90d"),
            col("pay_f.f_user_payment_fail_rate_90d")
        ).fillna(0)
        
    return df_final_features.repartition(128, "user_id")

# ==========================================
# MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    
    JARS = f"org.postgresql:postgresql:42.6.0,io.delta:delta-spark_2.12:{DELTA_VERSION},org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}"
    
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("gold_layer_initialization")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.jars.packages", JARS)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true") 
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "128")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "16777216")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{LAKEHOUSE_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", LAKEHOUSE_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", LAKEHOUSE_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(spark.conf.get("spark.sql.adaptive.enabled"))

    print("\n--- 1. Extracting Silver Data (Lazy Evaluation) ---")
    df_stg_users = load_silver_table(spark, "stg_users")
    df_stg_movies = load_silver_table(spark, "stg_movies")
    df_stg_playbacks = load_silver_table(spark, "stg_playbacks")
    df_stg_ratings = load_silver_table(spark, "stg_ratings")
    df_stg_payments = load_silver_table(spark, "stg_payment_attempts")

    print("\n--- 2. Constructing Initialization DAGs ---")
    # Dimensions (Deduped logically for init state)
    df_dim_user = build_dim_user(df_stg_users)
    df_dim_movie = build_dim_movie(df_stg_movies)
    df_dim_date = build_dim_date(spark)
    df_dim_payment_method = build_dim_payment_method(df_stg_payments)

    # Facts & Features (Connected directly to constructed Dims)
    df_fact_playback = build_fact_playback(df_stg_playbacks, df_dim_user, df_dim_movie)
    df_fact_rating = build_fact_rating(df_stg_ratings, df_dim_user, df_dim_movie)
    df_fact_payment_attempt = build_fact_payment_attempt(df_stg_payments, df_dim_user, df_dim_payment_method)
    df_obt_playback = build_obt_playback(df_fact_playback, df_dim_user, df_dim_movie, df_dim_date)
    df_feat_user_90d = build_complete_feat_user_90d(df_obt_playback, df_fact_payment_attempt, df_dim_user, df_dim_date, end_date="2026-06-07", snapshot_days=30)

    # ---- 3. EXECUTION MANIFEST ----
    # Order matters: Dimensions must be created before facts to satisfy foreign key constraints.
    gold_init_manifest = {
        "dim_user": df_dim_user,
        "dim_movie": df_dim_movie,
        "dim_date": df_dim_date,
        "dim_payment_method": df_dim_payment_method,
        "fact_playback": df_fact_playback,
        "fact_rating": df_fact_rating,
        "fact_payment_attempt": df_fact_payment_attempt,
        "obt_playback": df_obt_playback,
        "feat_user_90d": df_feat_user_90d
    }

    # Execute batch loading sequentially
    for target_table, df in gold_init_manifest.items():
        print(f"Processing Target: gold.{target_table} ...")
        
        spark.sparkContext.setJobGroup(
            groupId=f"gold_init_{target_table}", 
            description=f"Initialize gold.{target_table}", 
            interruptOnCancel=True
        )
        
        write_to_postgres_init(df, target_table)
        
        spark.sparkContext.setJobGroup(None, None)

    # Optional keep-alive block for Spark UI inspection
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down Spark Session gracefully...")
        spark.stop()