"""
python3 -m src.data_transformation.increment_transform_to_gold_baseline
"""

import os
import psycopg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, expr, when
from pyspark.sql.functions import col, expr, when, sum as _sum, count as _count, when, coalesce
from pyspark.sql.functions import col, lit, expr, when, count as _count, sum as _sum, count_distinct
from pyspark.sql import Window
from dotenv import load_dotenv

# 1. REUSE: Import the exact logic from your init script!
from src.data_transformation.init_transform_to_gold_baseline import (
    load_silver_table,
    build_dim_user,
    build_dim_movie,
    build_dim_payment_method
)
from src.config import DELTA_VERSION, HADOOP_VERSION, LAKEHOUSE_ENDPOINT, LAKEHOUSE_ACCESS_KEY, LAKEHOUSE_SECRET_KEY, SILVER_BUCKET

# Load database credentials
load_dotenv()
JDBC_URL = f"jdbc:postgresql://{os.getenv('DWH_HOST', 'localhost')}:{os.getenv('DWH_PORT')}/{os.getenv('DWH_DB')}"
JDBC_PROPERTIES = {
    "user": os.getenv("DWH_USER"),
    "password": os.getenv("DWH_PASSWORD"),
    "driver": "org.postgresql.Driver"
}
DB_CONFIG = {
    "host": os.getenv('DWH_HOST', 'localhost'),
    "port": os.getenv('DWH_PORT'),
    "dbname": os.getenv('DWH_DB'),
    "user": os.getenv("DWH_USER"),
    "password": os.getenv("DWH_PASSWORD")
}

def write_to_postgres(df: DataFrame, table_name: str, mode: str) -> None:
    """Writes to Postgres. mode='overwrite' for staging, mode='append' for facts."""
    (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"gold.{table_name}")
        .options(**JDBC_PROPERTIES)
        .mode(mode)
        .save()
    )
    print(f"✔ Successfully wrote to gold.{table_name} via {mode}")

# ==========================================
# INCREMENTAL PIPELINE FUNCTIONS
# ==========================================

def get_high_water_mark(db_config: dict, table_name: str, time_column: str) -> str:
    """Queries Postgres to find the most recent timestamp processed."""
    try:
        with psycopg.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COALESCE(MAX({time_column}), '1900-01-01') FROM gold.{table_name};")
                result = cursor.fetchone()[0]
                return str(result)
    except Exception as e:
        print(f"Failed to get High-Water Mark for {table_name}: {e}")
        # Default to a very old date if the table is empty
        return '1900-01-01'

def execute_user_scd2_merge(db_config: dict):
    """Executes the SCD2 logic for Users directly inside PostgreSQL."""
    with psycopg.connect(**db_config) as conn:
        with conn.cursor() as cursor:
            # Expire old records
            cursor.execute("""
            UPDATE gold.dim_user target
            SET valid_to_ts = staging.valid_from_ts,
                is_current = false
            FROM gold.stg_user_update staging
            WHERE target.user_id = staging.user_id
              AND target.is_current = true
              AND (target.gender IS DISTINCT FROM staging.gender OR 
                   target.age IS DISTINCT FROM staging.age OR 
                   target.subscription_type IS DISTINCT FROM staging.subscription_type);
            """)
            # Insert new records
            cursor.execute("""
            INSERT INTO gold.dim_user (user_key, user_id, gender, age, subscription_type, signup_ts, valid_from_ts, valid_to_ts, is_current)
            SELECT 
                staging.user_key, staging.user_id, staging.gender, staging.age, 
                staging.subscription_type, staging.signup_ts, staging.valid_from_ts, 
                staging.valid_to_ts, staging.is_current
            FROM gold.stg_user_update staging
            LEFT JOIN gold.dim_user target 
              ON staging.user_id = target.user_id AND target.is_current = true
            WHERE target.user_id IS NULL;
            """)

def execute_movie_scd2_merge(db_config: dict):
    """Executes the SCD2 logic for Movies directly inside PostgreSQL."""
    with psycopg.connect(**db_config) as conn:
        with conn.cursor() as cursor:
            # Expire
            cursor.execute("""
            UPDATE gold.dim_movie target
            SET valid_to_ts = staging.valid_from_ts,
                is_current = false
            FROM gold.stg_movie_update staging
            WHERE target.movie_id = staging.movie_id
              AND target.is_current = true
              AND (target.genre IS DISTINCT FROM staging.genre OR 
                   target.country IS DISTINCT FROM staging.country OR
                   target.runtime_seconds IS DISTINCT FROM staging.runtime_seconds OR
                   target.language IS DISTINCT FROM staging.language);
            """)
            # Insert
            cursor.execute("""
            INSERT INTO gold.dim_movie (movie_key, movie_id, genre, country, runtime_seconds, language, release_year, created_at, valid_from_ts, valid_to_ts, is_current)
            SELECT 
                staging.movie_key, staging.movie_id, staging.genre, staging.country, 
                staging.runtime_seconds, staging.language, staging.release_year, staging.created_at, staging.valid_from_ts, 
                staging.valid_to_ts, staging.is_current
            FROM gold.stg_movie_update staging
            LEFT JOIN gold.dim_movie target 
              ON staging.movie_id = target.movie_id AND target.is_current = true
            WHERE target.movie_id IS NULL;
            """)

def build_incremental_fact_playback(df_stg_playbacks: DataFrame, df_dim_user: DataFrame, df_dim_movie: DataFrame) -> DataFrame:
    """Builds the Playback Fact table using a Point-in-Time join to ensure historical accuracy."""
    
    # 1. Point-in-Time Join with Users
    df_joined_user = df_stg_playbacks.join(
        df_dim_user,
        (df_stg_playbacks.user_id == df_dim_user.user_id) & 
        (df_stg_playbacks.start_ts >= df_dim_user.valid_from_ts) & 
        (df_stg_playbacks.start_ts < df_dim_user.valid_to_ts),
        "inner"
    ).drop(df_dim_user.user_id) # Drop the extra user_id column after join
    
    # 2. Point-in-Time Join with Movies
    df_joined_movie = df_joined_user.join(
        df_dim_movie,
        (df_joined_user.movie_id == df_dim_movie.movie_id) & 
        (df_joined_user.start_ts >= df_dim_movie.valid_from_ts) & 
        (df_joined_user.start_ts < df_dim_movie.valid_to_ts),
        "inner"
    ).drop(df_dim_movie.movie_id)

    # 3. Select final columns
    return df_joined_movie.select(
        col("playback_id"),
        col("user_key"),
        col("movie_key"),
        expr("date_format(start_ts, 'yyyyMMdd')").cast("int").alias("playback_date_key"),
        col("start_ts"),
        col("duration_watched_seconds"),
        when(col("duration_watched_seconds") >= (col("runtime_seconds") * 0.90), 1).otherwise(0).alias("is_completed")
    )

def build_incremental_fact_rating(df_stg_ratings: DataFrame, df_dim_user: DataFrame, df_dim_movie: DataFrame) -> DataFrame:
    """Builds the Rating Fact table using a Point-in-Time join."""
    return df_stg_ratings.alias("r") \
        .join(df_dim_user.alias("u"), 
              (col("r.user_id") == col("u.user_id")) & 
              (col("r.rating_ts") >= col("u.valid_from_ts")) & 
              (col("r.rating_ts") < col("u.valid_to_ts")), "inner") \
        .join(df_dim_movie.alias("m"), 
              (col("r.movie_id") == col("m.movie_id")) & 
              (col("r.rating_ts") >= col("m.valid_from_ts")) & 
              (col("r.rating_ts") < col("m.valid_to_ts")), "inner") \
        .select(
            col("r.rating_id"),
            col("u.user_key"),
            col("m.movie_key"),
            expr("date_format(r.rating_ts, 'yyyyMMdd')").cast("int").alias("rating_date_key"),
            col("r.rating_ts"),
            col("r.rating_score")
        )

def build_incremental_fact_payment(df_stg_payments: DataFrame, df_dim_user: DataFrame, df_dim_payment: DataFrame) -> DataFrame:
    """Builds the Payment Fact table using a Point-in-Time join."""
    return df_stg_payments.alias("p") \
        .join(df_dim_user.alias("u"), 
              (col("p.user_id") == col("u.user_id")) & 
              (col("p.payment_ts") >= col("u.valid_from_ts")) & 
              (col("p.payment_ts") < col("u.valid_to_ts")), "inner") \
        .join(df_dim_payment.alias("pm"), 
              col("p.payment_method") == col("pm.payment_method"), "inner") \
        .select(
            col("p.payment_attempt_id"),
            col("u.user_key"),
            expr("date_format(p.payment_ts, 'yyyyMMdd')").cast("int").alias("payment_date_key"),
            col("pm.payment_method_key"),
            col("p.payment_ts"),
            col("p.amount"),
            col("p.currency"),
            when(col("p.status") == 'SUCCESS', 1).otherwise(0).alias("is_payment_success"),
            when(col("p.status") == 'FAILED', 1).otherwise(0).alias("is_payment_failed")
        )

def build_incremental_obt(df_fact_playback_new: DataFrame, df_dim_user: DataFrame, df_dim_movie: DataFrame) -> DataFrame:
    """Denormalizes new playbacks into the OBT."""
    return df_fact_playback_new.alias("f") \
        .join(df_dim_user.alias("u"), col("f.user_key") == col("u.user_key"), "inner") \
        .join(df_dim_movie.alias("m"), col("f.movie_key") == col("m.movie_key"), "inner") \
        .select(
            col("f.playback_id"),
            col("f.playback_date_key"),
            col("u.user_id"),
            col("u.subscription_type"),
            col("u.age"),
            col("m.movie_id"),
            col("m.genre"),
            col("m.runtime_seconds"),
            col("f.duration_watched_seconds"),
            col("f.is_completed")
        )


def build_incremental_features_90d(
    spark: SparkSession, 
    jdbc_url: str, 
    jdbc_properties: dict, 
    snapshot_date_str: str  # Format: "2026-06-08"
) -> DataFrame:
    """
    Directly pulls a 90-day lookback window from raw database facts 
    to build the feature state for a single target execution date.
    """
    snapshot_key = int(snapshot_date_str.replace("-", ""))
    
    # 1. Define Lookback Boundary
    start_lookback_expr = f"'{snapshot_date_str}'::DATE - INTERVAL '89 days'"
    end_lookback_expr = f"'{snapshot_date_str}'::DATE"
    
    # 2. Read Fact data directly inside the 90-day boundary windows
    df_playback_90d = spark.read.format("jdbc").option("url", jdbc_url) \
        .option("dbtable", f"(SELECT user_key, movie_key, duration_watched_seconds FROM gold.fact_playback WHERE start_ts::DATE BETWEEN {start_lookback_expr} AND {end_lookback_expr}) AS p_90d") \
        .options(**jdbc_properties).load()
        
    df_payment_90d = spark.read.format("jdbc").option("url", jdbc_url) \
        .option("dbtable", f"(SELECT user_key, is_payment_failed FROM gold.fact_payment_attempt WHERE payment_date_key BETWEEN date_format({start_lookback_expr}, 'yyyyMMdd')::int AND date_format({end_lookback_expr}, 'yyyyMMdd')::int) AS pay_90d") \
        .options(**jdbc_properties).load()
        
    df_dim_movie = spark.read.format("jdbc").option("url", jdbc_url) \
        .option("dbtable", "gold.dim_movie").options(**jdbc_properties).load()

    # 3. Aggregate Playback Features
    df_playback_feats = df_playback_90d.alias("p") \
        .join(df_dim_movie.alias("m"), "movie_key", "inner") \
        .groupBy("user_key") \
        .agg(
            _count("p.duration_watched_seconds").alias("f_user_total_playbacks_90d"),
            count_distinct("m.genre").alias("f_user_distinct_genre_90d"),
            _sum("p.duration_watched_seconds").alias("total_duration"),
            _sum("m.runtime_seconds").alias("total_runtime")
        ) \
        .withColumn(
            "f_user_avg_duration_watched_seconds_90d", 
            when(col("f_user_total_playbacks_90d") > 0, col("total_duration") / col("f_user_total_playbacks_90d")).otherwise(0.0)
        ) \
        .withColumn(
            "f_user_avg_completion_rate_90d", 
            when(col("total_runtime") > 0, col("total_duration") / col("total_runtime")).otherwise(0.0)
        )

    # 4. Aggregate Payment Features
    df_payment_feats = df_payment_90d \
        .groupBy("user_key") \
        .agg(
            _sum("is_payment_failed").alias("failed_count"),
            _count("is_payment_failed").alias("total_attempts")
        ) \
        .withColumn(
            "f_user_payment_fail_rate_90d", 
            when(col("total_attempts") > 0, col("failed_count") / col("total_attempts")).otherwise(0.0)
        )

    # 5. Full Outer Join on User to construct final layout
    df_final_features = df_playback_feats.join(df_payment_feats, "user_key", "outer") \
        .select(
            col("user_key"),
            lit(snapshot_key).alias("snapshot_date_key"),
            coalesce(col("f_user_total_playbacks_90d").cast("int"), lit(0)).alias("f_user_total_playbacks_90d"),
            coalesce(col("f_user_distinct_genre_90d").cast("int"), lit(0)).alias("f_user_distinct_genre_90d"),
            coalesce(col("f_user_avg_duration_watched_seconds_90d").cast("double"), lit(0.0)).alias("f_user_avg_duration_watched_seconds_90d"),
            coalesce(col("f_user_payment_fail_rate_90d").cast("double"), lit(0.0)).alias("f_user_payment_fail_rate_90d"),
            coalesce(col("f_user_avg_completion_rate_90d").cast("double"), lit(0.0)).alias("f_user_avg_completion_rate_90d")
        )

    return df_final_features

# ==========================================
# MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    
    JARS = f"org.postgresql:postgresql:42.6.0,io.delta:delta-spark_2.12:{DELTA_VERSION},org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}"
    
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("gold_layer_incremental") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.jars.packages", JARS) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{LAKEHOUSE_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", LAKEHOUSE_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", LAKEHOUSE_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    print("\n--- 1. Extracting Incremental Silver Data ---")
    
    # Get High-Water marks (Last time we ingested records)
    last_user_update = get_high_water_mark(DB_CONFIG, "dim_user", "valid_from_ts")
    last_movie_update = get_high_water_mark(DB_CONFIG, "dim_movie", "valid_from_ts")
    last_playback = get_high_water_mark(DB_CONFIG, "fact_playback", "start_ts")
    last_rating = get_high_water_mark(DB_CONFIG, "fact_rating", "rating_ts")
    last_payment = get_high_water_mark(DB_CONFIG, "fact_payment_attempt", "payment_ts") # Assuming payment_ts exists in stg

    # Load and filter Silver data based on the bookmark
    df_stg_users_new = load_silver_table(spark, "stg_users").filter(col("_ingested_at") > last_user_update)
    df_stg_movies_new = load_silver_table(spark, "stg_movies").filter(col("_ingested_at") > last_movie_update)
    df_stg_playbacks_new = load_silver_table(spark, "stg_playbacks").filter(col("start_ts") > last_playback)
    df_stg_ratings_new = load_silver_table(spark, "stg_ratings").filter(col("rating_ts") > last_rating)
    df_stg_payments_new = load_silver_table(spark, "stg_payment_attempts").filter(col("payment_ts") > last_payment)

    # --- PHASE 2: Process Slowly Changing Dimensions ---
    print("\n--- 2. Updating Dimensions via Staging ---")
    
    # Dimensions require 'overwrite' to staging tables and immediate SQL execution, 
    # so they are handled outside the append manifest loop.
    if df_stg_users_new.count() > 0:
        df_stg_user_update = build_dim_user(df_stg_users_new)
        write_to_postgres(df_stg_user_update, "stg_user_update", "overwrite")
        execute_user_scd2_merge(DB_CONFIG)
        print("✔ User SCD2 Merge Complete.")

    if df_stg_movies_new.count() > 0:
        df_stg_movie_update = build_dim_movie(df_stg_movies_new)
        write_to_postgres(df_stg_movie_update, "stg_movie_update", "overwrite")
        execute_movie_scd2_merge(DB_CONFIG)
        print("✔ Movie SCD2 Merge Complete.")

    # --- PHASE 3: Prepare Incremental DataFrames (Facts & OBT) ---
    print("\n--- 3. Generating Incremental Facts and OBT ---")
    
    # Read the newly updated Dimensions back into Spark for Point-in-Time Joins
    df_dim_user_current = spark.read.format("jdbc").option("url", JDBC_URL).option("dbtable", "gold.dim_user").options(**JDBC_PROPERTIES).load()
    df_dim_movie_current = spark.read.format("jdbc").option("url", JDBC_URL).option("dbtable", "gold.dim_movie").options(**JDBC_PROPERTIES).load()
    df_dim_payment_current = spark.read.format("jdbc").option("url", JDBC_URL).option("dbtable", "gold.dim_payment_method").options(**JDBC_PROPERTIES).load()

    # Dictionary to hold all tables that require an 'append' write
    gold_increment_manifest = {}

    if df_stg_playbacks_new.count() > 0:
        df_fact_playback_new = build_incremental_fact_playback(df_stg_playbacks_new, df_dim_user_current, df_dim_movie_current)
        gold_increment_manifest["fact_playback"] = df_fact_playback_new
        gold_increment_manifest["obt_playback"] = build_incremental_obt(df_fact_playback_new, df_dim_user_current, df_dim_movie_current)
        
    if df_stg_ratings_new.count() > 0:
        gold_increment_manifest["fact_rating"] = build_incremental_fact_rating(df_stg_ratings_new, df_dim_user_current, df_dim_movie_current)

    if df_stg_payments_new.count() > 0:
        gold_increment_manifest["fact_payment_attempt"] = build_incremental_fact_payment(df_stg_payments_new, df_dim_user_current, df_dim_payment_current)

    # --- PHASE 4: Execute Batch Append Loading Sequentially ---
    for target_table, df in gold_increment_manifest.items():
        print(f"Processing Target: gold.{target_table} ...")
        
        spark.sparkContext.setJobGroup(
            groupId=f"gold_inc_{target_table}", 
            description=f"Increment gold.{target_table}", 
            interruptOnCancel=True
        )
        
        write_to_postgres(df, target_table, "append")
        spark.sparkContext.setJobGroup(None, None)

    # --- PHASE 5: Compute and Append 90-Day Rolling Features ---
    # Only calculate features if there is new transaction data affecting the metrics
    if "fact_playback" in gold_increment_manifest or "fact_payment_attempt" in gold_increment_manifest:
        print("\n--- 5. Computing 90-Day Rolling Snapshot for Features ---")
        
        # Determine the target execution date (e.g., dynamically pull the max date from the incoming playback batch)
        # Assuming you extract this dynamically or pass it as an argument:
        TARGET_SNAPSHOT_DATE = "2026-06-08" 
        
        df_incremental_features = build_incremental_features_90d(
            spark=spark,
            jdbc_url=JDBC_URL,
            jdbc_properties=JDBC_PROPERTIES,
            snapshot_date_str=TARGET_SNAPSHOT_DATE
        )
        
        spark.sparkContext.setJobGroup(
            groupId="gold_inc_feat_user", 
            description="Increment gold.feat_user", 
            interruptOnCancel=True
        )
        write_to_postgres(df_incremental_features, "feat_user", "append")
        spark.sparkContext.setJobGroup(None, None)
    else:
        print("\nNo new playback or payment data detected; skipping 90-day feature computation for now.")

    print("\n🚀 Incremental Run Complete!")
    spark.stop()