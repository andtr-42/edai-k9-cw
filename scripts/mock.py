import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from dotenv import load_dotenv

load_dotenv()
from src.config import DELTA_VERSION, HADOOP_VERSION, LAKEHOUSE_ENDPOINT, LAKEHOUSE_ACCESS_KEY, LAKEHOUSE_SECRET_KEY, SILVER_BUCKET

if __name__ == "__main__":
    print("⏳ Initializing Spark Session for Mock Day 2 Data Injection...")
    
    JARS = f"io.delta:delta-spark_2.12:{DELTA_VERSION},org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}"
    
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("mock_day2_injector") \
        .config("spark.jars.packages", JARS) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{LAKEHOUSE_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", LAKEHOUSE_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", LAKEHOUSE_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    print("\n--- 🚀 Generating Day 2 Mock Data (2026-06-08) ---")

    # =========================================================================
    # 1. MOCK DIMENSION UPDATE: User 1 Upgrades to Premium
    # =========================================================================
    user_schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("gender", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("subscription_type", StringType(), True),
        StructField("signup_ts", TimestampType(), True),
        StructField("_ingested_at", TimestampType(), False)
    ])
    
    day2_users = [
        # User 1 upgrades their tier on Day 2
        ("1", "Female", 28, "Premium", datetime(2026, 1, 15, 10, 0, 0), datetime(2026, 6, 8, 10, 0, 0)),
        # Brand new User 3 registers on Day 2
        ("3", "Male", 34, "Basic", datetime(2026, 6, 8, 8, 30, 0), datetime(2026, 6, 8, 8, 30, 0))
    ]
    df_users = spark.createDataFrame(day2_users, schema=user_schema)
    users_path = f"s3a://{SILVER_BUCKET}/topics/stg_users"
    df_users.write.format("delta").mode("append").save(users_path)
    print(f"✔ Appended {df_users.count()} records to Silver stg_users")

    # =========================================================================
    # 2. MOCK FACT EVENT: User 1 watches a movie at 14:00 (Must tie to Premium status)
    # =========================================================================
    playback_schema = StructType([
        StructField("playback_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("movie_id", StringType(), False),
        StructField("start_ts", TimestampType(), False),
        StructField("duration_watched_seconds", IntegerType(), True)
    ])
    
    day2_playbacks = [
        # User 1 watches Movie 100 at 14:00 (SCD2 should tie this to the new Premium state)
        ("play_mock_day2", "1", "100", datetime(2026, 6, 8, 14, 0, 0), 4500)
    ]
    df_playbacks = spark.createDataFrame(day2_playbacks, schema=playback_schema)
    playbacks_path = f"s3a://{SILVER_BUCKET}/topics/stg_playbacks"
    df_playbacks.write.format("delta").mode("append").save(playbacks_path)
    print(f"✔ Appended {df_playbacks.count()} records to Silver stg_playbacks")

    # =========================================================================
    # 3. MOCK PAYMENT EVENT: User 1 runs into a transaction failure
    # =========================================================================
    payment_schema = StructType([
        StructField("payment_attempt_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("payment_method", StringType(), False),
        StructField("payment_ts", TimestampType(), False),
        StructField("amount", IntegerType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), False)
    ])
    
    day2_payments = [
        # User 1 experiences a billing failure trying to pay for Premium tier
        ("pay_mock_day2", "1", "Credit Card", datetime(2026, 6, 8, 10, 5, 0), 15, "USD", "FAILED")
    ]
    df_payments = spark.createDataFrame(day2_payments, schema=payment_schema)
    payments_path = f"s3a://{SILVER_BUCKET}/topics/stg_payments"
    df_payments.write.format("delta").mode("append").save(payments_path)
    print(f"✔ Appended {df_payments.count()} records to Silver stg_payments")

    print("\n🚀 Mock Data Simulation injected into Silver Layer successfully!")
    spark.stop()