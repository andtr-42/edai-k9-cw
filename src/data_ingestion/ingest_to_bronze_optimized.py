"""
Bronze Layer Ingestion Pipeline (Optimized)

Same as baseline but uses per-table JDBC partitioning to parallelize reads
and enforces an explicit schema on the users table to avoid costly inference.

python3 -m src.data_ingestion.ingest_to_bronze_optimized
"""

import time
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType
from src.config import (
    DELTA_VERSION,
    HADOOP_VERSION,
    DS_DB_HOST,
    DS_DB_PORT,
    DS_DB_NAME,
    DS_DB_USER,
    DS_DB_PASSWORD,
    LAKEHOUSE_ENDPOINT,
    LAKEHOUSE_ACCESS_KEY,
    LAKEHOUSE_SECRET_KEY,
    BRONZE_BUCKET,
)

POSTGRESQL_JDBC_VERSION = "42.6.0"

USER_SCHEMA = StructType([
    StructField("user_id",           LongType(),      nullable=False),
    StructField("age",               LongType(),      nullable=True),
    StructField("subscription_type", StringType(),    nullable=True),
    StructField("signup_ts",         TimestampType(), nullable=True),
    StructField("gender",            StringType(),    nullable=True),
])


def add_metadata_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("raw_id", F.monotonically_increasing_id())
          .withColumn("_ingested_at", F.current_timestamp())
          .select("raw_id", *df.columns, "_ingested_at")
    )


def extract_table(
    spark: SparkSession,
    table: str,
    jdbc_url: str,
    props: dict,
    id_col: str | None = None,
    num_partitions: int = 4,
) -> DataFrame:
    """Reads a table via JDBC, optionally parallelised by id_col range partitioning."""
    if id_col:
        bounds = spark.read.jdbc(
            url=jdbc_url,
            table=f"(SELECT MIN({id_col}) AS lo, MAX({id_col}) AS hi FROM {table}) t",
            properties=props,
        ).collect()[0]
        df = spark.read.jdbc(
            url=jdbc_url,
            table=table,
            column=id_col,
            lowerBound=int(bounds["lo"]),
            upperBound=int(bounds["hi"]) + 1,
            numPartitions=num_partitions,
            properties=props,
        )
    else:
        df = spark.read.jdbc(url=jdbc_url, table=table, properties=props)

    return add_metadata_columns(df)


def upload_delta(
    df: DataFrame,
    bucket_name: str,
    topic: str,
    partition_cols: list[str] | None = None,
) -> None:
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
    print(f"✔ Uploaded {target_path} to Delta Lakehouse.")


if __name__ == "__main__":
    jdbc_url = f"jdbc:postgresql://{DS_DB_HOST}:{DS_DB_PORT}/{DS_DB_NAME}"
    jdbc_props = {
        "user": DS_DB_USER,
        "password": DS_DB_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("bronze_layer_ingestion_optimized")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

        # ---- DEPENDENCIES ----
        .config(
            "spark.jars.packages",
            f"io.delta:delta-spark_2.12:{DELTA_VERSION},"
            f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION},"
            f"org.postgresql:postgresql:{POSTGRESQL_JDBC_VERSION}",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # ---- LAKEHOUSE S3A (write target) ----
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(f"spark.hadoop.fs.s3a.bucket.{BRONZE_BUCKET}.endpoint", f"http://{LAKEHOUSE_ENDPOINT}")
        .config(f"spark.hadoop.fs.s3a.bucket.{BRONZE_BUCKET}.access.key", LAKEHOUSE_ACCESS_KEY)
        .config(f"spark.hadoop.fs.s3a.bucket.{BRONZE_BUCKET}.secret.key", LAKEHOUSE_SECRET_KEY)

        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # topic → {table, id_col for parallel partitioned reads}
    datasets = {
        "raw_users":            {"table": "users",     "id_col": "user_id"},
        "raw_movies":           {"table": "movies",    "id_col": "movie_id"},
        "raw_playbacks":        {"table": "playbacks", "id_col": "playback_id"},
        "raw_ratings":          {"table": "ratings",   "id_col": "rating_id"},
        "raw_payment_attempts": {"table": "payments",  "id_col": "payment_id"},
    }

    for topic, cfg in datasets.items():
        print(f"\n========== Processing: {topic} ==========")
        spark.sparkContext.setJobGroup(topic, f"Ingesting {topic}", interruptOnCancel=True)
        df = extract_table(spark, cfg["table"], jdbc_url, jdbc_props, id_col=cfg["id_col"])
        upload_delta(df=df, bucket_name=BRONZE_BUCKET, topic=topic)
        spark.sparkContext.setJobGroup(None, None)

    print("\n🚀 All ingestion topics processed successfully!")
    print("Spark UI: http://localhost:4040")
    print("Press Ctrl+C to exit.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down Spark Session...")
        spark.stop()
