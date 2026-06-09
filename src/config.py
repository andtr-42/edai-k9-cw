# src/config.py
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from two levels up (root folder)
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# --- Software & Dependency Versions ---
DELTA_VERSION = "3.2.0"       # Matches PySpark 3.5.x
HADOOP_VERSION = "3.3.4"

# --- Static Project Paths ---
OFFLINE_DATA_PATH = BASE_DIR / "data" / "offline"
STREAMING_DATA_PATH = BASE_DIR / "data" / "streaming"

# MinIO Config
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze-bucket")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver-bucket")
GOLD_BUCKET = os.getenv("GOLD_BUCKET", "gold-bucket")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")