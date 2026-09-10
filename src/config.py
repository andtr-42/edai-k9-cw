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
OFFLINE_DATA_PATH = BASE_DIR / "output" / "offline"
STREAMING_DATA_PATH = BASE_DIR / "output" / "streaming"

# DATA SOURCE CREDENTIALS (PostgreSQL operational DB)
DS_DB_HOST = os.getenv("DS_DB_HOST", "localhost")
DS_DB_PORT = os.getenv("DS_DB_PORT", "5434")
DS_DB_NAME = os.getenv("DS_DB_NAME", "data_source_storage")
DS_DB_USER = os.getenv("DS_DB_USER", "dsadmin")
DS_DB_PASSWORD = os.getenv("DS_DB_PASSWORD", "dsadmin123")

# DATA LAKEHOUSE CREDENTIALS
LAKEHOUSE_ENDPOINT = os.getenv("LAKEHOUSE_ENDPOINT", "localhost:9002")
LAKEHOUSE_ACCESS_KEY = os.getenv("LAKEHOUSE_ACCESS_KEY", "lhadmin")
LAKEHOUSE_SECRET_KEY = os.getenv("LAKEHOUSE_SECRET_KEY", "lhadmin123")

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze-bucket")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver-bucket")