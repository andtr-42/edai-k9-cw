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

# DATA SOURCE CREDENTIALS
DATA_SOURCE_ENDPOINT = os.getenv("DATA_SOURCE_ENDPOINT", "localhost:9000")
DATA_SOURCE_ACCESS_KEY = os.getenv("DATA_SOURCE_ACCESS_KEY", "dsadmin")
DATA_SOURCE_SECRET_KEY = os.getenv("DATA_SOURCE_SECRET_KEY", "dsadmin123")
DATA_SOURCE_BUCKET = os.getenv("DATA_SOURCE_BUCKET", "data-source-bucket")

# DATA LAKEHOUSE CREDENTIALS
LAKEHOUSE_ENDPOINT = os.getenv("LAKEHOUSE_ENDPOINT", "localhost:9002")
LAKEHOUSE_ACCESS_KEY = os.getenv("LAKEHOUSE_ACCESS_KEY", "lhadmin")
LAKEHOUSE_SECRET_KEY = os.getenv("LAKEHOUSE_SECRET_KEY", "lhadmin123")

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze-bucket")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver-bucket")