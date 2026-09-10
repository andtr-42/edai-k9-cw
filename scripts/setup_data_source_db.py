"""
Data Source DB Initialization

Creates the operational tables in the PostgreSQL data-source-storage database.

python3 -m scripts.setup_data_source_db
"""

import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

DS_DB_HOST = os.getenv("DS_DB_HOST", "localhost")
DS_DB_PORT = os.getenv("DS_DB_PORT", "5434")
DS_DB_NAME = os.getenv("DS_DB_NAME", "data_source_storage")
DS_DB_USER = os.getenv("DS_DB_USER", "dsadmin")
DS_DB_PASSWORD = os.getenv("DS_DB_PASSWORD", "dsadmin123")

_CONN_INFO = f"dbname={DS_DB_NAME} user={DS_DB_USER} password={DS_DB_PASSWORD} host={DS_DB_HOST} port={DS_DB_PORT}"

EXPECTED_TABLES = ["users", "movies", "playbacks", "ratings", "payments"]


def apply_schema(ddl_path: str) -> None:
    with psycopg.connect(_CONN_INFO) as conn:
        with conn.cursor() as cur:
            print(f"⏳ Applying schema from {ddl_path} ...")
            with open(ddl_path) as f:
                cur.execute(f.read())
        conn.commit()
    print("✔ Schema applied successfully.")


def validate_schema() -> None:
    with psycopg.connect(_CONN_INFO) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
            """)
            actual = {row[0] for row in cur.fetchall()}

    print("\n--- Schema Validation ---")
    for table in EXPECTED_TABLES:
        status = "✅" if table in actual else "❌ MISSING"
        print(f"  {status}: {table}")


if __name__ == "__main__":
    ddl_path = os.path.join(os.path.dirname(__file__), "data_source_schema_ddl", "01_init_schema.sql")
    apply_schema(ddl_path)
    validate_schema()
