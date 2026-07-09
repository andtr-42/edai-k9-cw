"""
Gold Layer Schema Initialization

This script connects to the Data Warehouse, applies the DDL schema,
and validates that the tables and views were created successfully.

python3 -m scripts.setup_dwh_db
"""

import os
import psycopg
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

DWH_DB = os.getenv("DWH_DB", "dwh_database")
DWH_USER = os.getenv("DWH_USER", "dwh_user")
DWH_PASSWORD = os.getenv("DWH_PASSWORD", "dwh_password")
DWH_PORT = os.getenv("DWH_PORT", "5433")
DWH_HOST = os.getenv("DWH_HOST", "localhost")

def apply_schema(ddl_path):
    """Connects to Postgres and applies the Gold layer DDL."""
    conn_info = f"dbname={DWH_DB} user={DWH_USER} password={DWH_PASSWORD} host={DWH_HOST} port={DWH_PORT}"
    
    try:
        with psycopg.connect(conn_info) as conn:
            with conn.cursor() as cur:
                print("⏳ Applying schema...")
                
                # Execute the DDL file
                with open(ddl_path, "r") as f:
                    cur.execute(f.read())
                
                conn.commit()
                print("✔ Schema applied successfully.")
                
    except Exception as e:
        print(f"❌ Failed to apply schema: {e}")
        raise

def validate_schema():
    """Validates the existence of the Gold schema and tables."""
    conn_info = f"dbname={DWH_DB} user={DWH_USER} password={DWH_PASSWORD} host={DWH_HOST} port={DWH_PORT}"
    
    expected_entities = [
        "dim_user", "dim_movie", "dim_date", "dim_payment_method", 
        "fact_playback", "fact_rating", "fact_payment_attempt", "obt_playback", "feat_user_90d"
    ]
    
    try:
        with psycopg.connect(conn_info) as conn:
            with conn.cursor() as cur:
                # Query information_schema for tables/views in the 'gold' schema
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'gold';
                """)
                
                actual_entities = [row[0] for row in cur.fetchall()]
                
                print("\n--- Schema Validation ---")
                for entity in expected_entities:
                    if entity in actual_entities:
                        print(f"✅ {entity} exists.")
                    else:
                        print(f"❌ {entity} IS MISSING.")
                        
    except Exception as e:
        print(f"❌ Failed to validate schema: {e}")

if __name__ == "__main__":

    # Path to the SQL file created earlier
    ddl_init_schema_path = os.path.join(os.path.dirname(__file__), "dwh_schema_ddl", "01_init_schema.sql")
    ddl_add_indexes_path = os.path.join(os.path.dirname(__file__), "dwh_schema_ddl", "02_add_indexes.sql")
    
    apply_schema(ddl_init_schema_path)
    apply_schema(ddl_add_indexes_path)
    validate_schema()