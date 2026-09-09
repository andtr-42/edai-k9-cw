# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A movie streaming data pipeline coursework project implementing a **Medallion Architecture** (Bronze → Silver → Gold) lakehouse pattern. The pipeline ingests synthetic movie streaming data through PySpark, stores it in Delta Lake format on MinIO, and transforms it into a Star Schema data warehouse in PostgreSQL.

## Environment Setup

- Python 3.11 (pinned via `.python-version`)
- Package manager: `uv` (lockfile: `uv.lock`)
- Linter: `ruff` (line length 88)

Start all infrastructure:
```bash
docker-compose up -d
```

Services:
- MinIO Data Source: `http://localhost:9000` (UI: `http://localhost:9001`) — credentials: `dsadmin/dsadmin123`
- MinIO Lakehouse: `http://localhost:9002` (UI: `http://localhost:9003`) — credentials: `lhadmin/lhadmin123`
- PostgreSQL DWH: `localhost:5433` — credentials from `.env`
- Hive Metastore: `localhost:9083`

## Running the Pipeline

### Full pipeline (script):
```bash
bash run_pipeline.sh
```

### Step-by-step via Make:
```bash
# 1. Generate synthetic data (outputs to output/offline/ and output/streaming/)
python3 -m src.data_gen.data_gen

# 2. Setup storage and upload data to MinIO data source
python3 -m scripts.setup_data_source_storage
python3 -m scripts.store_data_source_data
python3 -m scripts.setup_lakehouse_storage  # creates bronze/silver buckets

# 3. Ingest to Bronze (MinIO S3 → Delta Lake, adds raw_id + _ingested_at metadata)
make data-ingest          # baseline
make data-ingest-opt      # optimized variant

# 4. Process to Silver (Bronze → Silver, deduplication + schema cleanup)
make data-process         # baseline
make data-process-opt     # optimized variant

# 5. Initialize Gold layer in PostgreSQL (Silver Delta → Postgres Star Schema)
make init-data-transform        # baseline (also runs setup_dwh_db)
make init-data-transform-opt    # optimized variant

# 6. Incremental Gold update
make increment-data-transform        # baseline
make increment-data-transform-opt    # optimized variant
```

### Data validation:
```bash
python3 -m src.data_gen.validate_data_gen
```

## Architecture

### Data Flow
```
[Synthetic Generator] → output/ (Parquet + JSONL)
         ↓
[MinIO data-source-bucket] (raw Parquet files)
         ↓  (PySpark + Delta)
[MinIO bronze-bucket] (Delta format, topics/raw_*)
         ↓  (PySpark + Delta)
[MinIO silver-bucket] (Delta format, topics/stg_*)
         ↓  (PySpark JDBC)
[PostgreSQL gold schema] (Star Schema + OBT + Feature tables)
```

### Layer Descriptions

**Bronze** (`bronze-bucket/topics/raw_*`): Raw ingested data with added metadata columns: `raw_id` (monotonically increasing) and `_ingested_at` (timestamp). Users dataset uses schema merging (`mergeSchema=true`) to handle schema evolution.

**Silver** (`silver-bucket/topics/stg_*`): Cleaned Bronze data with `raw_id` dropped and deduplication applied (playbacks deduped on `user_id, movie_id, click_ts`).

**Gold** (PostgreSQL `gold` schema): Star schema with:
- Dimensions: `dim_user`, `dim_movie`, `dim_date`, `dim_payment_method`
- Facts: `fact_playback`, `fact_rating`, `fact_payment_attempt`
- Denormalized: `obt_playback` (One Big Table)
- Feature store: `feat_user_90d` (point-in-time 90-day user features, 30-day snapshot matrix)

### Dual-storage S3A Configuration
Bronze ingestion uses **per-bucket credential routing** — separate S3A credentials are configured per-bucket name to allow Spark to read from the data source MinIO and write to the lakehouse MinIO in the same session. See `ingest_to_bronze_baseline.py` for the pattern.

### Baseline vs Optimized variants
Each pipeline stage has two implementations: `*_baseline.py` (straightforward, unoptimized) and `*_optimized.py` (performance-optimized). This is intentional for comparison/benchmarking.

## Configuration

All credentials and paths are centralized in `src/config.py`, loaded from `.env`. Key constants:
- `DELTA_VERSION`, `HADOOP_VERSION` — Spark JAR versions
- `OFFLINE_DATA_PATH` / `STREAMING_DATA_PATH` — local output paths
- MinIO and PostgreSQL connection details

The PostgreSQL DWH schema DDL is at `scripts/dwh_schema_ddl/01_init_schema.sql` (run via `scripts/setup_dwh_db.py`).

## Key Dependencies

- PySpark 3.5.0 with Delta Lake 3.2.0 and Hadoop AWS 3.3.4
- Delta Lake JARs downloaded automatically via `spark.jars.packages` at runtime
- PostgreSQL JDBC `org.postgresql:postgresql:42.6.0` (used in Gold transformation)
- MinIO Python client for bucket setup scripts
