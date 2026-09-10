#!/bin/bash

# Exit immediately if any command fails
set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_status() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

echo "============================================="
log_status "Starting Data Pipeline Orchestration"
echo "============================================="

# Step 1: Initialize data-source-storage DB tables
log_status "Step 1/4: Setting up data-source-storage DB..."
python3 -m scripts.setup_data_source_db

# Step 2: Generate synthetic data and insert into PostgreSQL
log_status "Step 2/4: Running Data Generation..."
python3 -m src.data_gen.data_gen

# Step 3: Data Ingestion (Bronze Layer) — reads from PostgreSQL via JDBC
log_status "Step 3/4: Ingesting to Bronze Layer..."
python3 -m src.data_ingestion.ingest_to_bronze_baseline

# Step 4: Data Processing (Silver Layer)
log_status "Step 4/4: Processing to Silver Layer..."
python3 -m src.data_processing.process_to_silver_baseline

echo "============================================="
echo -e "${GREEN}✅ Success: All pipeline stages completed successfully!${NC}"
echo "============================================="
