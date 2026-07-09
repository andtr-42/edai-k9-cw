#!/bin/bash

# Exit immediately if any command fails
set -e

# Visual colors for terminal output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_status() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

echo "============================================="
log_status "Starting Data Pipeline Orchestration"
echo "============================================="

# Step 1: Data Generation
log_status "Step 1/4: Running Data Generation..."
python3 -m src.data_gen.data_gen

# Step 2: Store Data Source
log_status "Step 2/4: Storing Data Source Data..."
python3 -m scripts.store_data_source_data

# Step 3: Data Ingestion (Bronze Layer)
log_status "Step 3/4: Ingesting to Bronze Layer..."
python3 -m src.data_ingestion.ingest_to_bronze

# Step 4: Data Processing (Silver Layer)
log_status "Step 4/4: Processing to Silver Layer..."
python3 -m src.data_processing.process_to_silver

echo "============================================="
echo -e "${GREEN}✅ Success: All pipeline stages completed successfully!${NC}"
echo "============================================="
