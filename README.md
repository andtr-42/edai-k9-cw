# Movie Streaming Data Pipeline 

## 1. Business Domain Overview

This project simulates a medium-size movie streaming platform. The generator produces:

- Offline historical/reference data (Parquet)
- Streaming real-time events (JSON)

The goal is to support downstream ingestion, transformation, and feature engineering while intentionally injecting realistic data quality and processing challenges.

## Table of Contents 

## System Architecture

## Getting started 

Clone the repo 

Start all the infras

Setup the environment 

Access the Services:

Postgres is accessible on the default port 5432.
Kafka Control Center is accessible at http://localhost:9021.
Debezium is accessible at http://localhost:8085.
MinIO is accessible at http://localhost:9001.
Airflow is accessible at http://localhost:8080.

## How To Guide 

Guide 
-> step 

data generate 

`python3 -m src.data_gen.data_gen`

data validation

`python3 -m src.data_gen.validate_data_gen`

`python3 -m scripts.setup_data_source_storage`

`python3 -m scripts.store_data_source_data`




-> photo to see the results 





