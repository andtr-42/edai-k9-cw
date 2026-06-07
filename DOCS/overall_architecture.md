

- Pipeline Orchestration: Airflow 
- Pipeline Governance: DataHub
- (Data Sources) - Offline historical data -> Ingestion Layer (Spark Batch Processing) -> Bronze Layer (Lakehouse Storage with MinIO, Delta Lake, Hive-Metastore) -> Transformation Layer 1 (Clean, dedup with Spark) -> Silver Layer (Lakehouse Storage with MinIO, Delta Lake, Hive-Metastore) -> Transformation Layer 2 (Feature engineer, aggregates with Spark) -> Gold Layer (Dim, Fact, OBT Warehouse with ClickHouse) -> Feature Store (Feast)
- (Data Source) - Streaming data -> Ingestion Layer (Kafka) -> Transformation Layer (Dedup, out of order and late/arrival event, aggreation, feature computation with Flink) -> Feature Store (Aggreates stored inside streaming feature with Feast).

