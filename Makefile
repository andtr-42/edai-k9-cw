PYTHON = python3

.PHONY: init-data-transform

data-ingest:
	$(PYTHON) -m src.data_ingestion.ingest_to_bronze_baseline

data-ingest-opt:
	$(PYTHON) -m src.data_ingestion.ingest_to_bronze_optimized

data-process:
	$(PYTHON) -m src.data_processing.process_to_silver_baseline

data-process-opt:
	$(PYTHON) -m src.data_processing.process_to_silver_optimized

init-data-transform:
	$(PYTHON) -m scripts.setup_dwh_db
	$(PYTHON) -m src.data_transformation.init_transform_to_gold_baseline

init-data-transform-opt:
	$(PYTHON) -m scripts.setup_dwh_db
	$(PYTHON) -m src.data_transformation.init_transform_to_gold_optimized

increment-data-transform:
	$(PYTHON) -m src.data_transformation.increment_transform_to_gold_baseline

increment-data-transform-opt:
	$(PYTHON) -m src.data_transformation.increment_transform_to_gold_optimized


	