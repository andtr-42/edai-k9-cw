PYTHON = python3

.PHONY: init-data-transform

init-data-transform:
	$(PYTHON) -m scripts.setup_dwh_db
	$(PYTHON) -m src.data_transformation.init_transform_to_gold_baseline

increment-data-transform:
	$(PYTHON) -m src.data_transformation.increment_transform_to_gold_baseline


	