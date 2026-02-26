PYTHON ?= python3

run:
	PYTHONPATH=src $(PYTHON) -m tech_pipeline.cli full

run_quick:
	PYTHONPATH=src $(PYTHON) -m tech_pipeline.cli quick

run_deep:
	PYTHONPATH=src $(PYTHON) -m tech_pipeline.cli deep

test:
	PYTHONPATH=src pytest -q
