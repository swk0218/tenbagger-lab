PYTHON ?= python3

run:
	$(PYTHON) -m tech_pipeline.cli full

run_quick:
	$(PYTHON) -m tech_pipeline.cli quick

run_deep:
	$(PYTHON) -m tech_pipeline.cli deep

test:
	PYTHONPATH=src pytest -q
