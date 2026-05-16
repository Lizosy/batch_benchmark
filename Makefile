# Convenience targets. On Windows without `make`, the equivalent commands are
# listed in the README under "Windows / PowerShell equivalents".

.DEFAULT_GOAL := help
PY ?= python

.PHONY: help setup env run-benchmark run-single dashboard dbt test lint clean docker-up docker-down

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS=":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

setup: ## Create venv-less install of all deps (incl. dev) + copy .env
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install ".[dev]"
	@[ -f .env ] || cp .env.example .env
	@echo "Setup complete. Edit .env if needed."

env: ## Copy .env.example -> .env (no overwrite)
	@[ -f .env ] || cp .env.example .env

run-single: ## Run the dlt pipeline once with .env defaults (standalone smoke test)
	$(PY) -m dlt_pipelines.pipeline

run-benchmark: ## Run the full Dagster batch-size sweep (72 runs)
	$(PY) -m dagster job execute -m dagster_project.definitions -j batch_size_sweep_job

dbt: ## Build the dbt models against the benchmark DuckDB
	cd dbt_project && dbt build --profiles-dir .

dashboard: ## Launch the Streamlit analysis dashboard
	streamlit run dashboard/streamlit_app.py

test: ## Run the test suite
	pytest

lint: ## Ruff + mypy
	ruff check .
	mypy dlt_pipelines dagster_project

docker-up: ## Bring the whole stack up via docker compose
	docker compose up --build

docker-down: ## Tear the stack down
	docker compose down

clean: ## Remove generated data + caches (keeps source)
	rm -rf data/duckdb/*.duckdb data/duckdb/*.wal data/raw/* dbt_project/target dbt_project/logs
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleaned generated artifacts."
