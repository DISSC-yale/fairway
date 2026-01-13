.PHONY: help install install-all test clean generate-data generate-schema run run-slurm docs

# Default target
.DEFAULT_GOAL := help

# Python interpreter
PYTHON := python

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install the package in editable mode
	pip install -e .

install-all: ## Install the package with all optional dependencies (spark, duckdb, redivis)
	pip install -e ".[all]"

test: ## Run the test suite
	pytest tests

clean: ## Remove build artifacts and temporary files
	rm -rf build/ dist/ *.egg-info .pytest_cache .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

generate-data: ## Generate test data (default: size=small, partitioned=True)
	fairway generate-data --size small --partitioned

generate-schema: ## Generate schema from data (requires input file, e.g., make generate-schema FILE=data/raw/data.csv)
	@if [ -z "$(FILE)" ]; then \
		echo "Error: FILE argument is required. Usage: make generate-schema FILE=<path_to_file>"; \
		exit 1; \
	fi
	fairway generate-schema $(FILE)

run: ## Run the pipeline locally (auto-discovers config)
	fairway run

run-slurm: ## Run the pipeline on Slurm (requires Slurm environment)
	fairway run --profile slurm --slurm

status: ## Show status of Fairway jobs on Slurm
	fairway status

cancel: ## Cancel a Fairway job (usage: make cancel JOB_ID=12345)
	@if [ -z "$(JOB_ID)" ]; then \
		echo "Error: JOB_ID argument is required. Usage: make cancel JOB_ID=<job_id>"; \
		exit 1; \
	fi
	fairway cancel $(JOB_ID)

build: ## Build or pull the Apptainer container
	fairway build


docs: ## Build and serve documentation using mkdocs
	mkdocs serve
