.PHONY: help setup install test clean generate-data generate-schema run run-slurm docs status cancel pull build

# Default target
.DEFAULT_GOAL := help

# Configuration
VENV := .venv
BIN := $(VENV)/bin
PYTHON := $(BIN)/python

# Detect Fairway executable (use venv if exists, else system)
ifneq (,$(wildcard $(BIN)/fairway))
    FAIRWAY := $(BIN)/fairway
else
    FAIRWAY := fairway
endif

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \\033[36m%-20s\\033[0m %s\\n", $$1, $$2}'

setup: ## Create virtual environment and install dependencies
	@echo "Checking/Creating virtual environment in $(VENV)..."
	@test -d $(VENV) || python3 -m venv $(VENV)
	@echo "Installing dependencies..."
	@if [ -f requirements.txt ]; then $(BIN)/pip install -r requirements.txt; fi
	@if [ -f pyproject.toml ] || [ -f setup.py ]; then $(BIN)/pip install -e .; fi
	@echo ""
	@echo "Setup complete."
	@echo "----------------------------------------------------------------"
	@echo "To activate the environment: source $(VENV)/bin/activate"
	@echo "To load HPC modules:         source scripts/fairway-hpc.sh setup"
	@echo "----------------------------------------------------------------"

install: ## Install the package in editable mode (assumes active env or uses system)
	pip install -e .

test: ## Run the test suite
	$(BIN)/pytest tests || pytest tests

clean: ## Remove build artifacts and temporary files
	rm -rf build/ dist/ *.egg-info .pytest_cache .coverage $(VENV)
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

generate-data: ## Generate test data (default: size=small, partitioned=True)
	$(FAIRWAY) generate-data --size small --partitioned

generate-schema: ## Generate schema from data (requires FILE=<path>)
	@if [ -z "$(FILE)" ]; then \\
		echo "Error: FILE argument is required. Usage: make generate-schema FILE=<path_to_file>"; \\
		exit 1; \\
	fi
	$(FAIRWAY) generate-schema $(FILE)

run: ## Run the pipeline locally (auto-discovers config)
	$(FAIRWAY) run

run-slurm: ## Run the pipeline on Slurm (requires Slurm environment)
	$(FAIRWAY) run --profile slurm --slurm

status: ## Show status of Fairway jobs on Slurm
	$(FAIRWAY) status

cancel: ## Cancel a Fairway job (usage: make cancel JOB_ID=12345)
	@if [ -z "$(JOB_ID)" ]; then \\
		echo "Error: JOB_ID argument is required. Usage: make cancel JOB_ID=<job_id>"; \\
		exit 1; \\
	fi
	$(FAIRWAY) cancel $(JOB_ID)

pull: ## Pull (mirror) the Apptainer container from registry
	$(FAIRWAY) pull

build: ## Build the Apptainer container from local definition
	$(FAIRWAY) build

docs: ## Build and serve documentation using mkdocs
	mkdocs serve
