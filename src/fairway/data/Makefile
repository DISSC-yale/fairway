# Fairway Project Makefile
.PHONY: all run submit-hpc shell build clean generate-schema submit-schema-hpc

# Default: Run locally
all: run

# ==============================================================================
# Configuration
# ==============================================================================
# USE_APPTAINER: auto (default), yes, no - controls container execution
USE_APPTAINER ?= auto
FAIRWAY_VENV ?=

# ==============================================================================
# Commands
# ==============================================================================

# Run locally (Interactive)
run: ## Run the pipeline locally
	fairway run

# Submit to HPC with Spark cluster (Fire-and-Forget)
submit-hpc: ## Submit pipeline as Slurm job with Spark cluster
	fairway submit --with-spark

# Enter a shell inside the container
shell: ## Enter a shell inside the container
	fairway shell

# Build the container
build: ## Build the container image
	fairway build

# Generate Schema
generate-schema: ## Generate schema from raw data (local)
	fairway generate-schema --config config/fairway.yaml

submit-schema-hpc: ## Submit schema generation job to Slurm
	sbatch scripts/driver-schema.sh

# Clean up artifacts
clean: ## Clean logs, cache, and temp data
	rm -rf logs/ .fairway_cache/ work/
