# Fairway Project Makefile
.PHONY: all run run-hpc run-worker clean test shell

# Default: Run locally
all: run

# Run the pipeline (wraps nextflow)
run: ## Run the pipeline locally (Orchestrator)
	nextflow run main.nf -profile standard

# Run on HPC (wraps sbatch submission via Nextflow)
run-hpc: ## Run on Slurm with Spark (Orchestrator)
	nextflow run main.nf -profile slurm

# Run worker manually (for debugging)
run-worker: ## Run the ingestion worker directly (no Nextflow)
	fairway run

# Enter a shell inside the container (useful for debugging)
shell: ## Enter a shell inside the ecosystem
	fairway shell

# Build the container (Apptainer/Docker)
build: ## Build the container image
	fairway build

# Clean up artifacts
clean: ## Clean logs and temp data
	rm -rf logs/ .nextflow* work/
