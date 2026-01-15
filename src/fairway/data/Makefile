# Fairway Project Makefile
.PHONY: all run run-hpc run-worker clean test shell

# Default: Run locally
all: run

# Run the pipeline (wraps nextflow)
run: ## Run the pipeline locally (Orchestrator)
	nextflow run main.nf -profile standard

# Run on HPC (wraps sbatch submission via Nextflow)
run-hpc: ## Run on Slurm with Spark (Orchestrator)
	@echo "Starting Spark Cluster..."
	@fairway spark start
	@export SPARK_MASTER=$$(cat ~/spark_master_url.txt); \
	echo "Spark Master: $$SPARK_MASTER"; \
	nextflow run main.nf -profile slurm --spark_master $$SPARK_MASTER || (fairway spark stop && exit 1)
	@fairway spark stop

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
