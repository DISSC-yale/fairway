# Fairway Project Makefile
.PHONY: all run run-hpc submit-hpc clean test shell bootstrap batches status logs

# Default: Run locally
all: run

# ==============================================================================
# Configuration Variables
# ==============================================================================
TABLE ?=
N_BATCHES ?=
USE_APPTAINER ?= auto
FAIRWAY_VENV ?=

ifeq ($(USE_APPTAINER), yes)
    HAS_APPTAINER := yes
else ifeq ($(USE_APPTAINER), no)
    HAS_APPTAINER := no
else
    HAS_APPTAINER := $(shell if [ -f Apptainer.def ] || [ -f fairway.sif ]; then echo "yes"; else echo "no"; fi)
endif

# ==============================================================================
# Helper: Bootstrap Nextflow (Zero-Dependency)
# ==============================================================================
NXF_VER := 23.10.1

bootstrap:
	@if command -v nextflow >/dev/null 2>&1; then \
		echo "Nextflow check: Found in PATH $$(which nextflow)"; \
	elif [ -x "./nextflow" ]; then \
		echo "Nextflow check: Found ./nextflow"; \
	elif command -v module >/dev/null 2>&1 && module avail Nextflow 2>&1 | grep -q -i "nextflow"; then \
		echo "Nextflow check: Module detected. Will attempt load."; \
	else \
		echo "Nextflow not found. Downloading..."; \
		curl -s https://get.nextflow.io | bash; \
		chmod +x nextflow; \
	fi

# ==============================================================================
# Batch Orchestration Commands
# ==============================================================================

batches: ## Show batch count for TABLE
ifndef TABLE
	$(error TABLE is required. Usage: make batches TABLE=my_table)
endif
	@fairway batches --table $(TABLE)

files: ## List files for TABLE (optionally filtered by BATCH)
ifndef TABLE
	$(error TABLE is required. Usage: make files TABLE=my_table [BATCH=0])
endif
ifdef BATCH
	@fairway files --table $(TABLE) --batch $(BATCH)
else
	@fairway files --table $(TABLE) --count
endif

# ==============================================================================
# Pipeline Execution
# ==============================================================================

# Internal run target (wraps nextflow)
run-internal: bootstrap
	@export FAIRWAY_VENV=$(FAIRWAY_VENV); \
	./scripts/run_pipeline.sh -profile $(PROFILES) \
		$(if $(TABLE),--table $(TABLE),) \
		$(if $(N_BATCHES),--n_batches $(N_BATCHES),) \
		$(if $(ENTRY),-entry $(ENTRY),) \
		$(if $(SPARK_MASTER),--spark-master '$(SPARK_MASTER)',) \
		-resume

# Run locally (Interactive)
run: ## Run the pipeline locally (TABLE optional - if omitted, runs all tables)
	$(MAKE) run-internal PROFILES=standard

# Run on HPC (Interactive on Login Node)
run-hpc: ## Run on Slurm (Login Node - Orchestrator stays here)
	@if [ "$(HAS_APPTAINER)" = "yes" ]; then \
		echo "Detected Apptainer. Enabling container execution..."; \
		$(MAKE) run-internal PROFILES=slurm,apptainer; \
	else \
		echo "No Apptainer detected. Using modules..."; \
		$(MAKE) run-internal PROFILES=slurm; \
	fi

# Submit Driver Job (Fire-and-Forget)
submit-hpc: bootstrap ## Submit the Nextflow orchestrator as a Slurm job
	@echo "Submitting driver job..."
	@FAIRWAY_TABLE=$(TABLE) FAIRWAY_N_BATCHES=$(N_BATCHES) FAIRWAY_VENV=$(FAIRWAY_VENV) \
		sbatch scripts/driver.sh $(HAS_APPTAINER)

# ==============================================================================
# Two-Phase Pipeline (Schema then Ingest)
# ==============================================================================

# Phase 1: Schema discovery (no Spark required)
schema: ## Run schema discovery phase only (no Spark)
	@echo "=== Phase 1: Schema Discovery (no Spark) ==="
	$(MAKE) run-internal PROFILES=slurm ENTRY=schema

schema-local: ## Run schema discovery locally
	$(MAKE) run-internal PROFILES=standard ENTRY=schema

# Phase 2: Data ingestion (requires Spark)
ingest-hpc: ## Run ingest phase on HPC (requires SPARK_MASTER)
ifndef SPARK_MASTER
	$(error SPARK_MASTER is required. Usage: make ingest-hpc SPARK_MASTER=spark://host:7077)
endif
	@echo "=== Phase 2: Ingest (Spark: $(SPARK_MASTER)) ==="
	$(MAKE) run-internal PROFILES=slurm ENTRY=ingest SPARK_MASTER=$(SPARK_MASTER)

# Submit two-phase jobs
submit-schema: bootstrap ## Submit schema phase to Slurm (no Spark)
	@echo "Submitting schema phase..."
	@FAIRWAY_VENV=$(FAIRWAY_VENV) FAIRWAY_ENTRY=schema \
		sbatch scripts/driver.sh $(HAS_APPTAINER)

submit-ingest: bootstrap ## Submit ingest phase to Slurm (starts Spark first)
	@echo "Submitting ingest phase (will start Spark cluster)..."
	@FAIRWAY_VENV=$(FAIRWAY_VENV) FAIRWAY_ENTRY=ingest \
		sbatch scripts/driver.sh $(HAS_APPTAINER)

# ==============================================================================
# Worker Commands (Debugging)
# ==============================================================================

run-worker: ## Run the ingestion worker directly (no Nextflow)
	fairway run

shell: ## Enter a shell inside the ecosystem
	fairway shell

build: ## Build the container image
	fairway build

# ==============================================================================
# Schema Commands
# ==============================================================================

generate-schema: ## Generate schema from raw data (local)
	fairway generate-schema --config config/fairway.yaml

submit-schema-hpc: ## Submit schema generation job to Slurm
	sbatch scripts/driver-schema.sh

# ==============================================================================
# Monitoring
# ==============================================================================

status: ## Show pipeline and cluster status
	@nextflow log -q 2>/dev/null | tail -5 || echo "No Nextflow runs found"
	@echo "---"
	@squeue -u $$USER 2>/dev/null || echo "Not on HPC (squeue unavailable)"

logs: ## Show recent Nextflow logs
	@tail -50 .nextflow.log 2>/dev/null || echo "No logs yet"

# ==============================================================================
# Cleanup
# ==============================================================================

clean: ## Clean logs and temp data
	rm -rf logs/ .nextflow* work/ .nextflow_driver.sh

clean-cache: ## Clear the fairway extraction cache
	fairway cache clean --force

# ==============================================================================
# Testing
# ==============================================================================

test: ## Run all tests (skips Nextflow tests if not installed)
	cd ../../../ && python -m pytest tests/ -v

test-nextflow: bootstrap ## Run Nextflow integration tests (requires Nextflow)
	cd ../../../ && python -m pytest tests/test_nextflow_integration.py -v

test-schema: bootstrap ## Quick test of schema workflow
	@echo "=== Testing Schema Workflow ==="
	nextflow run main.nf -entry schema -profile standard --config config/fairway.yaml --work_dir .fairway/work
	@echo ""
	@echo "=== Checking Results ==="
	@ls -la schema/*.yaml 2>/dev/null || echo "No schema files found"
	@echo ""
	@cat schema/*.yaml 2>/dev/null || echo "Could not read schema files"
