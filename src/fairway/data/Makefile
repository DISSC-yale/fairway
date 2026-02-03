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
