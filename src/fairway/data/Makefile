# Fairway Project Makefile
.PHONY: all run run-hpc submit-hpc clean test shell bootstrap

# Default: Run locally
all: run

# ==============================================================================
# Helper: Bootstrap Nextflow (Zero-Dependency)
# ==============================================================================
# If 'nextflow' is not in PATH, download it to the current directory
NXF_VER := 23.10.1
NXF_VER := 23.10.1

bootstrap:
	@if command -v nextflow >/dev/null 2>&1; then \
		echo "Nextflow check: Found in PATH used $$(which nextflow)"; \
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
# Execution Profiles
# ==============================================================================
# Detect Apptainer availability
HAS_APPTAINER := $(shell if [ -f Apptainer.def ] || [ -f fairway.sif ]; then echo "yes"; else echo "no"; fi)

# Base profiles
PROFILES := standard

# ==============================================================================
# Commands
# ==============================================================================

# Internal run target (wraps nextflow)
# Internal run target (wraps nextflow)
run-internal: bootstrap
	@# Attempt module load if available and run
	@if command -v module >/dev/null 2>&1; then module load Nextflow 2>/dev/null || true; fi; \
	NXF_CMD=$$(if [ -x "./nextflow" ]; then echo "./nextflow"; else echo "nextflow"; fi); \
	CONTAINER_ARG=$$(if [ -f "$$(pwd)/fairway.sif" ]; then echo "--container_image $$(pwd)/fairway.sif"; else echo ""; fi); \
	$$NXF_CMD run main.nf -profile $(PROFILES) -resume $$CONTAINER_ARG

# Run locally (Interactive)
run: ## Run the pipeline locally
	$(MAKE) run-internal PROFILES=standard

# Run on HPC (Interactive on Login Node)
run-hpc: PROFILES := slurm
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
	@sbatch scripts/driver.sh $(HAS_APPTAINER)

# Run worker manually (for debugging)
run-worker: ## Run the ingestion worker directly (no Nextflow)
	fairway run

# Enter a shell inside the container
shell: ## Enter a shell inside the ecosystem
	fairway shell

# Build the container
build: ## Build the container image
	fairway build

# Clean up artifacts
clean: ## Clean logs and temp data
	rm -rf logs/ .nextflow* work/ .nextflow_driver.sh
