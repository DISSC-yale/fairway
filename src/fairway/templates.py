
MAKEFILE_TEMPLATE = """.PHONY: help setup install test clean generate-data generate-schema run run-slurm docs status cancel build

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

build: ## Build or pull the Apptainer container
	$(FAIRWAY) build

docs: ## Build and serve documentation using mkdocs
	mkdocs serve
"""

NEXTFLOW_CONFIG = """params {
    config = "config/fairway.yaml"
    outdir = "data/final"
}

profiles {
    standard {
        process.executor = 'local'
    }
    
    slurm {
        process.executor = 'slurm'
        process.queue = { params.slurm_partition ?: 'day' }
        process.memory = { params.slurm_mem ?: '16 GB' }
        process.cpus = { params.slurm_cpus_per_task ?: 4 }
        process.time = { params.slurm_time ?: '24:00:00' }
        // Driver process for Spark or DuckDB always needs 1 node
        process.clusterOptions = { "--account=${params.account ?: 'borzekowski'} --nodes=1" }
    }

    apptainer {
        apptainer.enabled = true
        apptainer.autoMounts = true
        apptainer.runOptions = '--bind /vast'
    }
}
"""

# The MAIN_NF template needs to be robust.
MAIN_NF = """nextflow.enable.dsl=2

params.config = "config/fairway.yaml"
params.outdir = "data"
params.container = "fairway.sif"
params.spark_master = ""
params.batch_size = 30
params.slurm_nodes = 1
params.slurm_cpus_per_task = 4
params.slurm_mem = "16G"
params.slurm_time = "24:00:00"
params.slurm_partition = "day"
params.account = "borzekowski"

process RUN_INGESTION {
    maxForks params.batch_size
    tag "Ingesting ${params.config}"
    publishDir "${params.outdir}", mode: 'copy'

    // Only use container if profile enables it or params.container represents one
    // But Nextflow handles container mounting via profiles usually. 
    // Here we might just rely on profile.

    input:
    path config_file

    output:
    path "data/intermediate/*", optional: true
    path "data/final/*", optional: true
    path "data/fmanifest.json", optional: true

    script:
    \"\"\"
    export PYTHONPATH=\$PYTHONPATH:\$(pwd)/src
    fairway run --config ${config_file} --spark_master "${params.spark_master}" --profile standard
    // Note: Inside the process, we run as 'standard' (local to the node) because Slurm/Nextflow handled the allocation
    \"\"\"
}

workflow {
    config_ch = Channel.fromPath(params.config)
    RUN_INGESTION(config_ch)
}
"""

APPTAINER_DEF = """Bootstrap: docker
From: python:3.10-slim

%post
    apt-get update && apt-get install -y git openjdk-17-jre-headless
    pip install --upgrade pip
    pip install git+https://github.com/DISSC-yale/fairway.git
    
%environment
    export LC_ALL=C.UTF-8
    export LANG=C.UTF-8

%runscript
    exec fairway "$@"
"""

DOCKERFILE_TEMPLATE = """FROM python:3.10-slim

RUN apt-get update && apt-get install -y git openjdk-17-jre-headless
RUN pip install --upgrade pip
RUN pip install git+https://github.com/DISSC-yale/fairway.git

ENTRYPOINT ["fairway"]
"""

# Script to be placed in scripts/fairway-hpc.sh during init
FAIRWAY_HPC_SH_TEMPLATE = r"""#!/bin/bash
# =============================================================================
# fairway-hpc.sh - HPC Environment Setup
# =============================================================================
# 
# This script loads the necessary modules for the Fairway environment.
# Job submission and management are now handled by the 'fairway' CLI.
#
# Usage:
#   source fairway-hpc.sh setup           # Load required modules
#   source fairway-hpc.sh setup-spark     # Load modules including Spark
#
# =============================================================================

set -e

# -----------------------------------------------------------------------------
# Configuration - Customize these for your HPC environment
# -----------------------------------------------------------------------------

# Module names - adjust these to match your HPC's module system
MODULES_BASE=(
    "Nextflow/25.04.6"
    "Python/3.10.8-GCCcore-12.2.0"
)

MODULES_SPARK=(
    "Java/11.0.20"
    "Spark/3.5.0"
)

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------

print_header() {
    echo ""
    echo "============================================================"
    echo "  Fairway HPC Environment Setup"
    echo "============================================================"
}

print_usage() {
    cat << EOF

Usage: source fairway-hpc.sh <command>

Commands:
  setup             Load base modules (Nextflow, Python)
  setup-spark       Load all modules including Spark

Note:
  This script MUST be sourced to load modules into your current shell.
  For running jobs, use: fairway run --slurm ...
  For checking status, use: fairway status

EOF
}

load_modules() {
    echo "Loading HPC modules..."
    
    # Clear any conflicting modules first
    module purge 2>/dev/null || true
    
    for mod in "${MODULES_BASE[@]}"; do
        echo "  Loading: $mod"
        module load "$mod" 2>/dev/null || {
            echo "  Warning: Could not load module '$mod'"
            echo "  Available modules can be listed with: module avail"
        }
    done
    
    echo ""
    echo "Modules loaded successfully."
    echo "Python: $(which python)"
    echo ""
}

setup_venv() {
    # Determine project root (assuming script is in scripts/)
    local SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
    local VENV_DIR="$PROJECT_ROOT/.venv"

    echo "Checking virtual environment..."
    if [ ! -d "$VENV_DIR" ]; then
        echo "Creating virtual environment in $VENV_DIR..."
        python3 -m venv "$VENV_DIR"
        source "$VENV_DIR/bin/activate"
        echo "Installing dependencies..."
        pip install --upgrade pip
        if [ -f "$PROJECT_ROOT/requirements.txt" ]; then
            pip install -r "$PROJECT_ROOT/requirements.txt"
        fi
        if [ -f "$PROJECT_ROOT/pyproject.toml" ] || [ -f "$PROJECT_ROOT/setup.py" ]; then
            pip install -e "$PROJECT_ROOT"
        fi
        echo "Virtual environment created and dependencies installed."
    else
        echo "Activating virtual environment..."
        source "$VENV_DIR/bin/activate"
    fi
    
    echo "Environment active: $(which python)"
    echo ""
}

load_spark_modules() {
    load_modules
    setup_venv
    
    echo ""
    echo "Loading Spark modules..."
    
    for mod in "${MODULES_SPARK[@]}"; do
        echo "  Loading: $mod"
        module load "$mod" 2>/dev/null || {
            echo "  Warning: Could not load module '$mod'"
        }
    done
    
    echo ""
    echo "Spark modules loaded."
}


# -----------------------------------------------------------------------------
# Main Command Handler
# -----------------------------------------------------------------------------

main() {
    local COMMAND="${1:-}"
    shift || true
    
    case "$COMMAND" in
        setup)
            print_header
            load_modules
            setup_venv
            ;;
        setup-spark)
            print_header
            load_spark_modules
            ;;
        -h|--help|help|"")
            print_header
            print_usage
            ;;
        *)
            echo "Unknown command: $COMMAND"
            print_usage
            ;;
    esac
}

# Only run main if sourced (preferred) or executed
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
    # Script is sourced
    if [[ $# -gt 0 ]]; then
        main "$@"
    else
        # If sourced without args, print usage? Or maybe default to setup?
        # Let's print usage to be safe.
        print_usage
    fi
else
    # Script is executed directly - modules won't stick, but we can print info
    echo "Warning: This script should be sourced to load modules into your shell."
    echo "Usage: source fairway-hpc.sh setup"
    main "$@"
fi
"""
