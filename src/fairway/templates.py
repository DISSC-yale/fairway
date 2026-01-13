
MAKEFILE_TEMPLATE = """.PHONY: help install install-all test clean generate-data generate-schema run run-slurm docs status cancel build

# Default target
.DEFAULT_GOAL := help

# Python interpreter
PYTHON := python

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \\033[36m%-20s\\033[0m %s\\n", $$1, $$2}'

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
	@if [ -z "$(FILE)" ]; then \\
		echo "Error: FILE argument is required. Usage: make generate-schema FILE=<path_to_file>"; \\
		exit 1; \\
	fi
	fairway generate-schema $(FILE)

run: ## Run the pipeline locally (auto-discovers config)
	fairway run

run-slurm: ## Run the pipeline on Slurm (requires Slurm environment)
	fairway run --profile slurm --slurm

status: ## Show status of Fairway jobs on Slurm
	fairway status

cancel: ## Cancel a Fairway job (usage: make cancel JOB_ID=12345)
	@if [ -z "$(JOB_ID)" ]; then \\
		echo "Error: JOB_ID argument is required. Usage: make cancel JOB_ID=<job_id>"; \\
		exit 1; \\
	fi
	fairway cancel $(JOB_ID)

build: ## Build or pull the Apptainer container
	fairway build

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
FAIRWAY_HPC_SH_TEMPLATE = """#!/bin/bash
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

load_spark_modules() {
    load_modules
    
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
