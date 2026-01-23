#!/bin/bash
# =============================================================================
# fairway-hpc.sh - User Environment Setup Helper
# =============================================================================
# 
# PURPOSE: This is a USER-FACING utility to set up your shell environment
# on HPC clusters (like Yale Grace).
#
# USE THIS SCRIPT TO:
# - Load necessary modules (Python, Nextflow, Spark) via 'setup' commands.
#
# DO NOT confuse this with 'scripts/run_pipeline.sh', which is the internal
# runner used by the system.
#
# Usage:
#   source fairway-hpc.sh setup           # Load required modules
#   source fairway-hpc.sh setup-spark     # Load modules including Spark
#   fairway-hpc.sh run [options]          # Submit a fairway job
#   fairway-hpc.sh run --apptainer        # Submit using Apptainer container
#   fairway-hpc.sh build-container        # Build/pull Apptainer image
#   fairway-hpc.sh status                 # Check running fairway jobs
#   fairway-hpc.sh cancel [job_id]        # Cancel a fairway job
#
# =============================================================================

set -e

# -----------------------------------------------------------------------------
# Configuration - Customize these for your HPC environment
# -----------------------------------------------------------------------------
DEFAULT_ACCOUNT="borzekowski"
DEFAULT_PARTITION="day"
DEFAULT_TIME="24:00:00"
DEFAULT_NODES=2
DEFAULT_CPUS=32
DEFAULT_MEM="200G"

# Container configuration
CONTAINER_IMAGE="docker://ghcr.io/dissc-yale/fairway:latest"
CONTAINER_LOCAL="fairway.sif"

# Module names - adjust these to match your HPC's module system
MODULES_BASE=(
    "Nextflow/25.04.6"
    "Python/3.10.8-GCCcore-12.2.0"
    "Apptainer"
)

MODULES_SPARK=(
    "Java/17"
    "Spark"
)

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------

print_header() {
    echo ""
    echo "============================================================"
    echo "  Fairway HPC Helper"
    echo "============================================================"
}

print_usage() {
    cat << EOF

Usage: fairway-hpc.sh <command> [options]

Commands:
  setup             Load base modules (Nextflow, Python)
  setup-spark       Load all modules including Spark

Examples:
  # Load modules interactively
  source fairway-hpc.sh setup

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
    echo "To install fairway in your environment:"
    echo "  pip install --user git+https://github.com/DISSC-yale/fairway.git#egg=fairway[all]"
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

check_fairway() {
    if ! command -v fairway &> /dev/null; then
        echo "Error: fairway command not found"
        echo "Please ensure fairway is installed in your environment:"
        echo "  pip install git+https://github.com/DISSC-yale/fairway.git"
        exit 1
    fi
}


# -----------------------------------------------------------------------------
# Main Command Handler
# -----------------------------------------------------------------------------

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
            exit 1
            ;;
    esac
}

# Only run main if script is executed (not sourced)
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
else
    # Script was sourced - just run the command if provided
    if [[ $# -gt 0 ]]; then
        main "$@"
    fi
fi
