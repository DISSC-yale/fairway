#!/bin/bash
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
        pip install -e "$PROJECT_ROOT"
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
