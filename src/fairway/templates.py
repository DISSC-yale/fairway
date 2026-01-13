try:
    from importlib import resources
except ImportError:
    import importlib_resources as resources

def _read_data_file(filename):
    try:
        # Python 3.9+
        return resources.files('fairway.data').joinpath(filename).read_text()
    except (AttributeError, TypeError):
        # Python 3.8 fallback or older importlib
        with resources.path('fairway.data', filename) as p:
            with open(p, 'r') as f:
                return f.read()



NEXTFLOW_CONFIG = _read_data_file('nextflow.config')

# The MAIN_NF template needs to be robust.
MAIN_NF = _read_data_file('main.nf')

APPTAINER_DEF = _read_data_file('Apptainer.def')

DOCKERFILE_TEMPLATE = _read_data_file('Dockerfile')

MAKEFILE_TEMPLATE = _read_data_file('Makefile')

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
  registry-login    Authenticate Apptainer with GitHub Container Registry

Note:
  This script MUST be sourced to load modules into your current shell.
  For running jobs, use: fairway run --slurm ...
  For checking status, use: fairway status

EOF
}

registry_login() {
    print_header
    echo "This helper will log you into the GitHub Container Registry (GHCR)."
    echo ""
    echo "1. Create a Personal Access Token (PAT) on GitHub:"
    echo "   Settings -> Developer Settings -> Personal access tokens (Classic)"
    echo "   Scope required: read:packages"
    echo ""
    echo "2. When prompted for password, PASTE YOUR TOKEN (starts with ghp_)"
    echo ""
    echo "Logging into docker://ghcr.io..."
    apptainer remote login -u "$USER" docker://ghcr.io
    echo ""
    echo "Login process completed."
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
        registry-login)
            registry_login
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
