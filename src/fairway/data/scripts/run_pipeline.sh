#!/bin/bash
set -e

# =============================================================================
# Fairway Pipeline Runner (INTERNAL)
# =============================================================================
# PURPOSE: This is an INTERNAL wrapper for the Nextflow command.
# IT IS NOT MEANT TO BE CALLED DIRECTLY BY USERS.
#
# It is used by 'make run' (interactive) and 'sbatch scripts/driver.sh' (batch)
# to ensure consistent execution behavior, including:
# - Loading Nextflow modules if needed
# - Finding the correct Nextflow binary
# - Automatically detecting and using a local 'fairway.sif' container image

# 0. Load Virtual Environment (if specificed)
if [ -n "$FAIRWAY_VENV" ]; then
    if [ -f "$FAIRWAY_VENV/bin/activate" ]; then
        echo "Activating virtual environment: $FAIRWAY_VENV"
        source "$FAIRWAY_VENV/bin/activate"
    else
        echo "Warning: FAIRWAY_VENV set to $FAIRWAY_VENV but activate script not found."
    fi
fi

# 1. Load Nextflow (Module check)
if command -v module >/dev/null 2>&1; then
    # Only load if not already in path or if we want to ensure latest
    # But usually 'module load' is safe to run.
    module load Nextflow 2>/dev/null || true
fi

# 2. Determine Nextflow binary
NXF_CMD="nextflow"
if [ -x "./nextflow" ]; then
    NXF_CMD="./nextflow"
fi

# 3. Check for local container image
CONTAINER_ARG=""
if [ -f "$(pwd)/fairway.sif" ]; then
    echo "Found local fairway.sif, using it."
    CONTAINER_ARG="--container_image $(pwd)/fairway.sif"
fi

# 4. Run Pipeline
# "$@" passes all arguments provided to this script directly to Nextflow
# e.g. -profile slurm,apptainer -resume
echo "Launching Nextflow..."
echo "Command: $NXF_CMD run main.nf $CONTAINER_ARG $@"
$NXF_CMD run main.nf $CONTAINER_ARG "$@"
