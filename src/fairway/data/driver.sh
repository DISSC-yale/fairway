#!/bin/bash
#SBATCH --job-name=fairway_driver
#SBATCH --output=logs/driver_%j.log
#SBATCH --time=24:00:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1

# =============================================================================
# Fairway Driver Job
# =============================================================================
# This script is submitted to Slurm to run the Nextflow orchestrator on a
# compute node (preventing login node usage for long-running processes).

HAS_APPTAINER="$1"

# 1. Load Nextflow (Module check)
if command -v module >/dev/null 2>&1; then
    module load Nextflow 2>/dev/null || true
fi

# 2. Determine Nextflow binary
if [ -x "./nextflow" ]; then
    export NXF_CMD="./nextflow"
else
    export NXF_CMD="nextflow"
fi

echo "Using Nextflow: $NXF_CMD"
echo "Apptainer Mode: $HAS_APPTAINER"

# 3. Run Pipeline
CONTAINER_ARG=""
if [ -f "fairway.sif" ]; then
    echo "Found local fairway.sif, using it."
    CONTAINER_ARG="--container_image $(pwd)/fairway.sif"
fi

if [ "$HAS_APPTAINER" = "yes" ]; then
    $NXF_CMD run main.nf -profile slurm,apptainer -resume $CONTAINER_ARG
else
    $NXF_CMD run main.nf -profile slurm -resume
fi
