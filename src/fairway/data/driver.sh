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

# 3. Run Pipeline via Shared Script
if [ "$HAS_APPTAINER" = "yes" ]; then
    ./scripts/run_pipeline.sh -profile slurm,apptainer -resume
else
    ./scripts/run_pipeline.sh -profile slurm -resume
fi
